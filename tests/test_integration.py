"""端到端集成测试：转发器 × 受控对端的真实 TCP/h2c 链路。"""

import asyncio
import struct
import unittest

from h2fwd import frames as F
from h2fwd.flow import ReceivingWindow
from h2fwd.forwarder import FileSink, H2Forwarder
from h2fwd.frames import (
    CONNECTION_PREFACE,
    FLOW_CONTROL_ERROR,
    GOAWAY,
    SETTINGS,
    WINDOW_UPDATE,
    pack_frame,
    pack_window_update,
    read_frame,
    unpack_goaway,
)
from h2fwd.hpack import HpackDecoder
from h2fwd.peer import ControlledPeer, deterministic_payload
from h2fwd.scenarios import run_isolation, run_nearlimit, run_reset

import tempfile
from pathlib import Path


class _TempDir:
    def __init__(self):
        self._cm = tempfile.TemporaryDirectory()
        self.path = None

    def __enter__(self):
        self.path = Path(self._cm.__enter__())
        import h2fwd.scenarios as sc
        sc.OUTPUT_DIR = self.path
        return self.path

    def __exit__(self, *exc):
        return self._cm.__exit__(*exc)


class ScenarioTests(unittest.TestCase):
    def test_slow_reader_does_not_starve_other_streams(self):
        with _TempDir():
            result = asyncio.run(
                run_isolation(
                    fast_streams=3, size=64 * 1024, slow_bps=8192,
                    use_dashboard=False,
                )
            )
        snap = result["snapshot"]
        slow = next(s for s in snap["streams"] if s["sid"] == result["slow_sid"])
        fast = [s for s in snap["streams"] if s["sid"] != result["slow_sid"]]
        # 慢流缓冲始终被自己的初始窗口（32KiB）约束
        self.assertLessEqual(slow["buffered"], 32 * 1024)
        # 所有快流完整结束
        self.assertTrue(all(s["status"] == "done" for s in fast))
        self.assertEqual(slow["status"], "done")
        # 连接窗口没有被任何单流耗尽：全程仍有正余额
        self.assertGreater(snap["conn_window_available"], 0)

    def test_rst_releases_stream_but_keeps_connection(self):
        with _TempDir():
            result = asyncio.run(
                run_reset(streams=4, size=2 * 1024 * 1024, use_dashboard=False)
            )
        snap = result["snapshot"]
        victim = next(s for s in snap["streams"] if s["sid"] == result["victim"])
        self.assertEqual(victim["status"], "reset")
        self.assertEqual(victim["buffered"], 0)           # 缓冲已释放
        self.assertGreater(result["released_bytes"], 0)   # 确实丢弃了积压字节
        self.assertFalse(snap["conn_closed"])             # 连接保留
        self.assertGreaterEqual(snap["stats"]["rst_sent"], 1)
        # 丢弃字节必须产生连接级窗口返还
        self.assertGreaterEqual(snap["stats"]["window_update_bytes"], result["released_bytes"])
        others = [s for s in snap["streams"] if s["sid"] != result["victim"]]
        self.assertTrue(all(s["status"] == "done" for s in others))

    def test_nearlimit_window_never_overflows(self):
        with _TempDir():
            result = asyncio.run(
                run_nearlimit(
                    streams=3, size=1024 * 1024, initial_window=2_000_000_000,
                    use_dashboard=False,
                )
            )
        self.assertTrue(result["ok"])
        self.assertEqual(result["peer_overflow_goaway"], FLOW_CONTROL_ERROR)
        for st in result["snapshot"]["streams"]:
            self.assertEqual(st["status"], "done")
            self.assertLessEqual(st["available"], 2**31 - 1)


class RawPeerBehaviorTests(unittest.IsolatedAsyncioTestCase):
    """绕过转发器，用裸帧直接验证受控对端的协议裁决。"""

    async def asyncSetUp(self):
        self.peer = ControlledPeer()
        self.host, self.port = await self.peer.start()

    async def asyncTearDown(self):
        await self.peer.aclose()

    async def _connect_handshake(self):
        reader, writer = await asyncio.open_connection(self.host, self.port)
        writer.write(CONNECTION_PREFACE + pack_frame(SETTINGS, 0, 0))
        for _ in range(2):  # 服务端 SETTINGS + ACK
            await read_frame(reader)
        return reader, writer

    async def test_overflow_window_update_triggers_flow_control_goaway(self):
        reader, writer = await self._connect_handshake()
        try:
            writer.write(
                pack_frame(WINDOW_UPDATE, 0, 0, struct.pack(">I", 2**31 - 1))
            )
            await writer.drain()
            frame = await read_frame(reader)
            self.assertEqual(frame.ftype, GOAWAY)
            _, code, _ = unpack_goaway(frame.payload)
            self.assertEqual(code, FLOW_CONTROL_ERROR)
        finally:
            writer.close()

    async def test_peer_reset_stops_only_target_stream(self):
        reader, writer = await asyncio.open_connection(self.host, self.port)
        try:
            writer.write(CONNECTION_PREFACE + pack_frame(SETTINGS, 0, 0))
            for _ in range(2):
                await read_frame(reader)
            from h2fwd.hpack import encode_headers

            for sid in (1, 3):
                block = encode_headers(
                    [
                        (b":method", b"GET"),
                        (b":path", b"/stream/0/200000"),
                        (b":scheme", b"http"),
                        (b":authority", b"x"),
                    ]
                )
                writer.write(pack_frame(F.HEADERS, F.END_HEADERS | F.END_STREAM, sid, block))

            # 立即重置流 1；流 3 应照常完成
            writer.write(pack_frame(F.RST_STREAM, 0, 1, struct.pack(">I", F.CANCEL)))
            await writer.drain()

            # 裸客户端必须自己维护两级接收窗口，及时发 WINDOW_UPDATE
            conn_credit = 65_535
            stream_credit = {1: 65_535, 3: 65_535}
            stream1_data = bytearray()
            stream3_data = bytearray()
            end3 = False
            for _ in range(500):
                frame = await asyncio.wait_for(read_frame(reader), timeout=5)
                if frame.ftype == F.DATA:
                    n = len(frame.payload)
                    conn_credit -= n
                    stream_credit[frame.stream_id] -= n
                    if frame.stream_id == 1:
                        stream1_data += frame.payload
                    elif frame.stream_id == 3:
                        stream3_data += frame.payload
                    # 每 32 KiB 批量返还两级窗口
                    if conn_credit <= 32_768:
                        writer.write(
                            pack_frame(WINDOW_UPDATE, 0, 0, pack_window_update(65_535))
                        )
                        conn_credit += 65_535
                    if stream_credit[frame.stream_id] <= 32_768:
                        writer.write(
                            pack_frame(
                                WINDOW_UPDATE, 0, frame.stream_id,
                                pack_window_update(65_535),
                            )
                        )
                        stream_credit[frame.stream_id] += 65_535
                    await writer.drain()
                    if frame.has(F.END_STREAM) and frame.stream_id == 3:
                        end3 = True
                        break
            # 流 1 被重置：服务端不再向它发任何 DATA；流 3 完整且字节正确
            self.assertEqual(bytes(stream1_data), b"")
            self.assertTrue(end3)
            self.assertEqual(bytes(stream3_data), deterministic_payload(3, 0, 200000))
        finally:
            writer.close()


class ResetWindowRefundTests(unittest.TestCase):
    """RST 时连接窗口返还的簿记级验证（不依赖时序）。"""

    def test_discarded_bytes_returned_to_connection_window(self):
        cap = 1_000_000
        conn = ReceivingWindow(cap, cap)
        # 模拟两条流各占用 20 万，慢流被 RST 时其 20 万还在缓冲里
        conn.charge(200_000)
        conn.charge(200_000)
        self.assertEqual(conn.available, 600_000)
        conn.consume(200_000)  # RST 丢弃的缓冲
        inc = conn.take_update()
        self.assertEqual(inc, 200_000)
        self.assertEqual(conn.available, 800_000)  # 连接额度退回，其他流可用


if __name__ == "__main__":
    unittest.main()
