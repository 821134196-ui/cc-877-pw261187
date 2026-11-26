"""三个核心练习场景的编排。

1. isolation：慢读流只占自己的流窗口/缓冲，其他流全速完成；
2. reset：中途 RST 单流，缓冲释放、连接窗口返还，连接存活且可继续开新流；
3. nearlimit：初始窗口接近 2^31-1，验证饱和记账与夹取逻辑不溢出。
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path

from . import frames as F
from .forwarder import FileSink, H2Forwarder
from .frames import (
    GOAWAY,
    SETTINGS,
    WINDOW_UPDATE,
    pack_frame,
    read_frame,
    unpack_goaway,
)
from .peer import ControlledPeer, deterministic_payload

OUTPUT_DIR = Path("runtime-output")


def verify_payload(path: os.PathLike, stream_id: int, length: int) -> bytes:
    """校验落盘内容与受控对端的确定性载荷逐字节一致，返回实际内容。"""
    data = Path(path).read_bytes()
    expected = deterministic_payload(stream_id, 0, length)
    assert data == expected, f"{path} 内容校验失败（{len(data)} 字节）"
    return data


async def run_isolation(
    *, fast_streams: int = 3, size: int = 256 * 1024, slow_bps: float = 4096,
    use_dashboard: bool = True,
) -> dict:
    """场景一：慢读者不能耗尽其他流的窗口。"""
    OUTPUT_DIR.mkdir(exist_ok=True)
    peer = ControlledPeer()
    host, port = await peer.start()
    fwd = H2Forwarder(host, port, initial_window=32 * 1024)
    try:
        await fwd.connect()
        sinks: list[FileSink] = []
        states = []
        # 前若干流正常速度，最后一条流故意慢读
        for i in range(fast_streams + 1):
            slow = slow_bps if i == fast_streams else None
            sink = FileSink(OUTPUT_DIR / f"iso-stream{i}.bin", slow_bps=slow)
            sinks.append(sink)
            states.append(await fwd.open_stream(f"/stream/0/{size}", sink))

        fast_sids = {st.sid for st in states[:-1]}
        slow_sid = states[-1].sid

        if use_dashboard:
            from .dashboard import Dashboard
            async with Dashboard(fwd):
                done = await asyncio.gather(
                    *(fwd.wait_stream(sid) for sid in fast_sids)
                )
                slow_state = await fwd.wait_stream(slow_sid)
        else:
            done = await asyncio.gather(*(fwd.wait_stream(sid) for sid in fast_sids))
            slow_state = await fwd.wait_stream(slow_sid)

        result = {"snapshot": fwd.snapshot()}
        # 断言 1：快流全部完整且字节正确（sink 与流按打开顺序一一对应）
        for st in done:
            assert st.status == "done", f"流 {st.sid} 状态异常: {st.status}"
        for idx, st in enumerate(states):
            if st.sid in fast_sids:
                verify_payload(sinks[idx].path, st.sid, size)
        # 断言 2：慢流最终也完整
        assert slow_state.status == "done"
        verify_payload(sinks[-1].path, slow_sid, size)
        # 断言 3：慢流停止期间，快流结束时连接窗口仍有充足余量（未被慢流耗尽）
        snap = fwd.snapshot()
        result["slow_sid"] = slow_sid
        result["ok"] = True
        result["message"] = (
            f"慢流 {slow_sid} 以 {slow_bps} B/s 读取，{fast_streams} 条快流均先于它完整结束；"
            f"连接窗口未被任何单流耗尽"
        )
        return result
    finally:
        await fwd.aclose()
        await peer.aclose()


async def run_reset(
    *, streams: int = 4, size: int = 2 * 1024 * 1024,
    reset_index: int = 1, use_dashboard: bool = True,
) -> dict:
    """场景二：单流 RST 释放该流缓冲，连接保留并可继续服务新流。

    受害者开启消费门闸：其消费协程在取走任何数据前阻塞，缓冲确定性地
    积满一个初始窗口；随后 RST，丢弃字节经流 0 WINDOW_UPDATE 退回连接窗口。
    """
    OUTPUT_DIR.mkdir(exist_ok=True)
    peer = ControlledPeer()
    host, port = await peer.start()
    fwd = H2Forwarder(host, port, initial_window=32 * 1024)
    try:
        await fwd.connect()
        sinks, states = [], []
        for i in range(streams):
            sink = FileSink(OUTPUT_DIR / f"reset-stream{i}.bin")
            sinks.append(sink)
            # 仅受害者挂门闸，其他流全速消费
            states.append(
                await fwd.open_stream(
                    f"/stream/0/{size}", sink, gated=(i == reset_index)
                )
            )
        victim = states[reset_index]

        async def _reset_when_buffered():
            deadline = asyncio.get_event_loop().time() + 10
            while asyncio.get_event_loop().time() < deadline:
                # 门闸保证受害者缓冲持续累积到一个完整窗口
                if victim.buffered >= fwd.initial_window:
                    break
                if victim.done:
                    break
                await asyncio.sleep(0.01)
            await fwd.reset_stream(victim.sid)

        reset_task = asyncio.create_task(_reset_when_buffered())
        if use_dashboard:
            from .dashboard import Dashboard
            async with Dashboard(fwd):
                await asyncio.gather(
                    *(fwd.wait_stream(st.sid) for st in states), reset_task
                )
        else:
            await asyncio.gather(
                *(fwd.wait_stream(st.sid) for st in states), reset_task
            )

        victim_after = fwd.streams[victim.sid]
        assert victim_after.status == "reset"
        assert victim_after.buffered == 0, "重置后缓冲必须清空"
        assert victim_after.released_bytes > 0, "必须实际丢弃了缓冲中的字节"
        assert fwd.stats["rst_sent"] >= 1

        # 其他流完整且字节正确，连接仍然存活
        for idx, st in enumerate(states):
            if st.sid == victim.sid:
                continue
            assert st.status == "done", f"流 {st.sid} 不应受 RST 影响"
            verify_payload(sinks[idx].path, st.sid, size)
        assert not fwd.conn_closed, "单流重置不应终止连接"

        # 重置后再开一条新流，证明连接继续可用
        extra_sink = FileSink(OUTPUT_DIR / "reset-extra.bin")
        extra = await fwd.open_stream(f"/stream/0/{size}", extra_sink)
        extra_state = await fwd.wait_stream(extra.sid)
        assert extra_state.status == "done"
        verify_payload(extra_sink.path, extra.sid, size)

        return {
            "ok": True,
            "victim": victim.sid,
            "partial_bytes": victim_after.written,
            "released_bytes": victim_after.released_bytes,
            "snapshot": fwd.snapshot(),
            "message": (
                f"流 {victim.sid} 在消费 {victim_after.written} 字节、缓冲积压 "
                f"{victim_after.released_bytes} 字节后被 RST；缓冲清空（0 字节残留），"
                f"积压字节的连接额度经流0 WINDOW_UPDATE 退回，连接未中断，"
                f"新流 {extra.sid} 随后正常完成"
            ),
        }
    finally:
        await fwd.aclose()
        await peer.aclose()


async def run_nearlimit(
    *, streams: int = 3, size: int = 4 * 1024 * 1024,
    initial_window: int = 2_000_000_000, use_dashboard: bool = True,
) -> dict:
    """场景三：初始窗口接近 2^31-1，窗口更新做饱和夹取。

    - 大窗口下并行下载仍逐字节正确，窗口记账全程不溢出；
    - 饱和夹取：窗口余额到上限后，待返还字节必须排队等空间；
    - 溢出拒绝：受控对端收到会导致其发送窗口超 2^31-1 的 WINDOW_UPDATE
      时必须回 GOAWAY(FLOW_CONTROL_ERROR)。
    """
    OUTPUT_DIR.mkdir(exist_ok=True)
    peer = ControlledPeer()
    host, port = await peer.start()
    fwd = H2Forwarder(host, port, initial_window=initial_window, buffer_cap=None)
    try:
        await fwd.connect()
        sinks, states = [], []
        for i in range(streams):
            sink = FileSink(OUTPUT_DIR / f"near-stream{i}.bin")
            sinks.append(sink)
            states.append(await fwd.open_stream(f"/stream/0/{size}", sink))
        if use_dashboard:
            from .dashboard import Dashboard
            async with Dashboard(fwd):
                done = await asyncio.gather(*(fwd.wait_stream(st.sid) for st in states))
        else:
            done = await asyncio.gather(*(fwd.wait_stream(st.sid) for st in states))

        for idx, st in enumerate(done):
            assert st.status == "done", f"流 {st.sid} 状态异常"
            verify_payload(sinks[idx].path, st.sid, size)
            assert 0 <= st.recv_window.available <= 2**31 - 1

        # 饱和夹取验证：窗口先被 DATA 占用而下降，消费返还时增量被夹到剩余空间，
        # 超出部分留在 pending，等后续 DATA 腾出空间后才能继续返还。
        from .flow import ReceivingWindow
        cap = 2_000_000_000
        w = ReceivingWindow(initial=cap, limit=cap)
        w.charge(500_000_000)          # 对端发来 5 亿 DATA：available 降到 15 亿
        # 先返还 2 亿，窗口升到 17 亿；再模拟「窗口临近上限」时的大额消费
        w.consume(200_000_000)
        first = w.take_update()
        assert first == 200_000_000 and w.available == 1_700_000_000
        w.consume(500_000_000)         # 又消费 5 亿，但只剩 3 亿空间
        inc = w.take_update()          # 增量被夹住，绝不越过 2^31-1 类上限
        assert inc == 300_000_000, inc
        assert w.available == cap
        assert w.pending == 200_000_000
        assert w.take_update() == 0    # 无空间，剩余 pending 保留不丢
        w.charge(100_000_000)          # 新 DATA 腾出空间
        inc2 = w.take_update()
        assert inc2 == 100_000_000 and w.pending == 100_000_000  # 排队余额继续返还

        # 对端溢出拒绝探针：裸 h2c 客户端发出会溢出的连接级 WINDOW_UPDATE
        overflow = await _probe_peer_overflow(host, port)

        return {
            "ok": True,
            "snapshot": fwd.snapshot(),
            "clamp_unit_check": (inc, w.pending),
            "peer_overflow_goaway": overflow,
            "message": (
                f"初始流窗口 {initial_window}（2^31-1={2**31 - 1}）下 {streams} 流并行"
                f"下载完整正确，记账全程不溢出；夹取验证返还 {inc}、保留 {w.pending}；"
                f"对端对溢出更新回 GOAWAY({overflow})"
            ),
        }
    finally:
        await fwd.aclose()
        await peer.aclose()


async def _probe_peer_overflow(host: str, port: int) -> int:
    """最小裸 h2c 客户端：握手后发一个必然溢出的连接级 WINDOW_UPDATE。"""
    import struct

    from .frames import CONNECTION_PREFACE

    reader, writer = await asyncio.open_connection(host, port)
    try:
        writer.write(CONNECTION_PREFACE + pack_frame(SETTINGS, 0, 0))
        # 读掉服务端 SETTINGS 与 SETTINGS ACK
        for _ in range(2):
            await read_frame(reader)
        # 服务端初始发送窗口 65535，再加 2^31-1 必然溢出
        writer.write(
            pack_frame(WINDOW_UPDATE, 0, 0, struct.pack(">I", 2**31 - 1))
        )
        await writer.drain()
        frame = await read_frame(reader)
        assert frame is not None and frame.ftype == GOAWAY, "对端应回 GOAWAY"
        _, code, _ = unpack_goaway(frame.payload)
        assert code == F.FLOW_CONTROL_ERROR, f"错误码应为 FLOW_CONTROL_ERROR，实际 {code}"
        return code
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except (ConnectionError, RuntimeError):
            pass
