"""HTTP/2 帧转发器（h2c 客户端侧）。

职责与设计要点：

- 与受控对端建立一条 h2c 连接，复用到多条并行下载流；
- 连接级与每流两级接收窗口独立记账，DATA 先占额度再入每流缓冲；
- 每流缓冲有硬上限（默认等于其初始窗口），慢读者只会撑满自己的流窗口
  而停在自己的缓冲上，连接窗口与其他流不受牵连；
- RST_STREAM：丢弃该流缓冲、把缓冲中尚未返还的字节通过流 0 的
  WINDOW_UPDATE 还给连接窗口，连接继续服务其他流；
- 所有 WINDOW_UPDATE 增量经饱和运算产出，临近 2^31-1 时自动夹住，
  绝不溢出；对端溢出更新则回 GOAWAY(FLOW_CONTROL_ERROR)。
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import dataclass, field

from . import frames as F
from .flow import ReceivingWindow, SendingWindow
from .frames import (
    ACK,
    CANCEL,
    DATA,
    END_STREAM,
    GOAWAY,
    HEADERS,
    PING,
    RST_STREAM,
    SETTINGS,
    SETTINGS_HEADER_TABLE_SIZE,
    SETTINGS_INITIAL_WINDOW_SIZE,
    SETTINGS_MAX_FRAME_SIZE,
    WINDOW_UPDATE,
    Frame,
    H2Error,
    ProtocolViolation,
    pack_frame,
    pack_goaway,
    pack_settings,
    pack_window_update,
    read_frame,
    unpack_goaway,
    unpack_rst,
    unpack_settings,
    unpack_window_update,
)
from .hpack import HpackDecoder, encode_headers

# ---- 输出接收器 ----------------------------------------------------------


class FileSink:
    """把流数据写入文件；slow_bps 给定时模拟慢读者（每秒仅消费若干字节）。"""

    def __init__(self, path, slow_bps: float | None = None):
        self.path = path
        self.slow_bps = slow_bps
        self._fh = None

    async def open(self) -> None:
        self._fh = open(self.path, "wb")

    async def write(self, data: bytes) -> None:
        if self.slow_bps is None:
            self._fh.write(data)
            self._fh.flush()  # 立即落盘：仪表盘/校验随时可见真实进度
            return
        tick_bytes = max(1, int(self.slow_bps * 0.05))
        view = memoryview(data)
        written = 0
        while written < len(view):
            chunk = view[written : written + tick_bytes]
            self._fh.write(chunk)
            self._fh.flush()
            written += len(chunk)
            await asyncio.sleep(0.05)

    async def close(self) -> None:
        if self._fh:
            self._fh.close()
            self._fh = None


@dataclass
class StreamState:
    sid: int
    path: str
    recv_window: ReceivingWindow
    buffer_cap: int
    buffer: bytearray = field(default_factory=bytearray)
    headers: list[tuple[bytes, bytes]] = field(default_factory=list)
    expected: int | None = None
    total: int = 0          # 线上收到的 DATA 字节
    written: int = 0        # 已交给接收器的字节
    end_stream: bool = False
    reset: bool = False
    reset_code: int | None = None
    status: str = "open"    # open / done / reset / failed
    event: asyncio.Event = field(default_factory=asyncio.Event)
    # 消费门闸：未 set 时消费协程在取走缓冲之前阻塞，缓冲会确定性堆积，
    # 用于慢读/重置演示；None 表示不启用
    gate: asyncio.Event | None = None
    released_bytes: int = 0  # 重置时丢弃的缓冲字节数
    consumer: asyncio.Task | None = None
    sink: FileSink | None = None
    started_at: float = field(default_factory=time.monotonic)
    finished_at: float | None = None

    @property
    def buffered(self) -> int:
        return len(self.buffer)

    @property
    def done(self) -> bool:
        return self.status in ("done", "reset", "failed")


class H2Forwarder:
    def __init__(
        self,
        host: str,
        port: int,
        *,
        initial_window: int = 32 * 1024,
        max_frame_size: int = F.DEFAULT_FRAME_SIZE,
        buffer_cap: int | None = None,
        event_history: int = 50,
    ):
        self.host = host
        self.port = port
        self.initial_window = initial_window
        self.max_frame_size = max_frame_size
        # enforce_buffer：默认窗口配置下缓冲硬上限等于初始窗口，做防御性断言；
        # 大窗口演示（nearlimit）关闭断言，仅靠窗口记账约束在途数据。
        self.buffer_cap = buffer_cap if buffer_cap is not None else initial_window
        self.enforce_buffer = buffer_cap is not None or initial_window <= 256 * 1024
        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None

        # 连接级接收窗口（对端 DATA 占用），上限固定为协议最大值
        self.conn_window = ReceivingWindow(F.INITIAL_WINDOW_SIZE, F.MAX_WINDOW)
        # 本端请求方向的发送额度（仅用于校验对端 WINDOW_UPDATE 合法性）
        self._send_window = SendingWindow()

        self.streams: dict[int, StreamState] = {}
        self._next_sid = 1
        self._write_lock = asyncio.Lock()
        self._win_lock = asyncio.Lock()
        self._decoder = HpackDecoder()
        self.events: deque[tuple[float, str]] = deque(maxlen=event_history)
        self.conn_closed = False
        self.goaway: tuple[int, str] | None = None
        self.stats = {
            "data_frames": 0,
            "window_updates_sent": 0,
            "window_update_bytes": 0,
            "rst_sent": 0,
            "rst_received": 0,
            "clamps": 0,
        }
        self._read_task: asyncio.Task | None = None
        self._opened_at: float | None = None

    # ---- 连接管理 --------------------------------------------------------

    async def connect(self) -> None:
        self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
        self._opened_at = time.monotonic()
        settings = pack_settings(
            [
                (SETTINGS_HEADER_TABLE_SIZE, 0),
                (SETTINGS_INITIAL_WINDOW_SIZE, self.initial_window),
                (SETTINGS_MAX_FRAME_SIZE, self.max_frame_size),
            ]
        )
        await self._write(F.CONNECTION_PREFACE + pack_frame(SETTINGS, 0, 0, settings))
        self._read_task = asyncio.create_task(self._read_loop())
        self._log(f"已连接对端 {self.host}:{self.port}，初始流窗口={self.initial_window}")

    async def _write(self, data: bytes) -> None:
        async with self._write_lock:
            self.writer.write(data)
            await self.writer.drain()

    def _log(self, message: str) -> None:
        self.events.append((time.monotonic(), message))

    async def _read_loop(self) -> None:
        try:
            while True:
                frame = await read_frame(self.reader)
                if frame is None:
                    self._mark_all_failed(F.INTERNAL_ERROR, "对端关闭了连接")
                    return
                await self._dispatch(frame)
        except H2Error as exc:
            await self._goaway(exc.code, str(exc))
        except (asyncio.IncompleteReadError, ConnectionError, RuntimeError) as exc:
            self._mark_all_failed(F.INTERNAL_ERROR, f"连接断开: {exc}")

    async def _goaway(self, code: int, reason: str) -> None:
        self._log(f"协议错误，发送 GOAWAY({code}): {reason}")
        try:
            highest = max((s.sid for s in self.streams.values()), default=0)
            await self._write(pack_frame(GOAWAY, 0, 0, pack_goaway(highest, code, reason.encode())))
        except (ConnectionError, RuntimeError):
            pass
        self._mark_all_failed(code, reason)
        self.conn_closed = True
        if self.writer:
            self.writer.close()

    def _mark_all_failed(self, code: int, reason: str) -> None:
        self.conn_closed = True
        self.goaway = (code, reason)
        for st in self.streams.values():
            if not st.done:
                st.status = "failed"
                st.reset_code = code
                st.finished_at = time.monotonic()
                if st.consumer and not st.consumer.done():
                    st.consumer.cancel()
                st.event.set()

    # ---- 帧分发 ----------------------------------------------------------

    async def _dispatch(self, frame: Frame) -> None:
        sid = frame.stream_id
        if frame.ftype == SETTINGS:
            if sid != 0:
                raise ProtocolViolation("SETTINGS 必须关联流 0")
            if frame.has(ACK):
                return
            await self._apply_settings(frame.payload)
            await self._write(pack_frame(SETTINGS, ACK, 0))
        elif frame.ftype == PING:
            if sid != 0 or len(frame.payload) != 8:
                raise ProtocolViolation("PING 帧非法", F.FRAME_SIZE_ERROR)
            if not frame.has(ACK):
                await self._write(pack_frame(PING, F.ACK, 0, frame.payload))
        elif frame.ftype == GOAWAY:
            last_sid, code, debug = unpack_goaway(frame.payload)
            self._log(f"收到 GOAWAY last={last_sid} code={code} {debug!r}")
            self._mark_all_failed(code, debug.decode(errors="replace"))
        elif frame.ftype == WINDOW_UPDATE:
            inc = unpack_window_update(frame.payload)
            if sid != 0:
                # 请求方向无 DATA，仅做合法性校验
                SendingWindow().apply_update(inc)
            else:
                self._send_window.apply_update(inc)  # 溢出 → FLOW_CONTROL_ERROR
        elif frame.ftype == HEADERS:
            await self._on_headers(frame)
        elif frame.ftype == DATA:
            await self._on_data(frame)
        elif frame.ftype == RST_STREAM:
            await self._on_rst(frame)
        else:
            raise ProtocolViolation(f"未实现的帧类型: {frame.name}")

    async def _apply_settings(self, payload: bytes) -> None:
        for sid, value in unpack_settings(payload):
            if sid == SETTINGS_INITIAL_WINDOW_SIZE:
                if value > F.MAX_WINDOW:
                    raise F.FlowControlViolation("INITIAL_WINDOW_SIZE 超过 2^31-1")
                delta = value - self.initial_window
                self.initial_window = value
                for st in self.streams.values():
                    st.recv_window.apply_initial_delta(delta)
            elif sid == SETTINGS_MAX_FRAME_SIZE:
                if not F.DEFAULT_FRAME_SIZE <= value <= F.MAX_FRAME_SIZE_LIMIT:
                    raise ProtocolViolation("MAX_FRAME_SIZE 越界", F.FRAME_SIZE_ERROR)
                self.max_frame_size = value
            # 其余设置（推送、并发流等）本练习器忽略

    async def _on_headers(self, frame: Frame) -> None:
        st = self.streams.get(frame.stream_id)
        if st is not None and st.reset:
            return  # RST 竞态：忽略迟到的响应头
        if st is None or st.done:
            raise ProtocolViolation(f"HEADERS 到达未知流 {frame.stream_id}")
        payload = frame.payload
        if frame.has(F.PADDED):
            pad_len = payload[0]
            payload = payload[1 : len(payload) - pad_len]
        if frame.has(F.PRIORITY_FLAG):
            payload = payload[5:]
        st.headers = self._decoder.decode(payload)
        for name, value in st.headers:
            if name == b"content-length":
                st.expected = int(value)
        st.event.set()

    async def _on_data(self, frame: Frame) -> None:
        st = self.streams.get(frame.stream_id)
        if st is not None and st.reset:
            # RFC 7540 §5.1：RST_STREAM 与对端已发出的在途帧存在竞态，
            # 重置流上迟到的 DATA 直接丢弃，不影响连接与其他流。
            self._log(f"流 {st.sid} 已重置，忽略迟到的 {len(frame.payload)} 字节 DATA")
            return
        if st is None or st.done:
            raise ProtocolViolation(f"DATA 到达未知/已关闭流 {frame.stream_id}", F.STREAM_CLOSED)
        payload = frame.payload
        if frame.has(F.PADDED):
            pad_len = payload[0]
            payload = payload[1 : len(payload) - pad_len]
        n = len(payload)
        if n:
            # 先占连接窗口，再占流窗口；任一超额都属流控违约
            self.conn_window.charge(n)
            st.recv_window.charge(n)
            st.buffer.extend(payload)
            st.total += n
            self.stats["data_frames"] += 1
            if self.enforce_buffer and len(st.buffer) > st.buffer_cap:
                # 默认配置下窗口额度 == 缓冲上限，收到即说明窗口逻辑有 bug
                raise F.FlowControlViolation(
                    f"流 {st.sid} 缓冲超过硬上限 {st.buffer_cap}"
                )
        if frame.has(END_STREAM):
            st.end_stream = True
        st.event.set()

    async def _on_rst(self, frame: Frame) -> None:
        code = unpack_rst(frame.payload)
        st = self.streams.get(frame.stream_id)
        self.stats["rst_received"] += 1
        if st is None or st.done:
            return
        await self._release_stream(st, code, send_rst=False)

    # ---- 流的生命周期 ----------------------------------------------------

    async def open_stream(self, path: str, sink: FileSink, *, gated: bool = False) -> StreamState:
        if self.conn_closed:
            raise ConnectionError("连接已关闭，无法开流")
        sid = self._next_sid
        self._next_sid += 2
        # 每流接收窗口上限固定为协议最大值；初始额度即我们通告的初始窗口
        gate = asyncio.Event() if gated else None
        st = StreamState(
            sid=sid,
            path=path,
            recv_window=ReceivingWindow(self.initial_window, F.MAX_WINDOW),
            buffer_cap=self.buffer_cap,
            sink=sink,
            gate=gate,
        )
        self.streams[sid] = st
        await sink.open()
        block = encode_headers(
            [
                (b":method", b"GET"),
                (b":path", path.encode()),
                (b":scheme", b"http"),
                (b":authority", f"{self.host}:{self.port}".encode()),
            ]
        )
        await self._write(
            pack_frame(HEADERS, F.END_HEADERS | END_STREAM, sid, block)
        )
        st.consumer = asyncio.create_task(self._consume(st))
        self._log(f"流 {sid} 打开 GET {path}（缓冲上限 {st.buffer_cap} 字节）")
        return st

    async def _consume(self, st: StreamState) -> None:
        try:
            while True:
                if st.reset:
                    return
                if not st.buffer:
                    if st.end_stream:
                        break
                    # 缓冲为空：让出协程，等待读循环投递新数据或结束标记
                    await st.event.wait()
                    st.event.clear()
                    continue
                if st.gate is not None:
                    # 门闸未放行：在取走缓冲之前阻塞，缓冲保持堆积（慢读演示）
                    await st.gate.wait()
                    if st.reset:
                        return
                data = bytes(st.buffer)
                st.buffer.clear()
                await st.sink.write(data)  # 慢读者在这里睡住，不影响读循环
                st.written += len(data)
                await self._credit_consumed(st, len(data))
            if st.expected is not None and st.written != st.expected:
                raise ProtocolViolation(
                    f"流 {st.sid} 长度不符：{st.written} != {st.expected}"
                )
            st.status = "done"
            st.finished_at = time.monotonic()
            self._log(f"流 {st.sid} 完成：{st.written} 字节")
            st.event.set()
        except asyncio.CancelledError:
            pass
        except Exception as exc:  # 接收器失败 → 重置该流，连接保留
            self._log(f"流 {st.sid} 消费失败: {exc}")
            if not st.done:
                await self.reset_stream(st.sid, F.INTERNAL_ERROR)

    async def _credit_consumed(self, st: StreamState, n: int) -> None:
        """数据被上层消费后，按饱和运算返还两级窗口。"""
        if st.reset:
            return  # 流已重置：连接额度由 _release_stream 统一处理，不补发流级更新
        async with self._win_lock:
            st.recv_window.consume(n)
            self.conn_window.consume(n)
            frames = bytearray()
            inc_stream = st.recv_window.take_update()
            if inc_stream:
                frames += pack_frame(WINDOW_UPDATE, 0, st.sid, pack_window_update(inc_stream))
            inc_conn = self.conn_window.take_update()
            if inc_conn:
                frames += pack_frame(WINDOW_UPDATE, 0, 0, pack_window_update(inc_conn))
            if st.recv_window.pending or self.conn_window.pending:
                # 窗口临近上限：增量被夹住，余额留待窗口腾出空间后再发
                self.stats["clamps"] += 1
                self._log(
                    f"窗口临近上限，增量夹至 流+{inc_stream}/连接+{inc_conn}，"
                    f"待返还 流={st.recv_window.pending} 连接={self.conn_window.pending}"
                )
            if frames:
                # 一个 WINDOW_UPDATE 帧载荷固定 4 字节，逐帧计数
                n_frames = (1 if inc_stream else 0) + (1 if inc_conn else 0)
                self.stats["window_updates_sent"] += n_frames
                self.stats["window_update_bytes"] += inc_stream + inc_conn
                await self._write(bytes(frames))

    async def reset_stream(self, sid: int, code: int = CANCEL) -> None:
        """本地重置：发 RST_STREAM，释放缓冲，把连接额度还给连接窗口。"""
        st = self.streams.get(sid)
        if st is None or st.done:
            return
        await self._release_stream(st, code, send_rst=True)

    async def _release_stream(self, st: StreamState, code: int, *, send_rst: bool) -> None:
        async with self._win_lock:
            st.reset = True
            st.status = "reset"
            st.reset_code = code
            st.finished_at = time.monotonic()
            discarded = len(st.buffer)
            st.released_bytes = discarded
            st.buffer.clear()
            out = bytearray()
            if send_rst:
                import struct as _struct

                out += pack_frame(RST_STREAM, 0, st.sid, _struct.pack(">I", code))
                self.stats["rst_sent"] += 1
            # 缓冲里的字节已占连接额度但尚未返还：通过流 0 退回连接窗口，
            # 流级窗口随流消亡，不再补发流级 WINDOW_UPDATE。
            if discarded:
                self.conn_window.consume(discarded)
                inc = self.conn_window.take_update()
                if inc:
                    out += pack_frame(WINDOW_UPDATE, 0, 0, pack_window_update(inc))
                    self.stats["window_updates_sent"] += 1
                    self.stats["window_update_bytes"] += inc
            if out:
                await self._write(bytes(out))
        if st.consumer and not st.consumer.done():
            st.consumer.cancel()
        if st.sink:
            await st.sink.close()
        st.event.set()
        self._log(
            f"流 {st.sid} 重置(code={code})，释放 {discarded} 字节缓冲，连接继续可用"
        )

    async def wait_stream(self, sid: int) -> StreamState:
        st = self.streams[sid]
        while not st.done:
            await st.event.wait()
            st.event.clear()
        return st

    async def ping(self, opaque: bytes = b"h2fwdping") -> None:
        await self._write(pack_frame(PING, 0, 0, opaque))

    async def aclose(self) -> None:
        for st in self.streams.values():
            if st.consumer and not st.consumer.done():
                st.consumer.cancel()
            if st.sink:
                await st.sink.close()
        if self.writer and not self.conn_closed:
            try:
                highest = max((s.sid for s in self.streams.values()), default=0)
                await self._write(
                    pack_frame(GOAWAY, 0, 0, pack_goaway(highest, F.NO_ERROR, b"shutdown"))
                )
            except (ConnectionError, RuntimeError):
                pass
        if self.writer:
            self.writer.close()
            try:
                await self.writer.wait_closed()
            except (ConnectionError, RuntimeError):
                pass
        if self._read_task:
            self._read_task.cancel()

    # ---- 仪表盘快照 ------------------------------------------------------

    def snapshot(self) -> dict:
        now = time.monotonic()
        return {
            "elapsed": now - (self._opened_at or now),
            "conn_window_available": self.conn_window.available,
            "conn_window_pending": self.conn_window.pending,
            "conn_closed": self.conn_closed,
            "goaway": self.goaway,
            "stats": dict(self.stats),
            "streams": [
                {
                    "sid": st.sid,
                    "status": st.status,
                    "available": st.recv_window.available,
                    "pending": st.recv_window.pending,
                    "buffered": st.buffered,
                    "total": st.total,
                    "written": st.written,
                    "expected": st.expected,
                    "elapsed": (st.finished_at or now) - st.started_at,
                }
                for st in self.streams.values()
            ],
            "events": [
                (t - (self._opened_at or t), msg) for t, msg in list(self.events)[-8:]
            ],
        }
