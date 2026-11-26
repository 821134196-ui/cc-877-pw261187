"""受控 HTTP/2 对端：本机 h2c 服务端。

这是一个「听话」的协议对端，供转发器练习使用：

- 严格的连接前导 / SETTINGS 握手，非法设置或窗口溢出回 GOAWAY(FLOW_CONTROL_ERROR)；
- 每个请求流由独立发送协程驱动，同时遵守连接窗口与流窗口；
- 收到 RST_STREAM 立即停止该流、释放其缓冲，连接与其他流不受影响；
- 响应体是确定性可校验字节：第 i 字节 == ((i*7 + stream_id) & 0xFF)。
"""

from __future__ import annotations

import asyncio
import logging

from . import frames as F
from .flow import ReceivingWindow, SendingWindow
from .frames import (
    ACK,
    DATA,
    END_STREAM,
    GOAWAY,
    HEADERS,
    PING,
    RST_STREAM,
    SETTINGS,
    SETTINGS_ENABLE_PUSH,
    SETTINGS_INITIAL_WINDOW_SIZE,
    SETTINGS_MAX_CONCURRENT_STREAMS,
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
    read_preface,
    unpack_goaway,
    unpack_rst,
    unpack_settings,
    unpack_window_update,
)
from .hpack import HpackDecoder, encode_headers

log = logging.getLogger("h2fwd.peer")


def deterministic_payload(stream_id: int, offset: int, n: int) -> bytes:
    """生成 stream_id 流上 [offset, offset+n) 的确定性字节。"""
    block = bytes((i * 7 + stream_id) & 0xFF for i in range(256))
    out = bytearray()
    i = offset
    remaining = n
    while remaining:
        j = i % 256
        take = min(remaining, 256 - j)
        out += block[j : j + take]
        i += take
        remaining -= take
    return bytes(out)


class _ServerStream:
    def __init__(self, sid: int, headers: list[tuple[bytes, bytes]], content_length: int):
        self.sid = sid
        self.headers = headers
        self.content_length = content_length
        self.send_window = SendingWindow()   # 对端（客户端）授予的流窗口
        self.recv_window = ReceivingWindow()  # 请求体接收窗口（本练习请求通常无体）
        self.sender: asyncio.Task | None = None
        self.reset = False
        self.ended = False


class ControlledPeer:
    def __init__(self, host: str = "127.0.0.1", port: int = 0):
        self.host = host
        self.port = port
        self._server: asyncio.AbstractServer | None = None
        self._conns: set[_PeerConnection] = set()

    async def start(self) -> tuple[str, int]:
        self._server = await asyncio.start_server(self._accept, self.host, self.port)
        sock = self._server.sockets[0]
        self.host, self.port = sock.getsockname()[:2]
        log.info("受控对端监听 %s:%d", self.host, self.port)
        return self.host, self.port

    async def _accept(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        conn = _PeerConnection(reader, writer)
        self._conns.add(conn)
        try:
            await conn.run()
        except (H2Error, asyncio.IncompleteReadError, ConnectionError) as exc:
            await conn.shutdown(getattr(exc, "code", F.INTERNAL_ERROR), str(exc))
        except Exception:
            log.exception("对端连接异常")
            await conn.shutdown(F.INTERNAL_ERROR, "internal error")
        finally:
            self._conns.discard(conn)

    async def aclose(self) -> None:
        for conn in list(self._conns):
            await conn.shutdown(F.NO_ERROR, "server shutdown")
        if self._server:
            self._server.close()
            await self._server.wait_closed()


class _PeerConnection:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.reader = reader
        self.writer = writer
        # 单一写协程 + 有界队列：读循环只入队、绝不被 drain 阻塞，
        # 否则 TCP 发送缓冲打满时会与客户端的 WINDOW_UPDATE 形成死锁
        self._send_q: asyncio.Queue[bytes] = asyncio.Queue(maxsize=256)
        self._writer_task = asyncio.create_task(self._writer_loop())
        self._credit_cond = asyncio.Condition()
        self.streams: dict[int, _ServerStream] = {}
        self.highest_sid = 0

        # 对端（客户端）拥有的接收窗口 → 本端发送额度
        self.conn_send_window = SendingWindow()
        # 本端的请求体接收窗口
        self.conn_recv_window = ReceivingWindow()

        # 对端公布的设置
        self.peer_max_frame = F.DEFAULT_FRAME_SIZE
        self.peer_iws = F.INITIAL_WINDOW_SIZE
        self._settings_acked = asyncio.Event()
        self._closing = False

    async def _writer_loop(self) -> None:
        try:
            while True:
                data = await self._send_q.get()
                if data is None:  # 关闭哨兵
                    return
                self.writer.write(data)
                await self.writer.drain()
        except (ConnectionError, RuntimeError):
            pass

    async def _write(self, data: bytes) -> None:
        await self._send_q.put(data)

    async def shutdown(self, code: int, reason: str) -> None:
        if self._closing:
            return
        self._closing = True
        for st in self.streams.values():
            if st.sender and not st.sender.done():
                st.sender.cancel()
        try:
            await self._write(
                pack_frame(GOAWAY, 0, 0, pack_goaway(self.highest_sid, code, reason.encode()))
            )
            await self._send_q.put(None)  # 排空队列后结束写协程
            await self._writer_task
        except (ConnectionError, RuntimeError, asyncio.CancelledError):
            pass
        finally:
            self.writer.close()
            try:
                await self.writer.wait_closed()
            except (ConnectionError, RuntimeError):
                pass

    async def run(self) -> None:
        await read_preface(self.reader)
        # 第一帧必须是客户端 SETTINGS（可为空）
        first = await read_frame(self.reader)
        if first is None or first.ftype != SETTINGS or first.has(ACK):
            raise ProtocolViolation("前导之后必须是 SETTINGS 帧")
        await self._apply_peer_settings(first.payload)

        # 公布本端设置
        await self._write(
            pack_frame(
                SETTINGS,
                0,
                0,
                pack_settings(
                    [
                        (SETTINGS_MAX_CONCURRENT_STREAMS, 100),
                        (SETTINGS_INITIAL_WINDOW_SIZE, F.INITIAL_WINDOW_SIZE),
                        (SETTINGS_MAX_FRAME_SIZE, F.DEFAULT_FRAME_SIZE),
                    ]
                ),
            )
        )
        await self._write(pack_frame(SETTINGS, ACK, 0))  # ACK 客户端设置

        decoder = HpackDecoder()
        while True:
            frame = await read_frame(self.reader)
            if frame is None:
                await self.shutdown(F.NO_ERROR, "peer closed")
                return
            await self._handle(frame, decoder)

    # ---- 设置 / 窗口校验 -------------------------------------------------

    async def _apply_peer_settings(self, payload: bytes) -> None:
        for sid, value in unpack_settings(payload):
            if sid == SETTINGS_INITIAL_WINDOW_SIZE:
                if value > F.MAX_WINDOW:
                    raise F.FlowControlViolation("INITIAL_WINDOW_SIZE 超出 2^31-1")
                delta = value - self.peer_iws
                self.peer_iws = value
                for st in self.streams.values():
                    st.send_window.apply_initial_delta(delta)
                async with self._credit_cond:
                    self._credit_cond.notify_all()
            elif sid == SETTINGS_MAX_FRAME_SIZE:
                if not F.DEFAULT_FRAME_SIZE <= value <= F.MAX_FRAME_SIZE_LIMIT:
                    raise ProtocolViolation("MAX_FRAME_SIZE 越界", F.FRAME_SIZE_ERROR)
                self.peer_max_frame = value
            elif sid == SETTINGS_ENABLE_PUSH:
                if value not in (0, 1):
                    raise ProtocolViolation("ENABLE_PUSH 必须为 0 或 1")
            elif sid == SETTINGS_MAX_CONCURRENT_STREAMS:
                pass  # 本端不推流，无需处理
            # 未知设置标识符按 RFC 忽略

    async def _handle(self, frame: Frame, decoder: HpackDecoder) -> None:
        sid = frame.stream_id

        if frame.ftype == SETTINGS:
            if sid != 0:
                raise ProtocolViolation("SETTINGS 必须关联流 0")
            if frame.has(ACK):
                if frame.payload:
                    raise ProtocolViolation("SETTINGS ACK 必须为空", F.FRAME_SIZE_ERROR)
                self._settings_acked.set()
            else:
                await self._apply_peer_settings(frame.payload)
                await self._write(pack_frame(SETTINGS, ACK, 0))
            return

        if frame.ftype == PING:
            if sid != 0 or len(frame.payload) != 8:
                raise ProtocolViolation("PING 帧非法", F.FRAME_SIZE_ERROR)
            if not frame.has(ACK):
                await self._write(pack_frame(PING, F.ACK, 0, frame.payload))
            return

        if frame.ftype == GOAWAY:
            if sid != 0:
                raise ProtocolViolation("GOAWAY 必须关联流 0")
            _, code, _ = unpack_goaway(frame.payload)
            log.info("收到客户端 GOAWAY code=%d，开始排空关闭", code)
            await asyncio.gather(
                *(st.sender for st in self.streams.values() if st.sender),
                return_exceptions=True,
            )
            await self.shutdown(F.NO_ERROR, "client goaway")
            return

        if frame.ftype == WINDOW_UPDATE:
            inc = unpack_window_update(frame.payload)
            if sid == 0:
                self.conn_send_window.apply_update(inc)  # 溢出抛 FLOW_CONTROL_ERROR
            else:
                st = self.streams.get(sid)
                if st and not st.reset:
                    st.send_window.apply_update(inc)
            async with self._credit_cond:
                self._credit_cond.notify_all()
            return

        if sid == 0 or sid % 2 == 0:
            raise ProtocolViolation(f"帧 {frame.name} 的流标识符非法: {sid}")

        if frame.ftype == HEADERS:
            await self._handle_headers(frame, decoder)
        elif frame.ftype == DATA:
            await self._handle_request_data(frame)
        elif frame.ftype == RST_STREAM:
            code = unpack_rst(frame.payload)
            st = self.streams.get(sid)
            if st:
                log.info("流 %d 被 RST(code=%d)，停止发送协程", sid, code)
                st.reset = True
                if st.sender and not st.sender.done():
                    st.sender.cancel()
        else:
            raise ProtocolViolation(f"未实现的帧类型: {frame.name}")

    async def _handle_headers(self, frame: Frame, decoder: HpackDecoder) -> None:
        if not frame.has(F.END_HEADERS):
            raise ProtocolViolation("本对端不接受 CONTINUATION 分片")
        payload = frame.payload
        if frame.has(F.PADDED):
            pad_len = payload[0]
            payload = payload[1 : len(payload) - pad_len]
        if frame.has(F.PRIORITY_FLAG):
            payload = payload[5:]
        headers = decoder.decode(payload)

        method = path = None
        for name, value in headers:
            if name == b":method":
                method = value
            elif name == b":path":
                path = value
        if method != b"GET" or path is None:
            raise ProtocolViolation("仅支持 GET 请求")
        if frame.stream_id in self.streams:
            raise ProtocolViolation("流标识符被重复使用")

        # 路径 /stream/<sid>/<size> 决定载荷大小；默认 128 KiB
        size = 128 * 1024
        parts = path.lstrip(b"/").split(b"/")
        if len(parts) >= 3 and parts[0] == b"stream":
            try:
                size = int(parts[2])
            except ValueError:
                raise ProtocolViolation("路径中的大小非法")

        st = _ServerStream(frame.stream_id, headers, size)
        st.send_window.credit = self.peer_iws  # 以对端公布的初始窗口为准
        self.streams[frame.stream_id] = st
        self.highest_sid = max(self.highest_sid, frame.stream_id)
        st.sender = asyncio.create_task(self._send_response(st))

    async def _handle_request_data(self, frame: Frame) -> None:
        st = self.streams.get(sid := frame.stream_id)
        if st is None or st.reset:
            return
        payload = frame.payload
        if frame.has(F.PADDED):
            pad_len = payload[0]
            payload = payload[1 : len(payload) - pad_len]
        n = len(payload)
        # 请求体同样受流控；丢弃即消费，立即返还窗口
        self.conn_recv_window.charge(n)
        st.recv_window.charge(n)
        self.conn_recv_window.consume(n)
        st.recv_window.consume(n)
        updates: list[bytes] = []
        if inc := self.conn_recv_window.take_update():
            updates.append(pack_frame(WINDOW_UPDATE, 0, 0, pack_window_update(inc)))
        if inc := st.recv_window.take_update():
            updates.append(pack_frame(WINDOW_UPDATE, 0, st.sid, pack_window_update(inc)))
        if updates:
            await self._write(b"".join(updates))
        if frame.has(END_STREAM):
            st.ended = True

    # ---- 响应发送（双窗口流控）------------------------------------------

    async def _send_response(self, st: _ServerStream) -> None:
        try:
            resp_headers = encode_headers(
                [
                    (b":status", b"200"),
                    (b"content-type", b"application/octet-stream"),
                    (b"content-length", str(st.content_length).encode()),
                    (b"x-stream-id", str(st.sid).encode()),
                ]
            )
            await self._write(pack_frame(HEADERS, F.END_HEADERS, st.sid, resp_headers))

            sent = 0
            while sent < st.content_length:
                # 等待本流或连接窗口出现额度；条件广播保证所有流协程都被唤醒
                async with self._credit_cond:
                    await self._credit_cond.wait_for(
                        lambda: st.reset
                        or (
                            st.send_window.credit > 0
                            and self.conn_send_window.credit > 0
                        )
                    )
                if st.reset:
                    return
                n = min(
                    self.peer_max_frame,
                    st.content_length - sent,
                    max(st.send_window.credit, 0),
                    max(self.conn_send_window.credit, 0),
                )
                if n == 0:
                    continue
                # 两窗口同时占用，失败则整体不占用
                if not st.send_window.try_acquire(n) or not self.conn_send_window.try_acquire(n):
                    continue
                chunk = deterministic_payload(st.sid, sent, n)
                flags = END_STREAM if sent + n == st.content_length else 0
                await self._write(pack_frame(DATA, flags, st.sid, chunk))
                sent += n
            log.info("流 %d 完成，共 %d 字节", st.sid, sent)
        except asyncio.CancelledError:
            log.info("流 %d 发送协程因 RST 取消", st.sid)
            raise
        except Exception:
            log.exception("流 %d 发送失败", st.sid)
            await self.shutdown(F.INTERNAL_ERROR, "sender failure")
