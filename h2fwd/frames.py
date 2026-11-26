"""HTTP/2 帧层：帧类型/标志常量、线格式读写、协议异常。

仅实现练习所需的 h2c（明文 prior knowledge）子集，线格式严格遵循 RFC 7540。
"""

from __future__ import annotations

import asyncio
import struct
from dataclasses import dataclass

# ---- 帧类型 (RFC 7540 §11.2) ----
DATA = 0x0
HEADERS = 0x1
PRIORITY = 0x2
RST_STREAM = 0x3
SETTINGS = 0x4
PUSH_PROMISE = 0x5
PING = 0x6
GOAWAY = 0x7
WINDOW_UPDATE = 0x8
CONTINUATION = 0x9

FRAME_NAMES = {
    DATA: "DATA",
    HEADERS: "HEADERS",
    RST_STREAM: "RST_STREAM",
    SETTINGS: "SETTINGS",
    PING: "PING",
    GOAWAY: "GOAWAY",
    WINDOW_UPDATE: "WINDOW_UPDATE",
    CONTINUATION: "CONTINUATION",
}

# ---- 标志位 ----
END_STREAM = 0x1
ACK = 0x1
END_HEADERS = 0x4
PADDED = 0x8
PRIORITY_FLAG = 0x20

# ---- 协议常量 ----
CONNECTION_PREFACE = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
DEFAULT_FRAME_SIZE = 16_384
MAX_FRAME_SIZE_LIMIT = 16_777_215
INITIAL_WINDOW_SIZE = 65_535
MAX_WINDOW = 2**31 - 1  # 流控窗口上限 2^31-1

# SETTINGS 标识符
SETTINGS_HEADER_TABLE_SIZE = 1
SETTINGS_ENABLE_PUSH = 2
SETTINGS_MAX_CONCURRENT_STREAMS = 3
SETTINGS_INITIAL_WINDOW_SIZE = 4
SETTINGS_MAX_FRAME_SIZE = 5
SETTINGS_MAX_HEADER_LIST_SIZE = 6

# ---- 错误码 (RFC 7540 §11.4) ----
NO_ERROR = 0
PROTOCOL_ERROR = 1
INTERNAL_ERROR = 2
FLOW_CONTROL_ERROR = 3
SETTINGS_TIMEOUT = 4
STREAM_CLOSED = 5
FRAME_SIZE_ERROR = 6
REFUSED_STREAM = 7
CANCEL = 8
COMPRESSION_ERROR = 9
CONNECT_ERROR = 10
ENHANCE_YOUR_CALM = 11
INADEQUATE_SECURITY = 12
HTTP_1_1_REQUIRED = 13


class H2Error(Exception):
    """携带 GOAWAY/RST 错误码的协议异常。"""

    def __init__(self, code: int, message: str):
        super().__init__(message)
        self.code = code


class ProtocolViolation(H2Error):
    def __init__(self, message: str, code: int = PROTOCOL_ERROR):
        super().__init__(code, message)


class FlowControlViolation(H2Error):
    def __init__(self, message: str):
        super().__init__(FLOW_CONTROL_ERROR, message)


@dataclass
class Frame:
    ftype: int
    flags: int
    stream_id: int
    payload: bytes

    @property
    def name(self) -> str:
        return FRAME_NAMES.get(self.ftype, f"0x{self.ftype:x}")

    def has(self, flag: int) -> bool:
        return bool(self.flags & flag)


def pack_frame(ftype: int, flags: int, stream_id: int, payload: bytes = b"") -> bytes:
    """序列化一个 HTTP/2 帧（stream_id 的保留位被强制清零）。

    帧头固定 9 字节：长度(3 字节大端) + 类型(1) + 标志(1) + 流标识符(4)。
    """
    if len(payload) > MAX_FRAME_SIZE_LIMIT:
        raise ValueError("frame payload too large")
    header = (
        len(payload).to_bytes(3, "big")
        + bytes([ftype, flags])
        + struct.pack(">I", stream_id & 0x7FFFFFFF)
    )
    return header + payload


def pack_window_update(increment: int) -> bytes:
    if not 1 <= increment <= MAX_WINDOW:
        raise ValueError(f"illegal window increment {increment}")
    return struct.pack(">I", increment & 0x7FFFFFFF)


def unpack_window_update(payload: bytes) -> int:
    if len(payload) != 4:
        raise ProtocolViolation("WINDOW_UPDATE 载荷必须为 4 字节", FRAME_SIZE_ERROR)
    return struct.unpack(">I", payload)[0] & 0x7FFFFFFF


def unpack_rst(payload: bytes) -> int:
    if len(payload) != 4:
        raise ProtocolViolation("RST_STREAM 载荷必须为 4 字节", FRAME_SIZE_ERROR)
    return struct.unpack(">I", payload)[0] & 0x7FFFFFFF


def pack_goaway(last_stream_id: int, error_code: int, debug: bytes = b"") -> bytes:
    return struct.pack(">II", last_stream_id & 0x7FFFFFFF, error_code) + debug


def unpack_goaway(payload: bytes) -> tuple[int, int, bytes]:
    if len(payload) < 8:
        raise ProtocolViolation("GOAWAY 载荷过短", FRAME_SIZE_ERROR)
    last_sid, code = struct.unpack(">II", payload[:8])
    return last_sid & 0x7FFFFFFF, code, payload[8:]


def unpack_settings(payload: bytes) -> list[tuple[int, int]]:
    if len(payload) % 6 != 0:
        raise ProtocolViolation("SETTINGS 载荷长度必须为 6 的倍数", FRAME_SIZE_ERROR)
    pairs = []
    for off in range(0, len(payload), 6):
        sid, value = struct.unpack(">HI", payload[off : off + 6])
        pairs.append((sid, value))
    return pairs


def pack_settings(pairs: dict[int, int] | list[tuple[int, int]]) -> bytes:
    items = pairs.items() if isinstance(pairs, dict) else pairs
    return b"".join(struct.pack(">HI", sid, value) for sid, value in items)


async def read_frame(reader: asyncio.StreamReader) -> Frame | None:
    """读取一帧；连接干净关闭时返回 None。"""
    header = await reader.read(9)
    if header == b"":
        return None
    if len(header) < 9:
        raise ProtocolViolation("连接在帧头中途关闭", FRAME_SIZE_ERROR)
    length = int.from_bytes(header[:3], "big")
    ftype = header[3]
    flags = header[4]
    raw_sid = struct.unpack(">I", header[5:9])[0]
    sid = raw_sid & 0x7FFFFFFF
    if length > MAX_FRAME_SIZE_LIMIT:
        raise ProtocolViolation("帧长度超过 2^24-1", FRAME_SIZE_ERROR)
    payload = await reader.read(length) if length else b""
    if len(payload) != length:
        raise ProtocolViolation("连接在帧载荷中途关闭", FRAME_SIZE_ERROR)
    return Frame(ftype, flags, sid, payload)


async def read_preface(reader: asyncio.StreamReader) -> None:
    data = await reader.readexactly(len(CONNECTION_PREFACE))
    if data != CONNECTION_PREFACE:
        raise ProtocolViolation("HTTP/2 连接前导不匹配")
