"""最小 HPACK 实现（RFC 7541）。

- 解码：完整支持整数/字符串原语、索引字段、字面量（带/不带索引）、动态表大小更新；
  对端发来的 Huffman 字符串会被拒绝（本练习器只使用原始字符串）。
- 编码：仅使用静态表索引 + 不带索引的原始字符串字面量，足以承载练习中的请求与响应。
"""

from __future__ import annotations

from .frames import COMPRESSION_ERROR, H2Error

# RFC 7541 Appendix A 静态表（共 61 项，索引从 1 开始）
STATIC_TABLE: list[tuple[bytes, bytes | None]] = [
    (b":authority", None),
    (b":method", b"GET"),
    (b":method", b"POST"),
    (b":path", b"/"),
    (b":path", b"/index.html"),
    (b":scheme", b"http"),
    (b":scheme", b"https"),
    (b":status", b"200"),
    (b":status", b"204"),
    (b":status", b"206"),
    (b":status", b"304"),
    (b":status", b"400"),
    (b":status", b"404"),
    (b":status", b"500"),
    (b"accept-charset", None),
    (b"accept-encoding", b"gzip, deflate"),
    (b"accept-language", None),
    (b"accept-ranges", None),
    (b"accept", None),
    (b"access-control-allow-origin", None),
    (b"age", None),
    (b"allow", None),
    (b"authorization", None),
    (b"cache-control", None),
    (b"content-disposition", None),
    (b"content-encoding", None),
    (b"content-language", None),
    (b"content-length", None),
    (b"content-location", None),
    (b"content-range", None),
    (b"content-type", None),
    (b"cookie", None),
    (b"date", None),
    (b"etag", None),
    (b"expect", None),
    (b"expires", None),
    (b"from", None),
    (b"host", None),
    (b"if-match", None),
    (b"if-modified-since", None),
    (b"if-none-match", None),
    (b"if-range", None),
    (b"if-unmodified-since", None),
    (b"last-modified", None),
    (b"link", None),
    (b"location", None),
    (b"max-forwards", None),
    (b"proxy-authenticate", None),
    (b"proxy-authorization", None),
    (b"range", None),
    (b"referer", None),
    (b"refresh", None),
    (b"retry-after", None),
    (b"server", None),
    (b"set-cookie", None),
    (b"strict-transport-security", None),
    (b"transfer-encoding", None),
    (b"user-agent", None),
    (b"vary", None),
    (b"via", None),
    (b"www-authenticate", None),
]

_STATIC_INDEX: dict[tuple[bytes, bytes | None], int] = {
    pair: i + 1 for i, pair in enumerate(STATIC_TABLE)
}
_NAME_INDEX: dict[bytes, int] = {}
for _i, (_name, _value) in enumerate(STATIC_TABLE):
    _NAME_INDEX.setdefault(_name, _i + 1)


class HpackError(H2Error):
    def __init__(self, message: str):
        super().__init__(COMPRESSION_ERROR, message)


def _entry_size(name: bytes, value: bytes) -> int:
    return len(name) + len(value) + 32


def encode_integer(value: int, prefix_bits: int, first_byte_high: int) -> bytes:
    """HPACK 整数编码（first_byte_high 为第一字节的高位模式）。"""
    limit = (1 << prefix_bits) - 1
    if value < limit:
        return bytes([first_byte_high | value])
    out = [first_byte_high | limit]
    value -= limit
    while value >= 128:
        out.append((value % 128) | 0x80)
        value //= 128
    out.append(value)
    return bytes(out)


def decode_integer(data: bytes, offset: int, prefix_bits: int) -> tuple[int, int]:
    """返回 (整数, 新偏移)。"""
    if offset >= len(data):
        raise HpackError("HPACK 整数越界")
    limit = (1 << prefix_bits) - 1
    value = data[offset] & limit
    pos = offset + 1
    if value < limit:
        return value, pos
    shift = 0
    while True:
        if pos >= len(data):
            raise HpackError("HPACK 整数截断")
        byte = data[pos]
        pos += 1
        value += (byte & 0x7F) << shift
        if not byte & 0x80:
            break
        shift += 7
        if shift > 63:
            raise HpackError("HPACK 整数过长")
    return value, pos


def _encode_string(raw: bytes) -> bytes:
    # 原始字符串：H 位清零
    return encode_integer(len(raw), 7, 0x00) + raw


def _decode_string(data: bytes, offset: int) -> tuple[bytes, int]:
    if offset >= len(data):
        raise HpackError("HPACK 字符串截断")
    huffman = bool(data[offset] & 0x80)
    length, pos = decode_integer(data, offset, 7)
    if pos + length > len(data):
        raise HpackError("HPACK 字符串长度越界")
    raw = data[pos : pos + length]
    if huffman:
        # 本练习器的受控对端不产生 Huffman 编码
        raise HpackError("不支持 Huffman 编码的头部字段")
    return raw, pos + length


class HpackDecoder:
    """有状态 HPACK 解码器（维护动态表）。"""

    def __init__(self, max_dynamic_size: int = 4096):
        self._dynamic: list[tuple[bytes, bytes]] = []
        self._dynamic_size = 0
        self._max_size = max_dynamic_size

    def set_max_size(self, max_size: int) -> None:
        self._max_size = max_size
        self._evict_to_fit(0)

    def _evict_to_fit(self, incoming: int) -> None:
        while self._dynamic and self._dynamic_size + incoming > self._max_size:
            name, value = self._dynamic.pop()  # 动态表尾部是最旧条目
            self._dynamic_size -= _entry_size(name, value)

    def _table_get(self, index: int) -> tuple[bytes, bytes | None]:
        if index <= 0:
            raise HpackError("HPACK 索引必须 >= 1")
        if index <= len(STATIC_TABLE):
            return STATIC_TABLE[index - 1]
        dyn_idx = index - len(STATIC_TABLE) - 1
        if dyn_idx < len(self._dynamic):
            return self._dynamic[dyn_idx]  # 动态表头部是最新条目
        raise HpackError(f"HPACK 索引 {index} 越界")

    def _add_dynamic(self, name: bytes, value: bytes) -> None:
        size = _entry_size(name, value)
        self._evict_to_fit(size)
        if size <= self._max_size:
            self._dynamic.insert(0, (name, value))
            self._dynamic_size += size

    def decode(self, block: bytes) -> list[tuple[bytes, bytes]]:
        headers: list[tuple[bytes, bytes]] = []
        pos = 0
        while pos < len(block):
            byte = block[pos]
            if byte & 0x80:  # 索引头部字段
                index, pos = decode_integer(block, pos, 7)
                name, value = self._table_get(index)
                if value is None:
                    raise HpackError(f"静态表索引 {index} 的值为空，不能整体索引")
                headers.append((name, value))
            elif byte & 0x40:  # 带索引的字面量
                index, pos = decode_integer(block, pos, 6)
                if index:
                    name, _ = self._table_get(index)
                else:
                    name, pos = _decode_string(block, pos)
                value, pos = _decode_string(block, pos)
                headers.append((name, value))
                self._add_dynamic(name, value)
            elif byte & 0x20:  # 动态表大小更新
                new_size, pos = decode_integer(block, pos, 5)
                self.set_max_size(new_size)
            else:  # 不带索引（0x00 前缀 4 位）/ 绝不索引（0x10）
                index, pos = decode_integer(block, pos, 4)
                if index:
                    name, _ = self._table_get(index)
                else:
                    name, pos = _decode_string(block, pos)
                value, pos = _decode_string(block, pos)
                headers.append((name, value))
        return headers


def encode_headers(headers: list[tuple[bytes, bytes]]) -> bytes:
    """用「静态表索引或不带索引字面量」编码头部块。"""
    out = bytearray()
    for name, value in headers:
        exact = _STATIC_INDEX.get((name, value))
        if exact is not None:
            out += encode_integer(exact, 7, 0x80)
            continue
        name_ref = _NAME_INDEX.get(name)
        if name_ref is not None:
            # 字面量、不带索引（0000 高位），名称用静态表索引
            out += encode_integer(name_ref, 4, 0x00)
        else:
            out += b"\x00"
            out += _encode_string(name)
        out += _encode_string(value)
    return bytes(out)
