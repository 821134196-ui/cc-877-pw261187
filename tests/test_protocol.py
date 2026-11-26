"""协议原语的纯单元测试：帧线格式、HPACK、流控窗口饱和运算。"""

import struct
import unittest

from h2fwd import frames as F
from h2fwd.flow import ReceivingWindow, SendingWindow
from h2fwd.hpack import (
    HpackDecoder,
    HpackError,
    decode_integer,
    encode_headers,
    encode_integer,
)
from h2fwd.frames import (
    DATA,
    END_STREAM,
    FLOW_CONTROL_ERROR,
    GOAWAY,
    HEADERS,
    MAX_WINDOW,
    PING,
    RST_STREAM,
    SETTINGS,
    WINDOW_UPDATE,
    FlowControlViolation,
    H2Error,
    pack_frame,
    pack_goaway,
    pack_window_update,
    unpack_goaway,
    unpack_rst,
    unpack_settings,
    unpack_window_update,
)


class FrameTests(unittest.TestCase):
    def test_header_is_exactly_nine_bytes(self):
        raw = pack_frame(DATA, END_STREAM, 0x7FFFFFFF, b"hello")
        self.assertEqual(len(raw), 9 + 5)
        self.assertEqual(int.from_bytes(raw[:3], "big"), 5)
        self.assertEqual(raw[3], DATA)
        self.assertEqual(raw[4], END_STREAM)
        self.assertEqual(struct.unpack(">I", raw[5:9])[0], 0x7FFFFFFF)
        self.assertEqual(raw[9:], b"hello")

    def test_stream_id_reserved_bit_cleared(self):
        raw = pack_frame(PING, 0, 0xFFFFFFFF, b"12345678")
        self.assertEqual(struct.unpack(">I", raw[5:9])[0], 0x7FFFFFFF)

    def test_window_update_roundtrip(self):
        raw = pack_window_update(MAX_WINDOW)
        self.assertEqual(unpack_window_update(raw), MAX_WINDOW)

    def test_window_update_rejects_zero(self):
        with self.assertRaises(ValueError):
            pack_window_update(0)

    def test_rst_and_goaway_roundtrip(self):
        self.assertEqual(unpack_rst(struct.pack(">I", FLOW_CONTROL_ERROR)), FLOW_CONTROL_ERROR)
        raw = pack_goaway(13, FLOW_CONTROL_ERROR, b"boom")
        sid, code, debug = unpack_goaway(raw)
        self.assertEqual((sid, code, debug), (13, FLOW_CONTROL_ERROR, b"boom"))

    def test_settings_must_be_multiple_of_six(self):
        with self.assertRaises(H2Error):
            unpack_settings(b"\x00" * 7)

    def test_settings_roundtrip(self):
        pairs = [(4, 1 << 30), (5, 16384)]
        self.assertEqual(unpack_settings(F.pack_settings(pairs)), pairs)


class HpackTests(unittest.TestCase):
    def test_integer_encoding_edges(self):
        # 前缀内 / 单字节扩展 / 多字节扩展
        self.assertEqual(encode_integer(10, 5, 0x00), b"\x0a")
        self.assertEqual(decode_integer(encode_integer(10, 5, 0x00), 0, 5), (10, 1))
        self.assertEqual(encode_integer(1337, 5, 0x00), b"\x1f\x9a\x0a")
        self.assertEqual(decode_integer(b"\x1f\x9a\x0a", 0, 5), (1337, 3))

    def test_request_header_roundtrip(self):
        headers = [
            (b":method", b"GET"),
            (b":path", b"/stream/0/1024"),
            (b":scheme", b"http"),
            (b":authority", b"127.0.0.1:8080"),
        ]
        block = encode_headers(headers)
        decoded = HpackDecoder().decode(block)
        self.assertEqual(decoded, headers)

    def test_response_headers_with_literal_values(self):
        headers = [
            (b":status", b"200"),
            (b"content-length", b"4096"),
            (b"x-stream-id", b"7"),
        ]
        decoded = HpackDecoder().decode(encode_headers(headers))
        self.assertEqual(decoded, headers)

    def test_dynamic_table_literal_with_indexing(self):
        # 手工构造「带索引字面量」(0x40 前缀 6 位，名称索引 38=host) + 原始值
        block = b"\x66" + bytes([7]) + b"example"  # 0x40|38
        dec = HpackDecoder()
        first = dec.decode(block)
        self.assertEqual(first, [(b"host", b"example")])
        # 动态表第一项的索引是 61+1=62；索引字段编码为 0x80|62 = 0xBE
        again = dec.decode(bytes([0xBE]))
        self.assertEqual(again, [(b"host", b"example")])

    def test_huffman_rejected(self):
        # H=1 的字符串头
        block = b"\x00\x83\xaa"
        with self.assertRaises(HpackError):
            HpackDecoder().decode(block)

    def test_index_out_of_range(self):
        with self.assertRaises(HpackError):
            HpackDecoder().decode(b"\xff")  # 索引 127，越界


class ReceivingWindowTests(unittest.TestCase):
    def test_charge_beyond_available_is_violation(self):
        w = ReceivingWindow(100)
        w.charge(100)
        with self.assertRaises(FlowControlViolation):
            w.charge(1)

    def test_update_saturates_at_limit(self):
        cap = 1_000
        w = ReceivingWindow(initial=cap, limit=cap)
        w.charge(800)
        w.consume(800)
        self.assertEqual(w.take_update(), 800)          # 恢复到上限
        self.assertEqual(w.available, cap)
        # 再来一次大额消费：无空间，必须排队而不是溢出
        w.charge(600)
        w.consume(600)
        w.consume(900)  # pending 累计 1500，但只有 600 空间
        inc = w.take_update()
        self.assertEqual(inc, 600)
        self.assertEqual(w.available, cap)
        self.assertEqual(w.pending, 900)
        self.assertEqual(w.take_update(), 0)
        # DATA 腾出空间后，排队余额继续返还
        w.charge(400)
        self.assertEqual(w.take_update(), 400)
        self.assertEqual(w.pending, 500)

    def test_initial_window_delta_overflow(self):
        w = ReceivingWindow(MAX_WINDOW - 10)
        with self.assertRaises(FlowControlViolation):
            w.apply_initial_delta(11)
        # 负 delta 允许，甚至可为负余额
        w.apply_initial_delta(-MAX_WINDOW)
        self.assertEqual(w.available, -10)


class SendingWindowTests(unittest.TestCase):
    def test_update_overflow_rejected(self):
        w = SendingWindow(65_535)
        with self.assertRaises(FlowControlViolation):
            w.apply_update(MAX_WINDOW)  # 65535 + 2^31-1 必然溢出

    def test_zero_and_negative_increment_rejected(self):
        w = SendingWindow()
        with self.assertRaises(FlowControlViolation):
            w.apply_update(0)

    def test_credit_acquire_and_delta(self):
        w = SendingWindow(100)
        self.assertTrue(w.try_acquire(60))
        self.assertFalse(w.try_acquire(41))
        w.apply_update(10)
        self.assertTrue(w.try_acquire(50))
        self.assertEqual(w.credit, 0)
        w.apply_initial_delta(-50)
        self.assertEqual(w.credit, -50)
        self.assertFalse(w.try_acquire(1))


if __name__ == "__main__":
    unittest.main()
