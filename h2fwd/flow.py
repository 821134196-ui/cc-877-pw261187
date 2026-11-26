"""HTTP/2 流控窗口的簿记（纯同步逻辑，便于单测）。

两类窗口：

- ReceivingWindow：本端拥有的接收窗口。DATA 到达时扣减，数据被上层消费后
  通过 take_update() 产出 WINDOW_UPDATE 增量；临近 2^31-1 上限时增量被
  「夹住」，未返还的字节留在 pending 里，等窗口重新有空间再发。
- SendingWindow：对端拥有、本端记账的发送窗口。收到 WINDOW_UPDATE 时
  校验溢出（超过 2^31-1 即 FLOW_CONTROL_ERROR），SETTINGS 初始窗口调整
  允许把余额变成负数（RFC 7540 §6.9.2）。
"""

from __future__ import annotations

from .frames import INITIAL_WINDOW_SIZE, MAX_WINDOW, FlowControlViolation


class ReceivingWindow:
    def __init__(self, initial: int = INITIAL_WINDOW_SIZE, limit: int = MAX_WINDOW):
        if not 0 <= initial <= limit:
            raise ValueError("initial window out of range")
        if limit <= 0 or limit > MAX_WINDOW:
            raise ValueError("window limit out of range")
        self.available = initial          # 当前剩余接收额度
        self.limit = limit
        self._pending = 0                 # 已消费、等待 WINDOW_UPDATE 返还的字节

    @property
    def pending(self) -> int:
        return self._pending

    def charge(self, n: int) -> None:
        """DATA 帧占用 n 字节接收额度；超额即对端违约。"""
        if n < 0:
            raise ValueError("negative charge")
        if n > self.available:
            raise FlowControlViolation(
                f"DATA 超出接收窗口（{n} > 可用 {self.available}）"
            )
        self.available -= n

    def consume(self, n: int) -> None:
        """上层取走 n 字节缓冲，记为待返还。"""
        if n < 0:
            raise ValueError("negative consume")
        self._pending += n

    def take_update(self) -> int:
        """取一个可发送的 WINDOW_UPDATE 增量；无空间或无待返还时返回 0。

        饱和运算：增量绝不使 available 超过 limit，剩余 pending 保留，
        等后续 DATA 腾出空间后再发——这就是「窗口临近上限」的处理路径。
        """
        room = self.limit - self.available
        inc = min(self._pending, room)
        if inc <= 0:
            return 0
        self._pending -= inc
        self.available += inc
        return inc

    def apply_initial_delta(self, delta: int) -> None:
        """处理 SETTINGS_INITIAL_WINDOW_SIZE 变更（RFC 7540 §6.9.2）。"""
        new_value = self.available + delta
        if new_value > self.limit:
            raise FlowControlViolation(
                "SETTINGS_INITIAL_WINDOW_SIZE 使接收窗口超过 2^31-1"
            )
        self.available = new_value


class SendingWindow:
    def __init__(self, initial: int = INITIAL_WINDOW_SIZE):
        self.credit = initial             # 对端授予的发送额度，可能为负

    def apply_update(self, increment: int) -> None:
        """对端 WINDOW_UPDATE 到达；任何溢出都属于 FLOW_CONTROL_ERROR。"""
        if increment < 1:
            raise FlowControlViolation("WINDOW_UPDATE 增量必须 >= 1")
        if self.credit > MAX_WINDOW - increment:
            raise FlowControlViolation(
                f"发送窗口溢出（当前 {self.credit} + 增量 {increment} > 2^31-1）"
            )
        self.credit += increment

    def apply_initial_delta(self, delta: int) -> None:
        new_value = self.credit + delta
        if new_value > MAX_WINDOW:
            raise FlowControlViolation(
                "SETTINGS_INITIAL_WINDOW_SIZE 使发送窗口超过 2^31-1"
            )
        self.credit = new_value

    def try_acquire(self, n: int) -> bool:
        if n < 0:
            raise ValueError("negative acquire")
        if self.credit >= n:
            self.credit -= n
            return True
        return False
