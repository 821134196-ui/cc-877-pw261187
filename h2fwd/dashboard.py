"""并行下载的实时窗口/进度仪表盘。"""

from __future__ import annotations

import asyncio
import sys

from .forwarder import H2Forwarder

_BAR_WIDTH = 22


def _bar(progress: float) -> str:
    progress = min(max(progress, 0.0), 1.0)
    filled = int(progress * _BAR_WIDTH)
    return "█" * filled + "░" * (_BAR_WIDTH - filled)


def _human(n: int) -> str:
    for unit in ("B", "KiB", "MiB", "GiB"):
        if n < 1024 or unit == "GiB":
            return f"{n:7.1f}{unit}" if unit != "B" else f"{n:7d} B"
        n /= 1024
    return str(n)


_STATUS_MARK = {
    "open": "运行中",
    "done": "完成",
    "reset": "已重置",
    "failed": "失败",
}


def render(fwd: H2Forwarder) -> str:
    snap = fwd.snapshot()
    lines = []
    lines.append("HTTP/2 帧转发练习器 — 并行流窗口/进度")
    lines.append(
        f"已运行 {snap['elapsed']:6.2f}s | 连接接收窗口: 可用 {snap['conn_window_available']:>10d} "
        f"待返还 {snap['conn_window_pending']:>8d} | "
        f"DATA帧 {snap['stats']['data_frames']} WINDOW_UPDATE {snap['stats']['window_updates_sent']} "
        f"RST 发/收 {snap['stats']['rst_sent']}/{snap['stats']['rst_received']} "
        f"窗口夹取 {snap['stats']['clamps']}"
    )
    lines.append("─" * 108)
    lines.append(
        f"{'流':>4} {'状态':<6} {'流窗口可用':>10} {'待返还':>8} {'缓冲占用':>10} "
        f"{'进度':<{_BAR_WIDTH + 9}} {'已接收':>10} {'已输出':>10} {'耗时':>7}"
    )
    for s in snap["streams"]:
        expected = s["expected"] or s["total"] or 1
        progress = s["written"] / expected
        mark = _STATUS_MARK.get(s["status"], s["status"])
        lines.append(
            f"{s['sid']:>4} {mark:<6} {s['available']:>10d} {s['pending']:>8d} "
            f"{s['buffered']:>10d} {_bar(progress)} {progress * 100:5.1f}% "
            f"{s['written']:>10d} {s['total']:>10d} {s['elapsed']:>6.2f}s"
        )
    lines.append("─" * 108)
    if snap["events"]:
        lines.append("最近事件:")
        for t, msg in snap["events"]:
            lines.append(f"  [t={t:6.2f}s] {msg}")
    if snap["goaway"]:
        lines.append(f"!! 连接已终止 GOAWAY code={snap['goaway'][0]}: {snap['goaway'][1]}")
    return "\n".join(lines)


class Dashboard:
    def __init__(self, fwd: H2Forwarder, interval: float = 0.2, stream=sys.stdout):
        self.fwd = fwd
        self.interval = interval
        self.stream = stream
        self._task: asyncio.Task | None = None
        self._tty = stream.isatty()

    async def _run(self) -> None:
        while not self.fwd.conn_closed and not all(
            st.done for st in self.fwd.streams.values()
        ):
            self._paint()
            await asyncio.sleep(self.interval)
        self._paint(final=True)

    def _paint(self, final: bool = False) -> None:
        text = render(self.fwd)
        if self._tty:
            # 光标回到左上角并清屏，形成固定面板
            self.stream.write("\033[H\033[J" + text + "\n")
        else:
            self.stream.write("\n" + text + "\n")
        self.stream.flush()

    async def __aenter__(self) -> "Dashboard":
        self._task = asyncio.create_task(self._run())
        return self

    async def __aexit__(self, *exc) -> None:
        if self._task:
            await self._task
