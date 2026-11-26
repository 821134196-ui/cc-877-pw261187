"""命令行入口：python -m h2fwd <demo|serve|all>。"""

from __future__ import annotations

import argparse
import asyncio
import logging

from . import scenarios


async def _run_demo(name: str, no_dashboard: bool, **kwargs) -> None:
    runner = {
        "isolation": scenarios.run_isolation,
        "reset": scenarios.run_reset,
        "nearlimit": scenarios.run_nearlimit,
    }[name]
    use_dashboard = not no_dashboard
    print(f"\n=== 场景: {name} ===\n")
    result = await runner(use_dashboard=use_dashboard, **kwargs)
    print("\n--- 结果 ---")
    print(result["message"])
    stats = result["snapshot"]["stats"]
    print(
        f"DATA 帧 {stats['data_frames']} | WINDOW_UPDATE {stats['window_updates_sent']} "
        f"({stats['window_update_bytes']} 字节) | RST 发/收 "
        f"{stats['rst_sent']}/{stats['rst_received']} | 窗口夹取 {stats['clamps']} 次"
    )
    assert result["ok"]


async def amain() -> None:
    parser = argparse.ArgumentParser(prog="h2fwd", description="HTTP/2 帧转发练习器")
    parser.add_argument(
        "command", choices=["isolation", "reset", "nearlimit", "all", "serve"]
    )
    parser.add_argument("--no-dashboard", action="store_true", help="只在结束时打印一次快照")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=0, help="serve 模式监听端口（0=随机）")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if args.command == "serve":
        from .peer import ControlledPeer

        peer = ControlledPeer(args.host, args.port)
        host, port = await peer.start()
        print(f"受控 HTTP/2 对端已启动：h2c://{host}:{port}（Ctrl+C 退出）")
        try:
            await asyncio.Event().wait()
        except (KeyboardInterrupt, asyncio.CancelledError):
            pass
        finally:
            await peer.aclose()
        return

    names = ["isolation", "reset", "nearlimit"] if args.command == "all" else [args.command]
    for name in names:
        await _run_demo(name, args.no_dashboard)
        print()


def main() -> None:
    try:
        asyncio.run(amain())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
