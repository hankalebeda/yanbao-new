#!/usr/bin/env python
"""CLI entry point for the Multi-Agent Escort Team.

Usage::

    # Start team with auto-detected config
    python -m automation.agents.run_team

    # Start with explicit repo root
    python -m automation.agents.run_team --repo-root D:\\yanbao

    # Dry-run: show config without starting
    python -m automation.agents.run_team --dry-run
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import signal
import sys
from pathlib import Path

# Ensure package importable from repo root
_repo_root = Path(__file__).resolve().parent.parent.parent
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from automation.agents import create_team, EscortTeam  # noqa: E402

logger = logging.getLogger("automation.agents")


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Multi-Agent Escort Team")
    p.add_argument(
        "--repo-root",
        type=Path,
        default=_repo_root,
        help="Repository root (default: auto-detect)",
    )
    p.add_argument(
        "--backing-dir",
        type=Path,
        default=None,
        help="Directory for mailbox persistence",
    )
    p.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Show resolved config and exit",
    )
    return p.parse_args()


async def _run(team: EscortTeam) -> None:
    """Run the team until interrupted."""
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _signal_handler() -> None:
        logger.info("Shutdown signal received")
        stop_event.set()

    # Register signal handlers (Unix-safe; Windows may not support all)
    for sig_name in ("SIGINT", "SIGTERM"):
        sig = getattr(signal, sig_name, None)
        if sig is not None:
            try:
                loop.add_signal_handler(sig, _signal_handler)
            except NotImplementedError:
                # Windows doesn't support add_signal_handler for SIGTERM
                pass

    await team.start()
    logger.info("Escort team started — %d agents", len(team.agents))
    logger.info("Status: %s", json.dumps(team.get_status(), default=str, indent=2))

    try:
        await stop_event.wait()
    except KeyboardInterrupt:
        pass
    finally:
        logger.info("Shutting down escort team...")
        await team.shutdown("cli_stop")
        logger.info("Escort team stopped")


def main() -> None:
    args = _parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    repo_root = args.repo_root.resolve()
    backing_dir = args.backing_dir or (repo_root / "runtime" / "agents")

    team = create_team(
        repo_root=repo_root,
        backing_dir=backing_dir,
    )

    if args.dry_run:
        print("=== Escort Team Config (dry-run) ===")
        print(f"Repo root:   {repo_root}")
        print(f"Backing dir: {backing_dir}")
        print(f"Agents:      {len(team.agents)}")
        for agent in team.agents:
            print(f"  - {agent.agent_id} ({agent.role.value})")
        print(f"Service URLs: {json.dumps(team.config.service_urls, indent=2)}")
        return

    asyncio.run(_run(team))


if __name__ == "__main__":
    main()
