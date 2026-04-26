#!/usr/bin/env python3
"""Deploy systemd services to Ubuntu server via SSH.

Uploads channel-health daemon timer and escort-team service,
then enables and starts them.

Usage:
    python scripts/deploy_server_services.py [--host 192.168.232.141] [--dry-run]
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

logger = logging.getLogger("deploy_services")

SSH_HOST = os.environ.get("DEPLOY_SSH_HOST", "192.168.232.141")
SSH_PORT = int(os.environ.get("DEPLOY_SSH_PORT", "22"))
SSH_USER = os.environ.get("DEPLOY_SSH_USER", "hugh")
SSH_PASS = os.environ.get("DEPLOY_SSH_PASS", "Qwer1234")

REPO_ROOT = Path(__file__).resolve().parent.parent
SYSTEMD_DIR = REPO_ROOT / "automation" / "deploy" / "systemd"
REMOTE_SYSTEMD = "/etc/systemd/system"
REMOTE_REPO = "/home/hugh/yanbao"
REMOTE_VENV = f"{REMOTE_REPO}/.venv"

SERVICE_FILES = [
    "newapi-channel-health.service",
    "newapi-channel-health.timer",
    "escort-team.service",
]

SCRIPTS_TO_SYNC = [
    ("scripts/channel_health_daemon.py", f"{REMOTE_REPO}/scripts/channel_health_daemon.py"),
]


def deploy(dry_run: bool = False) -> None:
    try:
        import paramiko
    except ImportError:
        logger.error("paramiko is required: pip install paramiko")
        sys.exit(1)

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    logger.info("Connecting to %s@%s:%d", SSH_USER, SSH_HOST, SSH_PORT)
    client.connect(SSH_HOST, port=SSH_PORT, username=SSH_USER, password=SSH_PASS)
    sftp = client.open_sftp()

    def ssh_exec(cmd: str) -> tuple[int, str, str]:
        logger.info("  $ %s", cmd)
        if dry_run:
            return 0, "(dry-run)", ""
        stdin, stdout, stderr = client.exec_command(cmd, timeout=60)
        exit_code = stdout.channel.recv_exit_status()
        out = stdout.read().decode("utf-8", errors="replace").strip()
        err = stderr.read().decode("utf-8", errors="replace").strip()
        if out:
            logger.info("    stdout: %s", out[:500])
        if err:
            logger.info("    stderr: %s", err[:500])
        return exit_code, out, err

    # 1. Ensure remote directories exist
    ssh_exec(f"mkdir -p {REMOTE_REPO}/scripts")
    ssh_exec("sudo mkdir -p /var/log/newapi-channel-health")
    ssh_exec("sudo chown hugh:hugh /var/log/newapi-channel-health")

    # 2. Ensure venv + httpx installed
    ssh_exec(f"test -d {REMOTE_VENV} || python3 -m venv {REMOTE_VENV}")
    ssh_exec(f"{REMOTE_VENV}/bin/pip install -q httpx 2>/dev/null || true")

    # 3. Upload scripts
    for local_rel, remote_abs in SCRIPTS_TO_SYNC:
        local_path = REPO_ROOT / local_rel
        logger.info("Uploading %s → %s", local_path.name, remote_abs)
        if not dry_run:
            sftp.put(str(local_path), remote_abs)

    # 4. Upload systemd unit files
    for svc in SERVICE_FILES:
        local_path = SYSTEMD_DIR / svc
        remote_path = f"/tmp/{svc}"
        logger.info("Uploading %s → %s", svc, REMOTE_SYSTEMD)
        if not dry_run:
            sftp.put(str(local_path), remote_path)
            ssh_exec(f"sudo cp {remote_path} {REMOTE_SYSTEMD}/{svc}")
            ssh_exec(f"rm {remote_path}")

    # 5. Reload systemd
    ssh_exec("sudo systemctl daemon-reload")

    # 6. Enable and start services
    ssh_exec("sudo systemctl enable --now newapi-channel-health.timer")
    ssh_exec("sudo systemctl enable escort-team.service")  # Don't start yet

    # 7. Verify
    rc, out, _ = ssh_exec("systemctl is-active newapi-channel-health.timer")
    if "active" in out:
        logger.info("✓ newapi-channel-health.timer is active")
    else:
        logger.warning("✗ newapi-channel-health.timer not active: %s", out)

    rc, out, _ = ssh_exec("systemctl is-enabled escort-team.service")
    if "enabled" in out:
        logger.info("✓ escort-team.service is enabled (will start on boot)")
    else:
        logger.warning("✗ escort-team.service not enabled: %s", out)

    # 8. Run one health check immediately
    logger.info("Running initial health check...")
    ssh_exec(
        f"sudo -u hugh {REMOTE_VENV}/bin/python {REMOTE_REPO}/scripts/channel_health_daemon.py "
        f"--log-dir /var/log/newapi-channel-health --verbose"
    )

    sftp.close()
    client.close()
    logger.info("Deployment complete")


def main() -> None:
    parser = argparse.ArgumentParser(description="Deploy systemd services to server")
    parser.add_argument("--host", default=SSH_HOST, help="Target SSH host")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    global SSH_HOST
    SSH_HOST = args.host

    deploy(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
