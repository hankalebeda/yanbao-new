"""Full backfill: call generate_reports_batch directly for all pending stocks
across target dates. Chunks of 50 to respect endpoint limit; concurrency 3 to
avoid LLM rate issues. Writes progress log to run_full_backfill.log.
"""
import json
import sqlite3
import sys
import time
from datetime import datetime

sys.path.insert(0, 'd:/yanbao-new')

from app.core.db import SessionLocal
from app.services.report_generation_ssot import generate_reports_batch

DATES = [
    '2026-04-08', '2026-04-09', '2026-04-10',
    '2026-04-13', '2026-04-14', '2026-04-15', '2026-04-16',
]

LOG_PATH = '_archive/audit_v24_phase1_evidence/run_full_backfill.log'


def log(msg: str) -> None:
    ts = datetime.now().strftime('%H:%M:%S')
    line = f'[{ts}] {msg}'
    print(line, flush=True)
    with open(LOG_PATH, 'a', encoding='utf-8') as fh:
        fh.write(line + '\n')


def main() -> None:
    with open('_archive/audit_v24_phase1_evidence/ready_stocks.json', encoding='utf-8') as f:
        ready = json.load(f)

    c = sqlite3.connect('data/app.db')
    cur = c.cursor()

    log('=== Full backfill start ===')
    t_start = time.time()
    grand_ok = 0
    grand_fail = 0
    for td in DATES:
        stocks = ready.get(td, [])
        if not stocks:
            log(f'{td}: no ready stocks, skip')
            continue
        cur.execute(
            "SELECT stock_code FROM report WHERE trade_date=? AND is_deleted=0",
            (td,),
        )
        existing = {r[0] for r in cur.fetchall()}
        pending = [s for s in stocks if s not in existing]
        log(f'>>> {td}: pending={len(pending)} existing={len(existing)}')
        if not pending:
            continue

        date_ok = 0
        date_fail = 0
        for i in range(0, len(pending), 50):
            chunk = pending[i:i + 50]
            t0 = time.time()
            try:
                res = generate_reports_batch(
                    db_factory=SessionLocal,
                    stock_codes=chunk,
                    trade_date=td,
                    skip_pool_check=True,
                    force_same_day_rebuild=False,
                    max_concurrent_override=3,
                )
            except Exception as exc:
                log(f'  {td} chunk {i // 50 + 1}: EXCEPTION {exc!r}')
                date_fail += len(chunk)
                continue
            dt = time.time() - t0
            ok = int(res.get('succeeded') or 0)
            fail = int(res.get('failed') or 0)
            date_ok += ok
            date_fail += fail
            err_codes: dict[str, int] = {}
            for d in res.get('details', []) or []:
                if d.get('status') != 'ok':
                    k = str(d.get('error_code') or '?')
                    err_codes[k] = err_codes.get(k, 0) + 1
            log(
                f'  {td} chunk {i // 50 + 1}/{(len(pending) + 49) // 50}: '
                f'ok={ok} fail={fail} errs={err_codes} elapsed={dt:.1f}s'
            )

        grand_ok += date_ok
        grand_fail += date_fail
        log(f'<<< {td} done: ok={date_ok} fail={date_fail}')

    c.close()
    log(
        f'=== Full backfill end: ok={grand_ok} fail={grand_fail} '
        f'elapsed={(time.time() - t_start) / 60:.1f}min ==='
    )


if __name__ == '__main__':
    main()
