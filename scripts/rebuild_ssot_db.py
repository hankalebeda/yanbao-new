from __future__ import annotations

import argparse
from pathlib import Path
import sqlite3
import sys

from sqlalchemy import create_engine

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.ssot_schema import build_metadata


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a SQLite database from SSOT 04 schema.")
    parser.add_argument(
        "--doc",
        default="",
        help="Optional path to docs/core/04_数据治理与血缘.md. Defaults to auto-discovery.",
    )
    parser.add_argument(
        "--output",
        default="data/app.db.next",
        help="Output SQLite path. Defaults to data/app.db.next to avoid breaking the current runtime DB.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite the output file if it already exists.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if output_path.exists():
        if not args.force:
            raise SystemExit(f"Output already exists: {output_path}. Use --force to overwrite.")
        output_path.unlink()

    metadata, specs = build_metadata(args.doc or None)
    engine = create_engine(f"sqlite:///{output_path.as_posix()}")
    metadata.create_all(bind=engine)

    with sqlite3.connect(output_path) as conn:
        created_tables = [row[0] for row in conn.execute("select name from sqlite_master where type='table' order by name")]

    expected_tables = sorted(spec.name for spec in specs)
    missing = sorted(set(expected_tables) - set(created_tables))
    extra = sorted(set(created_tables) - set(expected_tables))

    print(f"output={output_path}")
    print(f"expected_table_count={len(expected_tables)}")
    print(f"created_table_count={len(created_tables)}")
    print(f"missing_count={len(missing)}")
    print(f"extra_count={len(extra)}")

    if missing:
        print("missing_tables=")
        for table_name in missing:
            print(table_name)
    if extra:
        print("extra_tables=")
        for table_name in extra:
            print(table_name)

    if missing or extra:
        raise SystemExit(1)

    print("ssot_schema_build=ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
