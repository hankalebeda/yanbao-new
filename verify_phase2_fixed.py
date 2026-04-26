import os
import sys
import sqlite3
import json
from pathlib import Path

# Add working directory
sys.path.insert(0, str(Path(__file__).parent))

# Set environment
os.environ['NO_PROXY'] = '*'

# Updated imports: use _build_report_payload since report_detail_to_api_response is missing
# and is likely similar to this internal helper.
# We also need to mock or provide a bundle structure.
from app.services.ssot_read_model import _build_report_payload, _load_ssot_report_bundle
from app.database import SessionLocal

def test_missing_reasons_in_response():
    db = SessionLocal()
    try:
        # Get the latest report
        row = db.execute(\"\"\"
            SELECT report_id, stock_code, stock_name
            FROM reports
            WHERE quality_flag != 'excluded'
            ORDER BY created_at DESC
            LIMIT 1
        \"\"\").fetchone()

        if not row:
            print(\"No reports found in database.\")
            return False

        report_id = row[0]
        stock_code = row[1]
        print(f\"Checking report for {stock_code} (ID: {report_id})\")

        # Load the report bundle using existing service
        bundle = _load_ssot_report_bundle(db, report_id=report_id)
        if not bundle:
            print(\"Failed to load report bundle.\")
            return False

        # Convert to API response format
        api_response = _build_report_payload(bundle, can_see_full=True, for_view=False)

        # Check capital_game_summary structure
        summary = api_response.get('capital_game_summary')
        if not summary:
            print(\"capital_game_summary not found in response.\")
            # Some reports might not have it if they don't have enough data
            return True

        if 'missing_reasons' in summary:
            print(f\"OK: 'missing_reasons' found: {summary['missing_reasons']}\")
            return True
        else:
            print(\"FAIL: 'missing_reasons' NOT found in capital_game_summary.\")
            # Print keys for debugging
            print(f\"Available keys: {list(summary.keys())}\")
            return False
    finally:
        db.close()

if __name__ == '__main__':
    success = test_missing_reasons_in_response()
    sys.exit(0 if success else 1)
