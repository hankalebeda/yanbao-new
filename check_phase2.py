import json
import sqlite3

def check():
    db_path = 'output/audit_v24_phase1_evidence/phase2_results.db'
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT response_payload FROM phase2_results LIMIT 1")
        row = cursor.fetchone()
        if not row:
            print("No data in phase2_results")
            return
        payload = json.loads(row[0])
        capital_summary = payload.get('capital_game_summary', {})
        print('Fields in capital_game_summary:', list(capital_summary.keys()))
        for k in ['headline', 'summary_text', 'missing_reasons']:
            v = capital_summary.get(k)
            print(f' - {k}: {"OK" if v else "MISSING/EMPTY"}')
        conn.close()
    except Exception as e:
        print(f'Error: {e}')

if __name__ == "__main__":
    check()
