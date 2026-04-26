"""清除预测结算历史（prediction_outcome），便于准确率统计从零开始。研报错误/对错记录即据此表统计。"""
import sys
from pathlib import Path

# 保证可导入 app
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.core.db import SessionLocal
from app.models import PredictionOutcome


def main():
    db = SessionLocal()
    try:
        n = db.query(PredictionOutcome).delete()
        db.commit()
        print(f"已清除 prediction_outcome 表 {n} 条历史记录。")
    finally:
        db.close()


if __name__ == "__main__":
    main()
