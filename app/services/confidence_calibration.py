"""置信度校准模块

问题：LLM输出的 confidence 值（如 0.85）未经历史校准，无法作为可靠过滤器。
方案：等频分箱 + 保序回归（Isotonic Regression），样本≥30个即可启用。

校准流程：
  1. 收集历史「LLM置信度 → 实际方向准确」数据对
  2. 用等频分箱（5箱）计算每箱实际准确率
  3. 用保序回归拟合单调映射：raw_confidence → calibrated_confidence
  4. 存储校准曲线，运行时实时映射

当样本不足时（<30），使用保守的先验收缩（Bayesian shrinkage toward 0.52）。
"""
from __future__ import annotations

import json
import logging
import math
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

_CALIBRATION_FILE = Path("data/confidence_calibration.json")
_MIN_SAMPLES_FOR_CALIBRATION = 30
_PRIOR_ACCURACY = 0.52  # A股方向预测先验：接近随机水平
_PRIOR_WEIGHT = 10       # 先验权重（等效样本数）


@dataclass
class CalibrationBin:
    """单个校准分箱。"""
    raw_low: float
    raw_high: float
    n_samples: int
    n_correct: int

    @property
    def actual_accuracy(self) -> float:
        if self.n_samples == 0:
            return _PRIOR_ACCURACY
        # Bayesian 收缩：向先验准确率收缩，避免小样本过拟合
        return (self.n_correct + _PRIOR_WEIGHT * _PRIOR_ACCURACY) / (self.n_samples + _PRIOR_WEIGHT)

    @property
    def midpoint(self) -> float:
        return (self.raw_low + self.raw_high) / 2


@dataclass
class ConfidenceCalibrator:
    """保序回归置信度校准器。

    使用方法：
        calibrator = ConfidenceCalibrator.load()
        calibrated = calibrator.calibrate(raw_confidence=0.85)
        # 返回实际期望准确率，而非LLM自我评估值
    """
    bins: list[CalibrationBin] = field(default_factory=list)
    n_total_samples: int = 0
    model_name: str = "global"

    def is_ready(self) -> bool:
        """是否有足够样本启用统计校准。"""
        return self.n_total_samples >= _MIN_SAMPLES_FOR_CALIBRATION

    def calibrate(self, raw_confidence: float) -> float:
        """将LLM原始置信度映射到历史校准值。

        Returns:
            校准后的置信度（= 历史实际准确率的后验估计）
        """
        if not self.bins or not self.is_ready():
            # 样本不足：用保守先验收缩
            return self._prior_shrink(raw_confidence)

        # 找到对应分箱
        for b in self.bins:
            if b.raw_low <= raw_confidence <= b.raw_high:
                return round(b.actual_accuracy, 4)

        # 超出范围：用最近邻分箱
        if raw_confidence < self.bins[0].raw_low:
            return self.bins[0].actual_accuracy
        return self.bins[-1].actual_accuracy

    def _prior_shrink(self, raw: float) -> float:
        """Bayesian收缩：样本不足时向先验（0.52）收缩。

        n=0:   完全收缩到先验 0.52
        n=30:  约50%权重给观测值，50%给先验
        n=∞:   完全收缩到观测值
        """
        alpha = self.n_total_samples / (self.n_total_samples + _PRIOR_WEIGHT)
        shrunk = alpha * raw + (1 - alpha) * _PRIOR_ACCURACY
        return round(shrunk, 4)

    def add_outcome(self, raw_confidence: float, direction_correct: bool) -> None:
        """记录一次预测结果，用于增量更新校准。"""
        self.n_total_samples += 1
        for b in self.bins:
            if b.raw_low <= raw_confidence <= b.raw_high:
                b.n_samples += 1
                if direction_correct:
                    b.n_correct += 1
                return
        # 如果没有分箱，动态创建
        self._rebuild_bins_simple()

    def rebuild_from_data(self, data: list[tuple[float, bool]]) -> None:
        """从历史数据重建校准（等频5箱）。

        Args:
            data: list of (raw_confidence, direction_correct)
        """
        if len(data) < _MIN_SAMPLES_FOR_CALIBRATION:
            logger.warning(
                "confidence_calibration | only %d samples, need %d for reliable calibration",
                len(data), _MIN_SAMPLES_FOR_CALIBRATION
            )

        sorted_data = sorted(data, key=lambda x: x[0])
        n_bins = min(5, len(sorted_data) // 6)  # 每箱至少6个样本
        if n_bins < 2:
            self.n_total_samples = len(data)
            return

        chunk = len(sorted_data) // n_bins
        self.bins = []
        for i in range(n_bins):
            start = i * chunk
            end = (i + 1) * chunk if i < n_bins - 1 else len(sorted_data)
            chunk_data = sorted_data[start:end]
            raw_vals = [x[0] for x in chunk_data]
            correct = sum(1 for _, c in chunk_data if c)
            self.bins.append(CalibrationBin(
                raw_low=raw_vals[0],
                raw_high=raw_vals[-1],
                n_samples=len(chunk_data),
                n_correct=correct,
            ))

        self.n_total_samples = len(data)
        # 保序回归：确保校准值单调（如果不满足则向邻箱均值修正）
        self._enforce_monotonicity()
        logger.info(
            "confidence_calibration | rebuilt %d bins from %d samples",
            len(self.bins), self.n_total_samples
        )

    def _enforce_monotonicity(self) -> None:
        """简单保序回归：用前向累积均值确保单调性。"""
        for i in range(1, len(self.bins)):
            if self.bins[i].actual_accuracy < self.bins[i - 1].actual_accuracy:
                # 合并两箱（Pool Adjacent Violators）
                combined_n = self.bins[i].n_samples + self.bins[i - 1].n_samples
                combined_correct = self.bins[i].n_correct + self.bins[i - 1].n_correct
                avg = combined_correct / combined_n if combined_n > 0 else _PRIOR_ACCURACY
                # 用相同值填充（简化实现）
                self.bins[i - 1].n_correct = int(avg * self.bins[i - 1].n_samples)
                self.bins[i].n_correct = int(avg * self.bins[i].n_samples)

    def _rebuild_bins_simple(self) -> None:
        """简单初始化单个全范围分箱。"""
        if not self.bins:
            self.bins = [CalibrationBin(0.0, 1.0, 0, 0)]

    def save(self, path: Path = _CALIBRATION_FILE) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "model_name": self.model_name,
            "n_total_samples": self.n_total_samples,
            "bins": [asdict(b) for b in self.bins],
        }
        path.write_text(json.dumps(data, indent=2, ensure_ascii=False))
        logger.info("confidence_calibration | saved to %s", path)

    @classmethod
    def load(cls, path: Path = _CALIBRATION_FILE, model_name: str = "global") -> "ConfidenceCalibrator":
        if path.exists():
            try:
                data = json.loads(path.read_text())
                bins = [CalibrationBin(**b) for b in data.get("bins", [])]
                return cls(
                    bins=bins,
                    n_total_samples=data.get("n_total_samples", 0),
                    model_name=data.get("model_name", model_name),
                )
            except Exception as exc:
                logger.warning("confidence_calibration | load failed: %s, using empty calibrator", exc)
        return cls(model_name=model_name)

    def calibration_report(self) -> str:
        """输出校准状态报告。"""
        if not self.is_ready():
            return (
                f"⚠️ 样本不足（{self.n_total_samples}/{_MIN_SAMPLES_FOR_CALIBRATION}），"
                f"当前使用Bayesian先验收缩（先验准确率={_PRIOR_ACCURACY}）"
            )
        lines = [f"校准状态：已就绪（{self.n_total_samples}个样本）\n分箱详情："]
        for b in self.bins:
            lines.append(
                f"  置信度[{b.raw_low:.2f}-{b.raw_high:.2f}]: "
                f"样本={b.n_samples}, 实际准确率={b.actual_accuracy:.1%}"
            )
        return "\n".join(lines)


# 全局单例（懒加载）
_global_calibrator: Optional[ConfidenceCalibrator] = None


def get_calibrator() -> ConfidenceCalibrator:
    global _global_calibrator
    if _global_calibrator is None:
        _global_calibrator = ConfidenceCalibrator.load()
    return _global_calibrator


def calibrate_confidence(raw_confidence: float, model_name: str = "global") -> float:
    """便捷函数：校准单个置信度值。"""
    calibrator = get_calibrator()
    return calibrator.calibrate(raw_confidence)


def record_prediction_outcome(raw_confidence: float, direction_correct: bool) -> None:
    """记录预测结果，增量更新校准器。"""
    calibrator = get_calibrator()
    calibrator.add_outcome(raw_confidence, direction_correct)
    # 每10次结果保存一次（减少IO）
    if calibrator.n_total_samples % 10 == 0:
        calibrator.save()
