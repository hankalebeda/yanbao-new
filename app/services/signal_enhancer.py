"""信号质量提升模块（P18：不依赖SFT的近期增强方案）

四种可立即落地的方法，按效果/成本比排序：

方法1: Prompt工程优化（效果上限+2-3%准确率，成本≈0）
  - 结构化CoT模板：强制先给反例再给结论
  - 锚点约束：要求模型给出"如果我错了，最可能的原因是..."

方法2: 自我批评（Self-Critique）（效果上限+3-5%，成本×2）
  - Round1: 生成初始判断
  - Round2: 要求模型批评自己的判断，找出潜在错误
  - Round3: 综合两轮给出最终结论

方法3: 多模型投票（Ensemble Voting）（效果上限+4-6%，成本×N）
  - 加权多数投票：DeepSeek + Gemini + Ollama
  - 权重基于各模型历史准确率（而非固定权重）
  - 一致性高时置信度高，分歧大时触发自我批评

方法4: Few-shot示例（效果上限+2-4%，取决于示例质量）
  - 每个场景维护3-5个高质量示例（事后验证正确的）
  - 负面示例（错误案例）比正面示例更有效
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 方法1：优化版Prompt模板
# ---------------------------------------------------------------------------

FEW_SHOT_EXAMPLES = """
【示例1 - 正确案例】
数据：MACD死叉+主力5日净流出2.3亿+利空公告
输出：{"recommendation":"卖出","reason":"技术面MACD死叉确认下行趋势，主力资金持续出逃，叠加公司发布盈利预警公告，三重利空共振。","trigger":"收盘价跌破20日均线确认","invalidation":"若主力资金单日净流入超1亿则止损观望","risks":"公告影响短暂反弹可能造成止损","confidence":0.72}

【示例2 - 反面案例，常见错误】
数据：仅凭MACD金叉，忽略资金大幅出逃
错误输出：{"recommendation":"买入","confidence":0.85}  ← 错误！高置信度但忽略反向资金信号
正确做法：置信度不超过0.6，需说明矛盾信号的处置方式

【示例3 - 不确定时】
当技术面与资金面信号相反时，不应给出高置信度方向性判断：
{"recommendation":"观望","reason":"技术面金叉信号与主力资金净流出矛盾，当前信号相互抵消","confidence":0.45}
""".strip()


def build_enhanced_prompt(
    stock_code: str,
    context: str,
    news_titles: list[str],
    policy_titles: list[str],
    social_titles: list[str],
    use_cot: bool = True,
    use_few_shot: bool = True,
) -> str:
    """构建增强版Prompt，融合CoT约束、少样本示例和自我矛盾检查。"""
    few_shot_block = f"\n{FEW_SHOT_EXAMPLES}\n\n" if use_few_shot else ""

    cot_instruction = """
分析步骤（必须按顺序）：
1. 数据解读：列出关键技术/资金/消息信号（不超过5条）
2. 多空论据：分别列出最强的看多理由和看空理由各1条
3. 矛盾识别：是否存在技术面与资金面相反的信号？如有，如何处理？
4. 结论及置信度：给出建议，置信度须与矛盾程度负相关
5. 失效条件：明确说明建议在何种情况下立即失效
""".strip() if use_cot else ""

    return (
        "你是专业A股研报分析师，需要基于多维度数据给出操作建议。\n"
        "【核心约束】：禁止忽略反向信号；置信度须反映真实不确定性（技术面与资金面矛盾时≤0.6）；"
        "如果证据不足以支持高置信判断，宁可给出'观望'。\n\n"
        + (f"【分析框架】\n{cot_instruction}\n\n" if cot_instruction else "")
        + (f"【参考示例】\n{few_shot_block}" if few_shot_block else "")
        + "【输出格式】严格JSON，不得有额外文字：\n"
        '{"recommendation":"买入|卖出|观望",'
        '"data_signals":["信号1","信号2","信号3"],'
        '"bull_case":"最强看多理由",'
        '"bear_case":"最强看空理由",'
        '"contradiction":"矛盾信号说明（无则填无）",'
        '"reason":"综合结论（2-3句）",'
        '"trigger":"操作触发条件",'
        '"invalidation":"失效条件",'
        '"risks":"主要风险（2条）",'
        '"confidence":0.0-1.0}\n\n'
        f"股票代码：{stock_code}\n"
        f"{context}\n\n"
        f"【近期新闻】{'；'.join(news_titles) or '无'}\n"
        f"【政策动向】{'；'.join(policy_titles) or '无'}\n"
        f"【市场热议】{'；'.join(social_titles) or '无'}"
    )


# ---------------------------------------------------------------------------
# 方法2：自我批评（Self-Critique Loop）
# ---------------------------------------------------------------------------

CRITIQUE_PROMPT_TEMPLATE = """
你之前对股票{stock_code}给出了以下判断：
{initial_output}

请以批评者身份审视这个判断，找出：
1. 忽略的反向信号
2. 过于乐观/悲观的地方
3. 缺乏支撑的论断

然后给出修正后的判断（如果判断正确则保持，如果有明显缺陷则修正）。
严格按原JSON格式输出，无需解释修改原因。
"""


async def self_critique_refine(
    stock_code: str,
    initial_result: str,
    call_model_fn,
    max_rounds: int = 1,
) -> str:
    """对初始判断进行自我批评并修正。

    Args:
        stock_code: 股票代码
        initial_result: 第一轮LLM输出的JSON字符串
        call_model_fn: async function(prompt) -> str
        max_rounds: 批评轮数（生产建议1轮）

    Returns:
        修正后的JSON字符串
    """
    current = initial_result
    for round_idx in range(max_rounds):
        critique_prompt = CRITIQUE_PROMPT_TEMPLATE.format(
            stock_code=stock_code,
            initial_output=current,
        )
        try:
            refined = await call_model_fn(critique_prompt)
            # 验证输出仍然是有效JSON
            _extract_json(refined)
            current = refined
            logger.info("self_critique | round=%d refined successfully", round_idx + 1)
        except Exception as exc:
            logger.warning("self_critique | round=%d failed: %s, keeping previous", round_idx + 1, exc)
            break
    return current


# ---------------------------------------------------------------------------
# 方法3：多模型加权投票
# ---------------------------------------------------------------------------

@dataclass
class ModelVote:
    model_name: str
    recommendation: str   # BUY | SELL | HOLD
    confidence: float
    raw_response: str
    weight: float = 1.0   # 历史准确率权重


@dataclass
class EnsembleResult:
    final_recommendation: str
    ensemble_confidence: float
    agreement_rate: float   # 模型间一致性（1.0=全部一致）
    votes: list[ModelVote]
    triggered_critique: bool   # 是否因分歧触发了自我批评
    raw_responses: dict[str, str]


# 模型历史权重（基于回测，需要定期更新）
# 初始值：DeepSeek略高（中文理解更强），Gemini次之，Ollama保守权重
_DEFAULT_MODEL_WEIGHTS = {
    "deepseek_api": 1.2,
    "gemini_api": 1.0,
    "ollama": 0.8,
}


def _parse_recommendation(response: str) -> tuple[str, float]:
    """从LLM输出中提取 recommendation 和 confidence。"""
    obj = _extract_json(response)
    if obj:
        reco = obj.get("recommendation", "")
        confidence = float(obj.get("confidence", 0.5))
        if "卖" in reco:
            return "SELL", confidence
        if "买" in reco:
            return "BUY", confidence
        return "HOLD", confidence
    # fallback 关键词
    if "卖" in response:
        return "SELL", 0.5
    if "买" in response:
        return "BUY", 0.5
    return "HOLD", 0.4


def _extract_json(text: str) -> dict | None:
    """从文本中提取第一个完整JSON对象。"""
    try:
        start = text.find("{")
        end = text.rfind("}") + 1
        if start >= 0 and end > start:
            return json.loads(text[start:end])
    except (ValueError, json.JSONDecodeError):
        pass
    return None


async def ensemble_vote(
    prompt: str,
    model_names: list[str],
    model_weights: dict[str, float] | None = None,
    disagreement_threshold: float = 0.5,
) -> EnsembleResult:
    """多模型加权投票。

    Args:
        prompt: 分析prompt
        model_names: 参与投票的模型列表
        model_weights: 各模型权重（None则用默认权重）
        disagreement_threshold: 分歧高于此阈值触发告警

    Returns:
        EnsembleResult 含最终结论和一致性指标
    """
    from app.services.llm_router import _call_model

    weights = model_weights or _DEFAULT_MODEL_WEIGHTS

    async def _get_vote(model_name: str) -> ModelVote | None:
        try:
            raw = await _call_model(model_name, prompt, temperature=0.3, use_cot=False)
            response = raw.get("response", "")
            reco, conf = _parse_recommendation(response)
            return ModelVote(
                model_name=model_name,
                recommendation=reco,
                confidence=conf,
                raw_response=response,
                weight=weights.get(model_name, 1.0),
            )
        except Exception as exc:
            logger.warning("ensemble | model=%s failed: %s", model_name, exc)
            return None

    votes_raw = await asyncio.gather(*[_get_vote(m) for m in model_names])
    votes = [v for v in votes_raw if v is not None]

    if not votes:
        raise RuntimeError("All models failed in ensemble vote")

    # 加权计票
    score: dict[str, float] = {"BUY": 0.0, "SELL": 0.0, "HOLD": 0.0}
    for v in votes:
        score[v.recommendation] += v.weight * v.confidence

    final_reco = max(score, key=lambda r: score[r])
    total_weight = sum(v.weight for v in votes)

    # 一致性：投票最多方向的票重占比
    agreement = score[final_reco] / total_weight if total_weight > 0 else 0.0

    # 集成置信度 = 方向加权置信度 × 一致性系数
    direction_confidences = [v.confidence for v in votes if v.recommendation == final_reco]
    avg_conf = sum(direction_confidences) / len(direction_confidences) if direction_confidences else 0.5
    ensemble_conf = round(avg_conf * agreement, 4)

    triggered = agreement < (1 - disagreement_threshold)

    logger.info(
        "ensemble | final=%s conf=%.3f agreement=%.2f votes=%s",
        final_reco, ensemble_conf, agreement,
        {v.model_name: v.recommendation for v in votes}
    )

    return EnsembleResult(
        final_recommendation=final_reco,
        ensemble_confidence=ensemble_conf,
        agreement_rate=round(agreement, 4),
        votes=votes,
        triggered_critique=triggered,
        raw_responses={v.model_name: v.raw_response for v in votes},
    )


# ---------------------------------------------------------------------------
# 综合增强管道
# ---------------------------------------------------------------------------

async def enhanced_signal_pipeline(
    stock_code: str,
    prompt: str,
    enable_ensemble: bool = True,
    enable_self_critique: bool = False,
    ensemble_models: list[str] | None = None,
) -> dict[str, Any]:
    """综合信号增强管道。

    优先级：
    - enable_ensemble=True: 多模型投票（推荐，成本×2-3，准确率+4-6%）
    - enable_self_critique=True: 在集成基础上再做自我批评（成本再×2）
    - 均False: 单模型+增强Prompt（成本×1，准确率+2-3%）

    Returns:
        dict with keys: recommendation, confidence, model_used, agreement_rate,
                        raw_responses, enhanced
    """
    models = ensemble_models or ["deepseek_api", "gemini_api"]

    if enable_ensemble and len(models) >= 2:
        try:
            result = await ensemble_vote(prompt, models)

            final_response = result.raw_responses.get(
                next((v.model_name for v in result.votes if v.recommendation == result.final_recommendation), models[0]),
                ""
            )

            # 分歧大时触发自我批评
            if enable_self_critique and result.triggered_critique:
                from app.services.llm_router import _call_model
                async def _call_primary(p: str) -> str:
                    raw = await _call_model(models[0], p, 0.3, False)
                    return raw.get("response", "")

                logger.info("enhanced_pipeline | high disagreement, triggering self-critique")
                final_response = await self_critique_refine(stock_code, final_response, _call_primary)

            reco, conf = _parse_recommendation(final_response)
            return {
                "recommendation": reco,
                "confidence": result.ensemble_confidence,
                "model_used": f"ensemble({','.join(models)})",
                "agreement_rate": result.agreement_rate,
                "raw_responses": result.raw_responses,
                "primary_response": final_response,
                "enhanced": True,
                "method": "ensemble_vote",
            }
        except Exception as exc:
            logger.warning("enhanced_pipeline | ensemble failed: %s, falling back to single", exc)

    # 降级到单模型
    from app.services.llm_router import route_and_call
    result = await route_and_call(prompt, temperature=0.3, use_cot=True)
    reco, conf = _parse_recommendation(result.response)
    return {
        "recommendation": reco,
        "confidence": conf,
        "model_used": result.model_used,
        "agreement_rate": 1.0,
        "raw_responses": {result.model_used: result.response},
        "primary_response": result.response,
        "enhanced": False,
        "method": "single_model_cot",
    }
