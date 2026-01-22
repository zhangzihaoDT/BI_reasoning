from typing import TypedDict, Literal, List, Dict, Any, Optional


AnomalyFlag = Literal["结构性异常", "比例假异常", "高波动异常", "正常波动"]


class AnomalyDecision(TypedDict):
    flag: AnomalyFlag
    z: float
    cv: float
    anomaly_detected: bool


class BiReasoningPlanStep(TypedDict):
    id: str
    tool: str
    parameters: Dict[str, Any]
    reasoning: str


def classify_anomaly_from_stats(
    value: float,
    mean: float,
    std: float,
    cv_threshold: float = 0.1,
) -> AnomalyDecision:
    if std <= 0 or mean == 0:
        return {
            "flag": "正常波动",
            "z": 0.0,
            "cv": 0.0,
            "anomaly_detected": False,
        }

    z = (value - mean) / std
    cv = abs(std / mean)
    abs_z = abs(z)

    if abs_z >= 2 and cv < cv_threshold:
        flag: AnomalyFlag = "结构性异常"
        anomaly = True
    elif abs_z >= 2:
        flag = "高波动异常"
        anomaly = True
    else:
        flag = "正常波动"
        anomaly = False

    return {
        "flag": flag,
        "z": z,
        "cv": cv,
        "anomaly_detected": anomaly,
    }


def classify_ratio_decomposition(
    delta_group: float,
    delta_total: float,
    delta_ratio: float,
    ratio_threshold: float = 0.2,
    scale_threshold: float = 0.2,
) -> Optional[AnomalyDecision]:
    """
    用于判断：比例异常是否为结构异常，还是分母效应
    输入为 log-diff 或 pct change
    """

    if abs(delta_ratio) < ratio_threshold:
        return None  # 比例本身不显著，不触发

    # 分组本身变化显著，且不是整体一起变
    if abs(delta_group) >= scale_threshold and abs(delta_group - delta_total) >= scale_threshold:
        return {
            "flag": "结构性异常",
            "z": delta_ratio,
            "cv": abs(delta_group),
            "anomaly_detected": True,
        }

    # 比例异常主要由整体变化驱动
    if abs(delta_total) >= scale_threshold and abs(delta_group) < scale_threshold:
        return {
            "flag": "比例假异常",
            "z": delta_ratio,
            "cv": abs(delta_total),
            "anomaly_detected": False,
        }

    return None


def build_additive_ratio_drilldown_plan(
    decision: AnomalyDecision,
    metric: str,
    date_range: str,
    dimensions: List[str],
    core_metrics: List[str],
) -> List[BiReasoningPlanStep]:
    if decision["flag"] == "比例假异常":
        return [
            {
                "id": "total_volume_check",
                "tool": "trend",
                "parameters": {
                    "metric": "total_volume",
                    "date_range": date_range,
                },
                "reasoning": "比例异常疑似由整体规模变化导致，优先确认分母变化。",
            }
        ]

    if not decision["anomaly_detected"]:
        return []

    primary_dimension: Optional[str] = dimensions[0] if dimensions else None

    steps: List[BiReasoningPlanStep] = []

    steps.append(
        {
            "id": "additive_decomposition",
            "tool": "additive",
            "parameters": {
                "metric": metric,
                "dimensions": dimensions,
                "date_range": date_range,
            },
            "reasoning": "锁定责任来源，按门店、品牌、城市或渠道拆分指标。",
        }
    )

    steps.append(
        {
            "id": "ratio_analysis",
            "tool": "ratio",
            "parameters": {
                "metrics": core_metrics,
                "date_range": date_range,
            },
            "reasoning": "从机制层解释异常，通过核心比率指标进行分析。",
        }
    )

    steps.append(
        {
            "id": "drilldown_locate",
            "tool": "rollup",
            "parameters": {
                "metric": metric,
                "dimension": primary_dimension,
                "date_range": date_range,
            },
            "reasoning": "定位异常具体发生在哪个维度切片。",
        }
    )

    return steps


def evaluate_breadth_scan_and_plan(
    results: Dict[str, Any],
    metric: str,
    date_range: str,
    dimensions: List[str],
    core_metrics: List[str],
    cv_threshold: float = 0.1,
) -> Dict[str, Any]:
    anomaly_node = results.get("anomaly_check") or results.get("short_term_trend")

    if not anomaly_node:
        decision: AnomalyDecision = {
            "flag": "正常波动",
            "z": 0.0,
            "cv": 0.0,
            "anomaly_detected": False,
        }
        return {
            "decision": decision,
            "next_steps": [],
        }

    value = float(anomaly_node.get("value", 0.0))
    mean = float(anomaly_node.get("mean", 0.0))
    std = float(anomaly_node.get("std", 0.0))

    decision = classify_anomaly_from_stats(
        value=value,
        mean=mean,
        std=std,
        cv_threshold=cv_threshold,
    )

    if anomaly_node.get("metric_type") == "ratio":
        ratio_decision = classify_ratio_decomposition(
            delta_group=float(anomaly_node.get("delta_group", 0.0)),
            delta_total=float(anomaly_node.get("delta_total", 0.0)),
            delta_ratio=float(anomaly_node.get("delta_ratio", 0.0)),
        )
        if ratio_decision:
            decision = ratio_decision

    next_steps = build_additive_ratio_drilldown_plan(
        decision=decision,
        metric=metric,
        date_range=date_range,
        dimensions=dimensions,
        core_metrics=core_metrics,
    )

    return {
        "decision": decision,
        "next_steps": next_steps,
    }
