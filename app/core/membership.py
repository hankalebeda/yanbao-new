"""会员权益枚举 (17_用户系统设计 §2.5)"""

PERMISSION_KEYS = [
    "instruction_card_full",
    "forecast_14d",
    "forecast_30d",
    "forecast_60d",
    "sim_history_detail",
    "advanced_reasoning",
    "sim_dashboard_by_type",
    "daily_push",
]

MEMBERSHIP_PERMISSIONS = {
    "free": [],
    "monthly": [
        "instruction_card_full",
        "forecast_14d",
        "forecast_30d",
        "forecast_60d",
        "sim_history_detail",
        "advanced_reasoning",
    ],
    "annual": PERMISSION_KEYS,
}


def get_permissions(membership_level: str) -> list[str]:
    return MEMBERSHIP_PERMISSIONS.get(membership_level, [])


def has_permission(membership_level: str, permission: str) -> bool:
    return permission in get_permissions(membership_level)


def can_see_instruction_card_full(membership_level: str) -> bool:
    return has_permission(membership_level, "instruction_card_full")


def can_see_forecast_14_30_60(membership_level: str) -> bool:
    return has_permission(membership_level, "forecast_14d")


def can_see_advanced_reasoning(membership_level: str) -> bool:
    return has_permission(membership_level, "advanced_reasoning")


def can_see_sim_dashboard_by_type(membership_level: str) -> bool:
    return has_permission(membership_level, "sim_dashboard_by_type")


def can_see_sim_history_detail(membership_level: str) -> bool:
    return has_permission(membership_level, "sim_history_detail")


def has_paid_membership(tier: str | None) -> bool:
    """Return True if the tier is a paid membership level."""
    if not tier:
        return False
    return tier.lower() not in ("free", "", "none")
