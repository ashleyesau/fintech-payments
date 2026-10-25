"""
simulator/archetypes.py: Merchant archetype definitions loaded from config/archetypes.yaml.

Provides get_archetype(), sample_province(), sample_metro(), and
sample_quality_percentile(). All province and quality sampling uses numpy
so the generator can seed numpy's RNG for reproducible runs.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict

import numpy as np

from simulator.config import load_archetypes_config, load_calibration

# Day order for normalization and lookups.
_DAY_ORDER = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


@dataclass(frozen=True)
class Archetype:
    name: str
    mean_ticket_zar: float
    ticket_std_zar: float
    tx_per_day: float
    open_days: tuple        # days where weekday_weight > 0
    refund_rate: float
    fraud_rate: float
    monthly_churn_prob: float
    mcc: str
    bank_weights: Dict[str, float]
    # Weekday weights normalized so mean across open days = 1.0.
    weekday_weights: Dict[str, float]
    # 24-element hourly weights, normalized so mean across non-zero hours = 1.0.
    hourly_weights: tuple
    # 12-element monthly seasonal multipliers (Jan index 0, Dec index 11).
    monthly_seasonal_multipliers: tuple


def _normalize_weights(weights: list[float]) -> list[float]:
    """Normalize a list so the mean of non-zero values equals 1.0."""
    non_zero = [w for w in weights if w > 0]
    if not non_zero:
        return weights
    mean = sum(non_zero) / len(non_zero)
    return [w / mean if w > 0 else 0.0 for w in weights]


def _build_archetype(name: str, cfg: dict) -> Archetype:
    raw_weekday = cfg["weekday_weights"]
    weekday_list = [raw_weekday.get(d, 0.0) for d in _DAY_ORDER]
    normalized_weekday = _normalize_weights(weekday_list)
    weekday_weights = dict(zip(_DAY_ORDER, normalized_weekday))
    open_days = tuple(d for d, w in weekday_weights.items() if w > 0)

    hourly_raw = cfg["hourly_weights"]
    hourly_weights = tuple(_normalize_weights(hourly_raw))

    return Archetype(
        name=name,
        mean_ticket_zar=cfg["mean_ticket_zar"],
        ticket_std_zar=cfg["ticket_std_zar"],
        tx_per_day=cfg["tx_per_day"],
        open_days=open_days,
        refund_rate=cfg["refund_rate"],
        fraud_rate=cfg["fraud_rate"],
        monthly_churn_prob=cfg["monthly_churn_prob"],
        mcc=cfg["mcc"],
        bank_weights=dict(cfg["bank_weights"]),
        weekday_weights=weekday_weights,
        hourly_weights=hourly_weights,
        monthly_seasonal_multipliers=tuple(cfg["monthly_seasonal_multipliers"]),
    )


def _load_archetypes() -> Dict[str, Archetype]:
    cfg = load_archetypes_config()
    return {name: _build_archetype(name, params) for name, params in cfg.items()}


def _load_province_matrix() -> tuple[dict, dict]:
    """Return (province_distributions, metro_probabilities)."""
    from simulator.config import _load_yaml
    data = _load_yaml("archetype_province_matrix.yaml")
    return data["province_distributions"], data["metro_probabilities"]


# Loaded once at startup.
ARCHETYPES: Dict[str, Archetype] = _load_archetypes()
_PROVINCE_DISTRIBUTIONS, _PROVINCE_METRO_PROBS = _load_province_matrix()


def get_archetype(name: str) -> Archetype:
    if name not in ARCHETYPES:
        raise ValueError(f"Unknown archetype: {name!r}. Available: {sorted(ARCHETYPES)}")
    return ARCHETYPES[name]


def sample_province(archetype_name: str) -> str:
    """Sample a province for a new merchant based on archetype distribution."""
    dist = _PROVINCE_DISTRIBUTIONS.get(archetype_name)
    if dist is None:
        raise ValueError(f"No province distribution for archetype: {archetype_name!r}")
    provinces = list(dist.keys())
    weights = np.array(list(dist.values()), dtype=float)
    weights /= weights.sum()
    return str(np.random.choice(provinces, p=weights))


def sample_metro(province: str) -> bool:
    """Sample is_metro for a merchant given their province."""
    prob = _PROVINCE_METRO_PROBS.get(province, 0.20)
    return bool(np.random.random() < prob)


def sample_quality_percentile() -> float:
    """
    Draw quality_percentile from Beta(1.5, 4.0).
    Called once at merchant creation; value is fixed for the merchant's lifetime.
    ~60% of merchants fall below the archetype mean; top decile runs at ~1.8-2.0x.
    """
    cfg = load_calibration().get("quality_tier", {})
    a = cfg.get("beta_a", 1.5)
    b = cfg.get("beta_b", 4.0)
    return float(np.random.beta(a, b))
