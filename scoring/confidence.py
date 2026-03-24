"""
Wilson score confidence intervals for proportion estimates.

Used throughout the scoring engine wherever a rate/proportion is computed
over a finite sample of records (coverage rate, OA rate, match rate, etc.).

The Wilson interval is preferred over the normal approximation because:
- It stays within [0, 1] by construction
- It is asymmetric near the boundaries (correct behaviour for small N)
- It performs well even for N < 30

Reference: Wilson, E.B. (1927). Probable inference, the law of succession,
and statistical inference. JASA 22(158):209–212.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from statsmodels.stats.proportion import proportion_confint


class ConfidenceTier(str, Enum):
    RELIABLE = "reliable"          # N >= 200
    MODERATE = "moderate"          # 50 <= N < 200
    LOW = "low"                    # 10 <= N < 50
    INSUFFICIENT = "insufficient"  # N < 10 — suppress estimate


@dataclass
class ProportionEstimate:
    """
    A proportion estimate with Wilson confidence interval and reliability tier.

    Fields:
        k           number of successes
        n           total observations (denominator)
        estimate    point estimate (k/n), None if insufficient
        ci_low      Wilson lower bound (95%), None if insufficient
        ci_high     Wilson upper bound (95%), None if insufficient
        tier        reliability tier
        suppressed  True if N < 10 (estimate should not be reported)
    """
    k: int
    n: int
    estimate: float | None
    ci_low: float | None
    ci_high: float | None
    tier: ConfidenceTier
    suppressed: bool

    def as_dict(self) -> dict:
        return {
            "k": self.k,
            "n": self.n,
            "estimate": self.estimate,
            "ci_low": self.ci_low,
            "ci_high": self.ci_high,
            "ci_width": (
                round(self.ci_high - self.ci_low, 4)
                if self.ci_low is not None and self.ci_high is not None
                else None
            ),
            "confidence_tier": self.tier.value,
            "suppressed": self.suppressed,
        }


# Tier thresholds — kept in sync with scoring_weights.yaml
_TIER_THRESHOLDS = {
    ConfidenceTier.RELIABLE: 200,
    ConfidenceTier.MODERATE: 50,
    ConfidenceTier.LOW: 10,
    ConfidenceTier.INSUFFICIENT: 0,
}


def wilson_estimate(k: int, n: int, alpha: float = 0.05) -> ProportionEstimate:
    """
    Compute a Wilson score confidence interval for k successes in n trials.

    Args:
        k:      number of successes (e.g. records with OA status)
        n:      total observations (e.g. total records)
        alpha:  significance level (default 0.05 → 95% CI)

    Returns:
        ProportionEstimate with tier and suppression flag.
    """
    if n < 0 or k < 0:
        raise ValueError(f"k and n must be non-negative, got k={k}, n={n}")
    if k > n:
        raise ValueError(f"k ({k}) cannot exceed n ({n})")

    tier = _classify_tier(n)

    if tier == ConfidenceTier.INSUFFICIENT:
        return ProportionEstimate(
            k=k, n=n,
            estimate=None, ci_low=None, ci_high=None,
            tier=tier, suppressed=True,
        )

    point = k / n if n > 0 else 0.0
    low, high = proportion_confint(count=k, nobs=n, alpha=alpha, method="wilson")

    return ProportionEstimate(
        k=k, n=n,
        estimate=round(point, 4),
        ci_low=round(low, 4),
        ci_high=round(high, 4),
        tier=tier,
        suppressed=False,
    )


def pooled_wilson_estimate(
    ks: list[int], ns: list[int], alpha: float = 0.05
) -> ProportionEstimate:
    """
    Pool k and n values across multiple institutions of the same type.
    Enables reliable aggregate estimates even when individual estimates
    fall below the confidence threshold.

    Example: 5 IFs each with N=15 records → pooled N=75 (moderate tier).
    """
    total_k = sum(ks)
    total_n = sum(ns)
    return wilson_estimate(total_k, total_n, alpha)


def _classify_tier(n: int) -> ConfidenceTier:
    if n >= _TIER_THRESHOLDS[ConfidenceTier.RELIABLE]:
        return ConfidenceTier.RELIABLE
    if n >= _TIER_THRESHOLDS[ConfidenceTier.MODERATE]:
        return ConfidenceTier.MODERATE
    if n >= _TIER_THRESHOLDS[ConfidenceTier.LOW]:
        return ConfidenceTier.LOW
    return ConfidenceTier.INSUFFICIENT
