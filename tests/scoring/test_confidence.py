"""
Tests for Wilson confidence interval module.
"""

import pytest
from scoring.confidence import (
    wilson_estimate,
    pooled_wilson_estimate,
    ConfidenceTier,
)


def test_reliable_tier():
    est = wilson_estimate(k=180, n=200)
    assert est.tier == ConfidenceTier.RELIABLE
    assert not est.suppressed
    assert est.estimate == pytest.approx(0.9, abs=0.01)


def test_moderate_tier():
    est = wilson_estimate(k=40, n=80)
    assert est.tier == ConfidenceTier.MODERATE
    assert not est.suppressed


def test_low_tier():
    est = wilson_estimate(k=5, n=20)
    assert est.tier == ConfidenceTier.LOW
    assert not est.suppressed


def test_insufficient_tier():
    est = wilson_estimate(k=3, n=5)
    assert est.tier == ConfidenceTier.INSUFFICIENT
    assert est.suppressed
    assert est.estimate is None
    assert est.ci_low is None
    assert est.ci_high is None


def test_ci_bounds_within_range():
    est = wilson_estimate(k=50, n=100)
    assert 0.0 <= est.ci_low <= est.estimate <= est.ci_high <= 1.0


def test_zero_successes():
    est = wilson_estimate(k=0, n=200)
    assert est.estimate == 0.0
    assert est.ci_low == 0.0
    assert est.ci_high > 0.0   # Wilson gives non-zero upper bound


def test_all_successes():
    est = wilson_estimate(k=200, n=200)
    assert est.estimate == 1.0
    assert est.ci_high == 1.0
    assert est.ci_low < 1.0   # Wilson gives non-trivial lower bound


def test_k_exceeds_n_raises():
    with pytest.raises(ValueError):
        wilson_estimate(k=10, n=5)


def test_negative_raises():
    with pytest.raises(ValueError):
        wilson_estimate(k=-1, n=10)


def test_pooled_estimate():
    """Pooling small groups should raise confidence tier."""
    # 5 institutions, each N=15 (LOW tier individually)
    ks = [8, 7, 9, 6, 8]
    ns = [15, 15, 15, 15, 15]
    pooled = pooled_wilson_estimate(ks, ns)
    # Pooled N=75 → MODERATE tier
    assert pooled.tier == ConfidenceTier.MODERATE
    assert pooled.n == 75


def test_as_dict_keys():
    est = wilson_estimate(k=100, n=200)
    d = est.as_dict()
    expected_keys = ["k", "n", "estimate", "ci_low", "ci_high",
                     "ci_width", "confidence_tier", "suppressed"]
    for key in expected_keys:
        assert key in d


def test_suppressed_as_dict():
    est = wilson_estimate(k=2, n=5)
    d = est.as_dict()
    assert d["suppressed"] is True
    assert d["estimate"] is None
    assert d["ci_width"] is None
