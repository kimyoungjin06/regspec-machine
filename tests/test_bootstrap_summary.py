from regspec_machine.bootstrap import summarize_bootstrap


def test_bootstrap_summary_uses_finite_sample_pvalue() -> None:
    est, ci_lo, ci_hi, se, p_boot = summarize_bootstrap([0.1, 0.2, 0.3, 0.4])

    assert est is not None
    assert ci_lo is not None and ci_hi is not None
    assert se is not None and se >= 0.0
    assert p_boot is not None and 0.0 < p_boot <= 1.0
