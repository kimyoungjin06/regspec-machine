from __future__ import annotations

from typing import Dict, Iterable, List, Optional, Sequence, Tuple


def _to_float(value: object) -> Optional[float]:
    try:
        text = str(value).strip()
        if not text:
            return None
        return float(text)
    except Exception:
        return None


def bh_fdr(indexed_pvalues: Sequence[Tuple[int, float]]) -> Dict[int, float]:
    if not indexed_pvalues:
        return {}
    ranked = sorted(indexed_pvalues, key=lambda x: x[1])
    m = len(ranked)
    out: Dict[int, float] = {}
    running = 1.0
    for i in range(m - 1, -1, -1):
        idx, pval = ranked[i]
        rank = i + 1
        qval = min(running, (pval * m) / float(rank))
        running = qval
        out[idx] = min(1.0, max(0.0, qval))
    return out


def attach_bh_qvalues(
    rows: List[Dict[str, object]],
    *,
    p_col: str,
    family_col: str,
) -> List[Dict[str, object]]:
    family_indexed_p: Dict[str, List[Tuple[int, float]]] = {}
    for i, row in enumerate(rows):
        family = str(row.get(family_col, "")).strip()
        pval = _to_float(row.get(p_col))
        if not family or pval is None:
            continue
        family_indexed_p.setdefault(family, []).append((i, pval))

    for pairs in family_indexed_p.values():
        qmap = bh_fdr(pairs)
        for idx, q in qmap.items():
            rows[idx]["q_value"] = q
    return rows

