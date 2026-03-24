"""
Dataset exporter.

Converts scoring results to CSV, JSON, and SQLite outputs.
All formats are open — no proprietary formats in the core dataset.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


class DatasetExporter:
    """
    Exports scored results to the data/processed directory.

    Usage:
        exporter = DatasetExporter(output_dir="data/processed")
        exporter.export_coverage(coverage_results, run_id="2025-01-15")
        exporter.export_oa(oa_results, run_id="2025-01-15")
    """

    def __init__(self, output_dir: str | Path = "data/processed"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Coverage
    # ------------------------------------------------------------------

    def export_coverage(self, results: list, run_id: str) -> Path:
        """Export coverage results to CSV."""
        rows = [r.as_dict() for r in results]
        df = pd.json_normalize(rows)
        path = self.output_dir / f"coverage_{run_id}.csv"
        df.to_csv(path, index=False)
        logger.info(f"Coverage exported: {path} ({len(df)} rows)")
        return path

    # ------------------------------------------------------------------
    # Open Access
    # ------------------------------------------------------------------

    def export_oa(self, results: list, run_id: str) -> Path:
        """Export OA results to CSV."""
        rows = [r.as_dict() for r in results]
        df = pd.json_normalize(rows)
        path = self.output_dir / f"oa_{run_id}.csv"
        df.to_csv(path, index=False)
        logger.info(f"OA exported: {path} ({len(df)} rows)")
        return path

    # ------------------------------------------------------------------
    # Records (full publication records — Parquet for size)
    # ------------------------------------------------------------------

    def export_records(self, records: list[dict], source: str, run_id: str) -> Path:
        """Export normalised publication records to Parquet."""
        df = pd.DataFrame(records)
        # Flatten nested fields to JSON strings for storage
        for col in ["authors", "institutions", "fields", "funding", "patent_citations"]:
            if col in df.columns:
                df[col] = df[col].apply(
                    lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (list, dict)) else x
                )
        path = self.output_dir / f"records_{source}_{run_id}.parquet"
        df.to_parquet(path, index=False)
        logger.info(f"Records exported: {path} ({len(df)} rows)")
        return path

    # ------------------------------------------------------------------
    # SQLite — queryable database
    # ------------------------------------------------------------------

    def export_to_sqlite(
        self,
        coverage_results: list,
        oa_results: list,
        run_id: str,
    ) -> Path:
        """
        Write all results to a SQLite database for ad-hoc querying.
        Creates or appends to inep_bibliometric.db.
        """
        db_path = self.output_dir / "inep_bibliometric.db"

        coverage_rows = [r.as_dict() for r in coverage_results]
        oa_rows = [r.as_dict() for r in oa_results]

        coverage_df = pd.json_normalize(coverage_rows)
        oa_df = pd.json_normalize(oa_rows)

        # Add run metadata
        for df in [coverage_df, oa_df]:
            df["run_id"] = run_id
            df["exported_at"] = datetime.now(timezone.utc).isoformat()

        with sqlite3.connect(db_path) as conn:
            self._sqlite_safe(coverage_df).to_sql("coverage", conn, if_exists="append", index=False)
            self._sqlite_safe(oa_df).to_sql("open_access", conn, if_exists="append", index=False)

        logger.info(f"SQLite updated: {db_path}")
        return db_path

    # ------------------------------------------------------------------
    # Sprint 1 summary report — Markdown
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # Convergence
    # ------------------------------------------------------------------

    def export_convergence(self, convergence: dict, run_id: str) -> dict[str, Path]:
        """Export overlap matrix, divergences and review queue."""
        paths: dict[str, Path] = {}

        # Overlap matrix
        overlap = convergence.get("overlap_matrix", [])
        if overlap:
            df = pd.DataFrame(overlap)
            p = self.output_dir / f"overlap_{run_id}.csv"
            df.to_csv(p, index=False)
            paths["overlap"] = p
            logger.info(f"Overlap matrix exported: {p}")

        # Divergences
        divs = convergence.get("divergences", [])
        if divs:
            rows = [vars(d) for d in divs]
            df = pd.DataFrame(rows)
            p = self.output_dir / f"divergences_{run_id}.csv"
            df.to_csv(p, index=False)
            paths["divergences"] = p
            logger.info(f"Divergences exported: {p} ({len(divs)} flags)")

        # Review queue
        queue = convergence.get("review_queue", [])
        if queue:
            rows = [vars(m) for m in queue]
            df = pd.DataFrame(rows)
            p = self.output_dir / f"review_queue_{run_id}.csv"
            df.to_csv(p, index=False)
            paths["review_queue"] = p
            logger.info(f"Review queue exported: {p} ({len(queue)} items)")

        # Full match table (potentially large — parquet)
        matches = convergence.get("match_table", [])
        if matches:
            rows = [vars(m) for m in matches]
            df = pd.DataFrame(rows)
            p = self.output_dir / f"matches_{run_id}.parquet"
            df.to_parquet(p, index=False)
            paths["matches"] = p
            logger.info(f"Match table exported: {p} ({len(matches)} pairs)")

        return paths

    # ------------------------------------------------------------------
    # Phase 2 report
    # ------------------------------------------------------------------

    def export_phase2_report(
        self,
        coverage_results: list,
        oa_results: list,
        convergence: dict,
        run_id: str,
        config: dict,
    ) -> Path:
        """Generate Phase 2 Markdown report — multi-source comparison."""
        sources = config.get("sources", [])
        max_r = config.get("max_records", 500)

        # Group by institution
        cov_by_inst: dict[str, dict] = {}
        for r in coverage_results:
            cov_by_inst.setdefault(r.e_mec_code, {})[r.source] = r

        oa_by_inst: dict[str, dict] = {}
        for r in oa_results:
            oa_by_inst.setdefault(r.e_mec_code, {})[r.source] = r

        lines = [
            "# INEP Bibliometric Tool — Phase 2 Report",
            "",
            f"**Run:** `{run_id}`  ",
            f"**Sources:** {', '.join(sources)}  ",
            f"**Temporal window:** {config.get('start_year')}–{config.get('end_year')}  ",
            f"**Generated:** {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
            "",
            f"> Record counts capped at {max_r:,} per institution per source. "
            f"⚠ = ceiling hit. Actual corpus size shown where available.",
            "",
            "---",
            "",
            "## Coverage by Institution × Source",
            "",
        ]

        # Coverage table — one row per institution, one col per source
        header = "| Institution | Region |" + "".join(f" {s} (N) |" for s in sources)
        sep = "|---|---|" + "---|" * len(sources)
        lines += [header, sep]

        for e_mec, src_map in sorted(cov_by_inst.items()):
            inst_name = next(iter(src_map.values())).institution_name
            region = next(iter(src_map.values())).institution_region or "?"
            row = f"| {inst_name} | {region} |"
            for s in sources:
                r = src_map.get(s)
                if r:
                    ceiling = "⚠" if getattr(r, "ceiling_hit", False) else ""
                    row += f" {r.n_records}{ceiling} |"
                else:
                    row += " — |"
            lines.append(row)

        lines += ["", "---", "", "## OA Rate by Institution × Source", ""]

        header = "| Institution |" + "".join(f" {s} OA% |" for s in sources)
        sep = "|---|" + "---|" * len(sources)
        lines += [header, sep]

        for e_mec, src_map in sorted(oa_by_inst.items()):
            inst_name = next(iter(src_map.values())).institution_name
            row = f"| {inst_name} |"
            for s in sources:
                r = src_map.get(s)
                if r and r.oa_rate and not r.oa_rate.suppressed:
                    row += f" {r.oa_rate.estimate:.1%} |"
                elif r and r.n_records == 0:
                    row += " not found |"
                else:
                    row += " — |"
            lines.append(row)

        # Overlap matrix summary
        overlap = convergence.get("overlap_matrix", [])
        if overlap:
            lines += ["", "---", "",
                      "## Source Overlap Matrix (% of smaller set matched)", "",
                      "> **Ceiling caveat:** When both sources hit the 500-record ceiling the overlap "
                      "reflects a sample-vs-sample comparison, not full-corpus overlap. "
                      "True overlap for high-output institutions is likely higher.", ""]
            # Build pivot: for each source pair, average overlap across institutions
            from collections import defaultdict
            pair_overlaps: dict[tuple, list[float]] = defaultdict(list)
            for o in overlap:
                pct = o.get("overlap_pct_min")
                if pct is not None:
                    pair_overlaps[(o["source_a"], o["source_b"])].append(pct)

            header = "| |" + "".join(f" {s} |" for s in sources)
            sep = "|---|" + "---|" * len(sources)
            lines += [header, sep]
            for sa in sources:
                row = f"| {sa} |"
                for sb in sources:
                    if sa == sb:
                        row += " — |"
                    else:
                        key = (sa, sb) if (sa, sb) in pair_overlaps else (sb, sa)
                        vals = pair_overlaps.get(key, [])
                        avg = sum(vals) / len(vals) if vals else None
                        row += f" {avg:.1%} |" if avg is not None else " ? |"
                lines.append(row)

        # Divergence summary
        divergences = convergence.get("divergences", [])
        if divergences:
            lines += ["", "---", "", f"## Divergence Flags ({len(divergences)} total)", "",
                      "| Institution | Source A | Source B | Count A | Count B | Discrepancy |",
                      "|---|---|---|---|---|---|"]
            # Track which counts hit the ceiling for flagging
            ceiling = convergence.get("ceiling", 500)
            for d in sorted(divergences, key=lambda x: -x.discrepancy_pct)[:20]:
                a_flag = "⚠" if d.count_a >= ceiling else ""
                b_flag = "⚠" if d.count_b >= ceiling else ""
                lines.append(
                    f"| {d.institution_name} | {d.source_a} | {d.source_b} | "
                    f"{d.count_a}{a_flag} | {d.count_b}{b_flag} | {d.discrepancy_pct:.1%} |"
                )

        # Structural gaps
        lines += [
            "", "---", "",
            "## Structural Gaps",
            "",
            "- **Extensão universitária**: absent from all evaluated sources",
            "- **Livros e capítulos de livros**: systematically undercounted",
            "",
            "---",
            "",
            f"*Generated by inep-bibliometric-tool v0.1.0*",
        ]

        path = self.output_dir / f"phase2_report_{run_id}.md"
        path.write_text("\n".join(lines), encoding="utf-8")
        logger.info(f"Phase 2 report: {path}")
        return path

    # ------------------------------------------------------------------
    # Fitness matrix & report
    # ------------------------------------------------------------------

    def export_fitness_matrix(self, matrix: "FitnessMatrix", run_id: str) -> Path:
        """Export fitness matrix as CSV and SQLite."""
        records = matrix.to_records()
        df = pd.DataFrame(records)
        path = self.output_dir / f"fitness_matrix_{run_id}.csv"
        df.to_csv(path, index=False, encoding="utf-8")
        logger.info(f"Fitness matrix: {path} ({len(df)} rows)")
        db_path = self.output_dir / f"fitness_{run_id}.db"
        try:
            import sqlite3
            with sqlite3.connect(db_path) as conn:
                # append so reruns with same run_id accumulate rather than silently wipe
                df.to_sql("fitness_matrix", conn, if_exists="append", index=False)
        except Exception as exc:
            logger.error(f"Fitness matrix SQLite write failed: {exc}")
            raise
        return path

    def export_fitness_report(self, matrix: "FitnessMatrix", run_id: str) -> Path:
        """Markdown fitness report with rankings and Barcelona Declaration note."""
        inst_types = sorted({r.inst_type for r in matrix.rows})
        sources    = sorted({r.source for r in matrix.rows})
        dims = ["coverage", "data_quality", "reliability", "accessibility",
                "social_impact", "governance", "innovation_link"]

        lines = [
            "# INEP Bibliometric Tool — Source Fitness Report",
            "",
            f"**Run:** `{run_id}`  ",
            f"**Generated:** {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
            "",
            "> Composite score = weighted average across 7 dimensions "
            "(Coverage 20%, Data Quality 20%, Reliability 15%, "
            "Accessibility 20%, Social Impact 10%, Governance 10%, Innovation 5%)",
            "",
            "> ★ = recommended primary source  |  ☆ = recommended supplementary",
            "",
            "---",
            "",
            "## Composite Fitness Matrix",
            "",
        ]

        header = "| Institution Type |" + "".join(f" {s} |" for s in sources)
        sep    = "|---|" + "---|" * len(sources)
        lines += [header, sep]
        for inst_type in inst_types:
            ranking = matrix.rank_by_inst_type(inst_type)
            top     = ranking[0]["source"] if ranking else None
            row = f"| {inst_type} |"
            for s in sources:
                score_rec = next((r for r in ranking if r["source"] == s), None)
                if score_rec:
                    flag = " ★" if s == top else ""
                    row += f" {score_rec['composite']:.2f}{flag} |"
                else:
                    row += " — |"
            lines.append(row)

        lines += ["", "---", "", "## Recommendations by Institution Type", ""]
        for inst_type in inst_types:
            ranking = matrix.rank_by_inst_type(inst_type)
            lines.append(f"### {inst_type.replace('_', ' ').title()}")
            lines.append("")
            for i, r in enumerate(ranking, 1):
                badge = "★ Primary" if i == 1 else ("☆ Supplementary" if i == 2 else "")
                lines.append(
                    f"{i}. **{r['source']}** — composite {r['composite']:.2f}"
                    f" | accessibility {r['accessibility']:.2f}"
                    + (f"  `{badge}`" if badge else "")
                )
            lines.append("")

        lines += ["---", "", "## Dimension Breakdown by Source", ""]
        dim_header = "| Source |" + "".join(f" {d[:8]} |" for d in dims) + " composite |"
        dim_sep    = "|---|" + "---|" * (len(dims) + 1)
        lines += [dim_header, dim_sep]
        for src in sources:
            src_rows = [r for r in matrix.rows if r.source == src]
            if not src_rows:
                continue
            avg = {d: sum(getattr(r, d) for r in src_rows) / len(src_rows) for d in dims}
            avg_c = sum(r.composite for r in src_rows) / len(src_rows)
            row = f"| {src} |" + "".join(f" {avg[d]:.2f} |" for d in dims)
            row += f" **{avg_c:.2f}** |"
            lines.append(row)

        lines += [
            "", "---", "",
            "## Barcelona Declaration Alignment Note",
            "",
            "Sources scoring above 0.80 on accessibility are aligned with the "
            "[Barcelona Declaration on Open Research Information](https://barcelona-declaration.org/). "
            "SINAES indicator design should prioritise these sources as primary infrastructure.",
            "",
            "---",
            "",
            "*Generated by inep-bibliometric-tool v0.1.0*",
        ]

        path = self.output_dir / f"fitness_report_{run_id}.md"
        path.write_text("\n".join(lines), encoding="utf-8")
        logger.info(f"Fitness report: {path}")
        return path

    def _sqlite_safe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Serialize any list/dict columns to JSON strings for SQLite compatibility."""
        df = df.copy()
        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
                df[col] = df[col].apply(
                    lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (list, dict)) else x
                )
        return df

    def export_sprint1_report(
        self,
        coverage_results: list,
        oa_results: list,
        run_id: str,
        config: dict,
    ) -> Path:
        """
        Generate a plain Markdown report for the sprint 1 finding:
        OA coverage by institution type for the spotlight sample.
        """
        max_records = config.get("max_records", 500)

        lines = [
            f"# INEP Bibliometric Tool — Sprint 1 Report",
            f"",
            f"**Run:** `{run_id}`  ",
            f"**Sources:** OpenAlex + Unpaywall  ",
            f"**Temporal window:** {config.get('start_year', '?')}–{config.get('end_year', '?')}  ",
            f"**Generated:** {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
            f"",
            f"> **Note on record counts:** Results are capped at {max_records:,} records per "
            f"institution per source (`max_records_per_query` in `sample_config.yaml`). "
            f"Where the ceiling was hit, the actual corpus size is shown in parentheses. "
            f"OA rates and coverage metrics are computed on the fetched sample, not the full corpus.",
            f"",
            f"---",
            f"",
            f"## Coverage by Institution",
            f"",
            f"| Institution | Category | Region | Fetched | Actual corpus | Ceiling hit |",
            f"|-------------|----------|--------|---------|---------------|-------------|",
        ]

        for r in coverage_results:
            present = "yes" if r.institution_present else "**NO**"
            total = getattr(r, "total_count", None)
            ceiling = getattr(r, "ceiling_hit", False)
            actual_str = f"{total:,}" if total else "?"
            ceiling_str = "⚠ yes" if ceiling else "no"
            lines.append(
                f"| {r.institution_name} | {r.institution_category or '?'} | "
                f"{r.institution_region or '?'} | {r.n_records} | "
                f"{actual_str} | {ceiling_str} |"
            )

        lines += [
            f"",
            f"---",
            f"",
            f"## Open Access by Institution",
            f"",
            f"OA rates are computed on the fetched sample (N ≤ {max_records:,}). "
            f"Where ceiling was hit, estimates apply to the fetched sample only.",
            f"",
            f"| Institution | N (fetched) | OA% | CI (95%) | Tier | Diamond% | Unpaywall agree% |",
            f"|-------------|-------------|-----|----------|------|----------|-----------------|",
        ]

        for r in oa_results:
            oa = r.oa_rate
            dia = r.diamond_rate
            uw = r.unpaywall_agreement
            ceiling = getattr(r, "ceiling_hit", False)

            n_str = f"{r.n_records}{'⚠' if ceiling else ''}"
            oa_str = f"{oa.estimate:.1%}" if oa and not oa.suppressed else "—"
            ci_str = (
                f"[{oa.ci_low:.1%}, {oa.ci_high:.1%}]"
                if oa and not oa.suppressed else "—"
            )
            tier_str = oa.tier.value if oa else "—"
            dia_str = f"{dia.estimate:.1%}" if dia and not dia.suppressed else "—"
            uw_str = f"{uw.estimate:.1%}" if uw and not uw.suppressed else "—"

            lines.append(
                f"| {r.institution_name} | {n_str} | {oa_str} | "
                f"{ci_str} | {tier_str} | {dia_str} | {uw_str} |"
            )

        lines += [
            f"",
            f"---",
            f"",
            f"## Structural Gaps",
            f"",
            f"The following research activities are **not captured by any evaluated source** "
            f"and represent structural gaps for SINAES indicator design:",
            f"",
            f"- **Extensão universitária**: constitutionally mandated, entirely invisible "
            f"to bibliometric infrastructure",
            f"- **Livros e capítulos de livros**: critical for humanities and social sciences, "
            f"systematically undercounted in OpenAlex and absent from Unpaywall OA tracking",
            f"",
            f"---",
            f"",
            f"*Generated by inep-bibliometric-tool v0.1.0*",
        ]

        path = self.output_dir / f"sprint1_report_{run_id}.md"
        path.write_text("\n".join(lines), encoding="utf-8")
        logger.info(f"Sprint 1 report: {path}")
        return path
