from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import polars_cloud.polars_cloud as pcr


def _parse_dt(value: str | None) -> datetime | None:
    """Parse an RFC3339 timestamp string into a timezone-aware datetime."""
    if value is None:
        return None
    return datetime.fromisoformat(value)


@dataclass
class QueryPlanTiming:
    """Timing breakdown of the query planning phases."""

    plan_start_time: datetime | None
    """When query planning started."""

    parse_start_time: datetime | None
    """When parsing started."""

    optimize_start_time: datetime | None
    """When optimization started."""

    distribute_start_time: datetime | None
    """When distribution planning started."""

    distribute_end_time: datetime | None
    """When distribution planning ended."""

    plan_end_time: datetime | None
    """When query planning ended."""

    @classmethod
    def _from_inner(cls, inner: pcr.QueryPlanTimingPy) -> QueryPlanTiming:
        return cls(
            plan_start_time=_parse_dt(inner.plan_start_time),
            parse_start_time=_parse_dt(inner.parse_start_time),
            optimize_start_time=_parse_dt(inner.optimize_start_time),
            distribute_start_time=_parse_dt(inner.distribute_start_time),
            distribute_end_time=_parse_dt(inner.distribute_end_time),
            plan_end_time=_parse_dt(inner.plan_end_time),
        )

    def __repr__(self) -> str:
        lines = ["QueryPlanTiming:"]
        if self.plan_start_time:
            lines.append(f"  plan_start_time:      {self.plan_start_time.isoformat()}")
        if self.parse_start_time:
            lines.append(f"  parse_start_time:     {self.parse_start_time.isoformat()}")
        if self.optimize_start_time:
            lines.append(
                f"  optimize_start_time:  {self.optimize_start_time.isoformat()}"
            )
        if self.distribute_start_time:
            lines.append(
                f"  distribute_start_time:{self.distribute_start_time.isoformat()}"
            )
        if self.distribute_end_time:
            lines.append(
                f"  distribute_end_time:  {self.distribute_end_time.isoformat()}"
            )
        if self.plan_end_time:
            lines.append(f"  plan_end_time:        {self.plan_end_time.isoformat()}")
        return "\n".join(lines)


@dataclass
class QueryDetail:
    """Detailed information about a query from the observatory."""

    id: str
    user_name: str
    status: str
    request_time: datetime
    start_time: datetime | None
    end_time: datetime | None
    query_plan_timing: QueryPlanTiming
    total_num_stages: int | None
    failed_stage: int | None
    in_progress_stages: list[int]
    finished_stages: list[int]
    total_bytes_shuffled: int | None
    total_node_time_ns: int
    percentage_time_shuffling: float | None
    total_num_files: int | None
    original_num_files: int | None
    total_rows_read: int | None
    errors: list[str] | None
    engine: str
    query_type: str
    output_location: str | None
    output_files: int | None
    output_rows: int | None

    @classmethod
    def _from_inner(cls, inner: pcr.QueryDetailPy) -> QueryDetail:
        return cls(
            id=inner.id,
            user_name=inner.user_name,
            status=inner.status,
            request_time=_parse_dt(inner.request_time),  # type: ignore[arg-type]
            start_time=_parse_dt(inner.start_time),
            end_time=_parse_dt(inner.end_time),
            query_plan_timing=QueryPlanTiming._from_inner(inner.query_plan_timing),
            total_num_stages=inner.total_num_stages,
            failed_stage=inner.failed_stage,
            in_progress_stages=inner.in_progress_stages,
            finished_stages=inner.finished_stages,
            total_bytes_shuffled=inner.total_bytes_shuffled,
            total_node_time_ns=inner.total_node_time_ns,
            percentage_time_shuffling=inner.percentage_time_shuffling,
            total_num_files=inner.total_num_files,
            original_num_files=inner.original_num_files,
            total_rows_read=inner.total_rows_read,
            errors=inner.errors,
            engine=inner.engine,
            query_type=inner.query_type,
            output_location=inner.output_location,
            output_files=inner.output_files,
            output_rows=inner.output_rows,
        )

    def _elapsed_ms(self) -> int | None:
        """Wall-clock duration of the query in milliseconds, if both times are known."""
        if self.start_time is not None and self.end_time is not None:
            return int((self.end_time - self.start_time).total_seconds() * 1000)
        return None

    @staticmethod
    def _status_color(status: str) -> str:
        if status == "Success":
            return "#4CAF50"
        if status in ("Failed", "Canceled"):
            return "#f44336"
        if status == "InProgress":
            return "#2196F3"
        return "#FFC107"

    def _repr_html_(self) -> str:
        """Format output data in HTML for display in Jupyter Notebooks."""
        elapsed = self._elapsed_ms()
        elapsed_str = f"{elapsed:,} ms" if elapsed is not None else "n/a"

        total = self.total_num_stages or 0
        done = len(self.finished_stages)
        in_prog = len(self.in_progress_stages)
        status_color = self._status_color(self.status)

        def row(label: str, value: str) -> str:
            return (
                f"<tr>"
                f'<td style="padding: 4px 16px 4px 0; color: #666; white-space: nowrap;">{label}</td>'
                f'<td style="padding: 4px 0; font-weight: 500;">{value}</td>'
                f"</tr>"
            )

        def code(value: str) -> str:
            return f'<code style="background: #f5f5f5; padding: 2px 6px; border-radius: 3px; font-size: 11px;">{value}</code>'

        # Stage progress bar
        pct_done = (done / total * 100) if total > 0 else 0
        pct_in_prog = (in_prog / total * 100) if total > 0 else 0
        progress_bar = (
            f'<div style="background: #eee; border-radius: 4px; height: 8px; width: 100%; margin-top: 4px; overflow: hidden;">'
            f'  <div style="display: flex; height: 100%;">'
            f'    <div style="width: {pct_done:.1f}%; background: #4CAF50;"></div>'
            f'    <div style="width: {pct_in_prog:.1f}%; background: #2196F3;"></div>'
            f"  </div>"
            f"</div>"
        )

        # Shuffle row
        if self.total_bytes_shuffled is not None:
            pct_shuffle = (
                f' <span style="color: #999; font-size: 11px;">({self.percentage_time_shuffling:.1%} of time)</span>'
                if self.percentage_time_shuffling is not None
                else ""
            )
            shuffle_value = f"{self.total_bytes_shuffled:,} bytes{pct_shuffle}"
        else:
            shuffle_value = None

        rows = [
            row("Query ID", code(self.id)),
            row("User", self.user_name),
            row(
                "Status",
                f'<span style="color: {status_color}; font-weight: 600;">{self.status}</span>',
            ),
            row("Engine", self.engine),
            row("Query type", self.query_type),
            row("Requested at", self.request_time.strftime("%Y-%m-%d %H:%M:%S %Z")),
            row("Elapsed", elapsed_str),
            row(
                "Stages",
                f"{done}/{total} done, {in_prog} in-progress{progress_bar}",
            ),
        ]

        if self.failed_stage is not None:
            rows.append(row("Failed stage", str(self.failed_stage)))

        if shuffle_value is not None:
            rows.append(row("Bytes shuffled", shuffle_value))

        if self.total_rows_read is not None:
            rows.append(row("Rows read", f"{self.total_rows_read:,}"))

        if self.output_rows is not None:
            rows.append(row("Output rows", f"{self.output_rows:,}"))

        if self.output_location is not None:
            rows.append(row("Output location", code(self.output_location)))

        errors_html = ""
        if self.errors:
            error_items = "".join(
                f'<li style="margin: 2px 0; color: #c62828;">{err}</li>'
                for err in self.errors
            )
            errors_html = (
                f'<div style="margin-top: 12px; padding-top: 12px; border-top: 1px solid #eee;">'
                f'  <div style="color: #c62828; font-weight: 600; margin-bottom: 4px;">Errors</div>'
                f'  <ul style="margin: 0; padding-left: 20px; font-size: 12px;">{error_items}</ul>'
                f"</div>"
            )

        return (
            '<div style="padding: 12px; border: 1px solid #ddd; border-radius: 4px; background: white; max-width: 640px; font-family: sans-serif;">'
            '  <h3 style="margin: 0 0 12px 0; color: #333; font-size: 16px; font-weight: 600;">In Progress Query</h3>'
            '  <table style="border-collapse: collapse; font-size: 13px; width: 100%;">'
            + "".join(rows)
            + "  </table>"
            + errors_html
            + "</div>"
        )

    def __repr__(self) -> str:
        elapsed = self._elapsed_ms()
        elapsed_str = f"{elapsed} ms" if elapsed is not None else "n/a"

        total = self.total_num_stages or 0
        done = len(self.finished_stages)
        in_prog = len(self.in_progress_stages)

        lines = [
            "Query Progress:",
            f"  id:              {self.id}",
            f"  user:            {self.user_name}",
            f"  status:          {self.status}",
            f"  engine:          {self.engine}",
            f"  query_type:      {self.query_type}",
            f"  requested_at:    {self.request_time.isoformat()}",
            f"  elapsed:         {elapsed_str}",
            f"  stages:          {done}/{total} done, {in_prog} in-progress",
        ]

        if self.failed_stage is not None:
            lines.append(f"  failed_stage:    {self.failed_stage}")

        if self.total_bytes_shuffled is not None:
            pct = (
                f" ({self.percentage_time_shuffling:.1%})"
                if self.percentage_time_shuffling is not None
                else ""
            )
            lines.append(f"  bytes_shuffled:  {self.total_bytes_shuffled:,}{pct}")

        if self.total_rows_read is not None:
            lines.append(f"  rows_read:       {self.total_rows_read:,}")

        if self.output_rows is not None:
            lines.append(f"  output_rows:     {self.output_rows:,}")

        if self.output_location is not None:
            lines.append(f"  output_location: {self.output_location}")

        if self.errors:
            lines.append("  errors:")
            for err in self.errors:
                lines.append(f"    - {err}")

        return "\n".join(lines)
