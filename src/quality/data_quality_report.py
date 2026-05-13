from __future__ import annotations

from src.config.settings import DOCS_ROOT, REPORTS_ROOT
from src.quality.validate_formatted import validate_formatted_data
from src.quality.validate_raw import validate_raw_data
from src.utils.file_utils import ensure_dir, write_json, write_text


def generate_data_quality_report(run_date: str | None = None) -> str:
    raw_report = validate_raw_data(run_date=run_date)
    formatted_report = validate_formatted_data(run_date=run_date)

    report = {
        "run_date": run_date,
        "raw": raw_report,
        "formatted": formatted_report,
    }

    ensure_dir(REPORTS_ROOT)
    ensure_dir(DOCS_ROOT)

    json_path = REPORTS_ROOT / "data_quality_report.json"
    markdown_path = DOCS_ROOT / "data_quality_report.md"
    write_json(json_path, report)

    lines = [
        "# Data Quality Report",
        "",
        "## Raw Layer",
        "",
    ]

    for table_name, result in raw_report["tables"].items():
        lines.append(f"- `{table_name}`: status={result['status']}, files={result.get('file_count', 0)}, partition={result.get('partition')}")

    lines.extend(["", "## Formatted / Usage Layer", ""])

    for table_name, result in formatted_report["tables"].items():
        row_count = result.get("row_count", "n/a")
        lines.append(f"- `{table_name}`: status={result['status']}, rows={row_count}, partition={result.get('partition')}")

    write_text(markdown_path, "\n".join(lines))
    return str(json_path)
