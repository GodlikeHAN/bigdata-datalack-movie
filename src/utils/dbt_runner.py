from __future__ import annotations

import json
import os
import subprocess

from src.config.settings import DATA_ROOT, DBT_PROJECT_ROOT


def run_dbt_bonus_models(run_date: str | None = None) -> str:
    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = str(DBT_PROJECT_ROOT)

    warehouse_root = DATA_ROOT / "warehouse"
    warehouse_root.mkdir(parents=True, exist_ok=True)

    vars_arg = json.dumps({"run_date": run_date}) if run_date else json.dumps({})
    commands: list[list[str]] = []

    if any((DBT_PROJECT_ROOT / filename).exists() for filename in ("packages.yml", "dependencies.yml")):
        commands.append(["dbt", "deps"])

    commands.extend([
        ["dbt", "run", "--vars", vars_arg],
        ["dbt", "test", "--vars", vars_arg],
        ["dbt", "docs", "generate", "--vars", vars_arg],
    ])

    for command in commands:
        subprocess.run(command, cwd=DBT_PROJECT_ROOT, env=env, check=True)

    return "dbt run/test/docs completed"
