"""
Run build/clean/all dataset pipeline stages from one entrypoint.
"""

from __future__ import annotations
import argparse
from pathlib import Path
import subprocess
import sys

root = Path(__file__).resolve().parent
runner = root / "scripts" / "pipeline" / "run_pipeline.py"
config_by_mode = {
    "all": root / "config" / "pipeline_steps.json",
    "build": root / "config" / "pipeline_build_steps.json",
    "clean": root / "config" / "pipeline_clean_steps.json",
}

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run pipeline stages for dataset build and cleaning.")
    parser.add_argument(
        "--mode",
        choices=["all", "build", "clean"],
        default="all",
        help="Which pipeline stage group to run.",
    )
    parser.add_argument(
        "--pipeline-config",
        default="",
        help="Optional explicit pipeline config JSON path. Overrides --mode defaults.",
    )
    parser.add_argument(
        "--step",
        default="",
        help="Optional single step key to run from selected config.",
    )
    parser.add_argument(
        "--include-optional",
        action="store_true",
        help="Compatibility flag. Optional pipeline steps are included by default.",
    )
    parser.add_argument(
        "--no-optional",
        action="store_true",
        help="Skip optional pipeline steps.",
    )
    args = parser.parse_args()

    config_path = Path(args.pipeline_config) if args.pipeline_config else config_by_mode[args.mode]
    cmd = [sys.executable, str(runner), "--pipeline-config", str(config_path)]

    if args.step:
        cmd.extend(["--step", args.step])
    if not args.no_optional:
        cmd.append("--include-optional")

    result = subprocess.run(cmd, cwd=root, check=False)
    raise SystemExit(result.returncode)
