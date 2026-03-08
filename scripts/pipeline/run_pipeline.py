"""
Run the dataset pipeline in strict sequential order.
"""

from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
import argparse
import subprocess
import sys
import time

root = Path(__file__).resolve().parents[2]
src = root / "src"
if str(src) not in sys.path:
    sys.path.insert(0, str(src))

from pink_tax.config import get_paths, load_pipeline_definition

@dataclass(frozen=True)
class pipeline_step:
    """
    Pipeline step definition.
    """

    key: str
    description: str
    command: list[str]

def load_steps(config_path: Path) -> list[pipeline_step]:
    """
    Load and validate step list from JSON.
    """

    cfg = load_pipeline_definition(config_path)
    raw_steps = cfg.get("steps", [])
    steps = [pipeline_step(s["key"], s["description"], s["command"]) for s in raw_steps]

    if not steps:
        raise ValueError("No steps found in pipeline config.")
    return steps

def run_one_step(step: pipeline_step, repo_root: Path) -> None:
    """
    Run one step and fail fast on non-zero exit.
    """

    start = time.time()
    result = subprocess.run([sys.executable, *step.command], cwd=repo_root, check=False)
    elapsed = time.time() - start

    if result.returncode != 0:
        print(f"   failed (exit {result.returncode})")
        raise SystemExit(result.returncode)

def main() -> None:
    """
    CLI entrypoint.
    """

    paths = get_paths(root)

    parser = argparse.ArgumentParser(description="Run scraping + dataset cleaning pipeline.")
    parser.add_argument(
        "--pipeline-config",
        default=str(paths.pipeline_steps),
        help="Path to pipeline config JSON.",
    )
    parser.add_argument(
        "--step",
        default=None,
        help="Optional single step key to run.",
    )
    args = parser.parse_args()

    config_path = Path(args.pipeline_config)
    steps = load_steps(config_path)

    if args.step:
        lookup = {s.key: s for s in steps}
        if args.step not in lookup:
            raise SystemExit(f"Unknown step: {args.step}")
        steps = [lookup[args.step]]

    for step in steps:
        run_one_step(step, paths.root)

if __name__ == "__main__":
    main()