"""
Summarize YOLO/Ultralytics results.csv files in a folder:
- Find the best epoch based on a primary metric (like mAP50-95)
- Save all metrics from that epoch

Usage:
  python summary.py --folder ./results --recursive \
    --metric "metrics/mAP50-95(M)" --goal max \
    --out summary.csv
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    # Ultralytics sometimes writes headers with leading/trailing spaces
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df


def _find_epoch_col(df: pd.DataFrame) -> str | None:
    for cand in ["epoch", "Epoch", "epochs", "step", "Step"]:
        if cand in df.columns:
            return cand
    return None


def summarize_folder(
    folder: Path, recursive: bool, metric: str, goal: str
) -> pd.DataFrame:
    pattern = "**/*.csv" if recursive else "*.csv"
    csv_files = sorted(folder.glob(pattern))

    rows: List[Dict[str, Any]] = []

    for csv_path in csv_files:
        try:
            df = pd.read_csv(csv_path)
            df = _normalize_columns(df)
            epoch_col = _find_epoch_col(df)

            if metric not in df.columns:
                rows.append(
                    {
                        "file": str(csv_path),
                        "run": csv_path.parent.name,
                        "error": f"Metric '{metric}' not found in CSV",
                    }
                )
                continue

            # Drop rows with NaN in the metric column
            df_clean = df.dropna(subset=[metric])
            if df_clean.empty:
                rows.append(
                    {
                        "file": str(csv_path),
                        "run": csv_path.parent.name,
                        "error": f"All values for '{metric}' are NaN",
                    }
                )
                continue

            # Find best epoch
            if goal.lower() == "max":
                best_idx = df_clean[metric].idxmax()
            else:
                best_idx = df_clean[metric].idxmin()

            best_row = df_clean.loc[best_idx]

            # Build output row with all metrics from best epoch
            row: Dict[str, Any] = {
                "file": str(csv_path.relative_to(folder)),
            }

            # Add epoch
            if epoch_col:
                row["best_epoch"] = int(best_row[epoch_col])
            else:
                row["best_epoch"] = int(best_idx)

            # Add all other columns from the best row
            for col in df.columns:
                if col != epoch_col:  # Skip epoch column (already added)
                    value = best_row[col]
                    # Convert to appropriate type
                    if pd.notna(value):
                        try:
                            row[col] = float(value)
                        except (ValueError, TypeError):
                            row[col] = value
                    else:
                        row[col] = None

            rows.append(row)

        except Exception as e:
            rows.append(
                {
                    "file": str(csv_path),
                    "run": csv_path.parent.name,
                    "error": repr(e),
                }
            )

    return pd.DataFrame(rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--folder",
        type=str,
        default="./results/",
        help="Folder containing YOLO results.csv files.",
    )
    ap.add_argument(
        "--recursive", action="store_true", help="Search subfolders recursively."
    )
    ap.add_argument(
        "--metric",
        type=str,
        default="metrics/mAP50-95(M)",
        help='Metric to use for finding best epoch (e.g., "metrics/mAP50-95(M)" for segmentation)',
    )
    ap.add_argument(
        "--goal",
        type=str,
        default="max",
        choices=["max", "min"],
        help="Whether to maximize or minimize the metric",
    )
    ap.add_argument(
        "--out", type=str, default="yolo_best_summary.csv", help="Output CSV filename."
    )
    args = ap.parse_args()

    folder = Path(args.folder).expanduser().resolve()
    if not folder.exists():
        raise SystemExit(f"Folder not found: {folder}")

    df_out = summarize_folder(folder, args.recursive, args.metric, args.goal)

    out_path = Path(args.out).expanduser().resolve()
    df_out.to_csv(out_path, index=False)
    print(f"Saved: {out_path}")
    print(f"\nBest epochs based on {args.metric} ({args.goal}):\n")
    print(df_out.to_string(index=False))


if __name__ == "__main__":
    main()
