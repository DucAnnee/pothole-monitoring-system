from pathlib import Path

RESULT_DIR = Path("./results")

for f in RESULT_DIR.glob("*.csv"):
    name = f.stem  # Get filename without extension
    version, variant, epochs = name.split("_")
    new_name = f"{version}_{epochs}_{variant}.csv"
    f.rename(RESULT_DIR / new_name)
