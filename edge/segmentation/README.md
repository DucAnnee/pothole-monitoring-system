# **summary.py**: script to get the summary of best metrics in each .csv in results/ folder

## Usage

### For segmentation (default)
python summary.py --folder ./results --recursive

### For detection
python summary.py --folder ./results --recursive --metric "metrics/mAP50-95(B)"

### Minimize loss instead
python summary.py --folder ./results --recursive --metric "val/seg_loss" --goal min

