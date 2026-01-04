# Large File Storage Note

The following files exceed GitHub's API upload limit and need to be added via Git LFS:

- `data/winemag_150k.csv` (48 MB) - 150,000 wine reviews
- `data/kaggle_wine_reviews_130k.csv` (51 MB) - 130,000 wine reviews

## To add locally:

```bash
# Clone the repo
git clone https://github.com/roy-henderson/javari-spirits-data.git
cd javari-spirits-data

# Install Git LFS
git lfs install

# Track large CSV files
git lfs track "data/*.csv"

# Add the large files
# (download from the automation package or data sources)
cp /path/to/winemag_150k.csv data/
cp /path/to/kaggle_wine_reviews_130k.csv data/

# Commit and push
git add .
git commit -m "Add large wine datasets via LFS"
git push
```

## Alternative: Direct Download Links

The large datasets can also be downloaded from:
- WineMag: https://www.kaggle.com/datasets/zynicide/wine-reviews
- Kaggle Wine: https://www.kaggle.com/datasets/mysarahmadbhat/wine-tasting
