import pandas as pd
from pathlib import Path

# Load input datasets from Week 1
BASE_DIR = Path(__file__).resolve().parent
sold_file = BASE_DIR / "CRMLSSold_Combined_Residential.csv"
listing_file = BASE_DIR / "CRMLSListing_Combined_Residential.csv"

sold = pd.read_csv(sold_file)
listings = pd.read_csv(listing_file)

# Inspect dataset structure
print("SOLD SHAPE:", sold.shape)
print("LISTINGS SHAPE:", listings.shape)
print(sold.head())
print(listings.head())

# Show column types
print("\nSOLD DTYPES:\n", sold.dtypes)
print("\nLISTINGS DTYPES:\n", listings.dtypes)

# Check unique property types
print("\nSOLD PROPERTY TYPES:", sold["PropertyType"].unique())
print("LISTINGS PROPERTY TYPES:", listings["PropertyType"].unique())

# Build missing value report
def missing_report(df):
    report = pd.DataFrame({
        "column": df.columns,
        "null_count": df.isnull().sum().values,
        "null_pct": df.isnull().mean().values * 100
    }).sort_values(by="null_pct", ascending=False)
    return report

sold_missing = missing_report(sold)
listings_missing = missing_report(listings)

print("\nSOLD MISSING:\n", sold_missing.head(20))
print("\nLISTINGS MISSING:\n", listings_missing.head(20))

# Flag columns with >90% missing values
sold_high_missing = sold_missing[sold_missing["null_pct"] > 90]
listings_high_missing = listings_missing[listings_missing["null_pct"] > 90]

print("\nSOLD >90% MISSING:\n", sold_high_missing)
print("\nLISTINGS >90% MISSING:\n", listings_high_missing)

# Generate numeric summaries for key fields
def numeric_summary(df, cols):
    summary = {}
    for col in cols:
        if col in df.columns:
            series = pd.to_numeric(df[col], errors="coerce")
            summary[col] = {
                "min": series.min(),
                "max": series.max(),
                "mean": series.mean(),
                "median": series.median()
            }
    return pd.DataFrame(summary).T

fields = ["ClosePrice", "LivingArea", "DaysOnMarket"]

sold_summary = numeric_summary(sold, fields)
listings_summary = numeric_summary(listings, fields)

print("\nSOLD NUMERIC SUMMARY:\n", sold_summary)
print("\nLISTINGS NUMERIC SUMMARY:\n", listings_summary)

# Save validated datasets
sold.to_csv(BASE_DIR / "CRMLSSold_Validated_Residential.csv", index=False)
listings.to_csv(BASE_DIR / "CRMLSListing_Validated_Residential.csv", index=False)

# Fetch mortgage rate data from FRED
url = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=MORTGAGE30US"
mortgage = pd.read_csv(url, parse_dates=["DATE"])
mortgage.columns = ["date", "rate_30yr_fixed"]

# Convert to monthly averages
mortgage["year_month"] = mortgage["date"].dt.to_period("M")
mortgage_monthly = mortgage.groupby("year_month")["rate_30yr_fixed"].mean().reset_index()

# Create matching keys for merge
sold["CloseDate"] = pd.to_datetime(sold["CloseDate"], errors="coerce")
listings["ListingContractDate"] = pd.to_datetime(listings["ListingContractDate"], errors="coerce")

sold["year_month"] = sold["CloseDate"].dt.to_period("M")
listings["year_month"] = listings["ListingContractDate"].dt.to_period("M")

# Merge mortgage rates onto datasets
sold_with_rates = sold.merge(mortgage_monthly, on="year_month", how="left")
listings_with_rates = listings.merge(mortgage_monthly, on="year_month", how="left")

# Validate merge results
print("\nSOLD NULL RATES:", sold_with_rates["rate_30yr_fixed"].isnull().sum())
print("LISTINGS NULL RATES:", listings_with_rates["rate_30yr_fixed"].isnull().sum())

# Save enriched datasets
sold_with_rates.to_csv(BASE_DIR / "CRMLSSold_WithRates.csv", index=False)
listings_with_rates.to_csv(BASE_DIR / "CRMLSListing_WithRates.csv", index=False)

print("\nWeek 2 complete.")