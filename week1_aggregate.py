import pandas as pd
from pathlib import Path

# --------------------------------------------------
# Week 1 - Monthly Dataset Aggregation
# --------------------------------------------------
# 1. Finds all monthly CRMLS sold and listing CSV files
# 2. Concatenates them into two combined datasets
# 3. Filters both datasets to PropertyType == 'Residential'
# 4. Prints row counts before/after concat and filtering
# 5. Saves the final combined residential CSVs
# --------------------------------------------------

# Use the current project folder
BASE_DIR = Path(__file__).resolve().parent

# File patterns
sold_pattern = "CRMLSSold*.csv"
listing_pattern = "CRMLSListing*.csv"

# Find all matching files and sort them
sold_files = sorted(BASE_DIR.glob(sold_pattern))
listing_files = sorted(BASE_DIR.glob(listing_pattern))

# Basic safety check
if not sold_files:
    raise FileNotFoundError("No sold CSV files found matching CRMLSSold*.csv")
if not listing_files:
    raise FileNotFoundError("No listing CSV files found matching CRMLSListing*.csv")

print("Sold files found:")
for f in sold_files:
    print(" -", f.name)

print("\nListing files found:")
for f in listing_files:
    print(" -", f.name)

# Load and concatenate SOLD data

sold_dfs = []
sold_row_counts_before_concat = 0

for file in sold_files:
    df = pd.read_csv(file)
    row_count = len(df)
    sold_row_counts_before_concat += row_count
    sold_dfs.append(df)
    print(f"Loaded {file.name}: {row_count} rows")

sold = pd.concat(sold_dfs, ignore_index=True)
sold_row_count_after_concat = len(sold)

print("\n--- SOLD DATA ROW COUNTS ---")
print(f"Total rows before concatenation: {sold_row_counts_before_concat}")
print(f"Rows after concatenation: {sold_row_count_after_concat}")

# Filter to Residential only
sold_rows_before_filter = len(sold)
sold = sold[sold["PropertyType"] == "Residential"].copy()
sold_rows_after_filter = len(sold)

print(f"Rows before Residential filter: {sold_rows_before_filter}")
print(f"Rows after Residential filter: {sold_rows_after_filter}")

# Load and concatenate LISTING data

listing_dfs = []
listing_row_counts_before_concat = 0

for file in listing_files:
    df = pd.read_csv(file)
    row_count = len(df)
    listing_row_counts_before_concat += row_count
    listing_dfs.append(df)
    print(f"Loaded {file.name}: {row_count} rows")

listings = pd.concat(listing_dfs, ignore_index=True)
listing_row_count_after_concat = len(listings)

print("\n--- LISTING DATA ROW COUNTS ---")
print(f"Total rows before concatenation: {listing_row_counts_before_concat}")
print(f"Rows after concatenation: {listing_row_count_after_concat}")

# Filter to Residential only
listing_rows_before_filter = len(listings)
listings = listings[listings["PropertyType"] == "Residential"].copy()
listing_rows_after_filter = len(listings)

print(f"Rows before Residential filter: {listing_rows_before_filter}")
print(f"Rows after Residential filter: {listing_rows_after_filter}")

# Save outputs

sold_output_file = BASE_DIR / "CRMLSSold_Combined_Residential.csv"
listing_output_file = BASE_DIR / "CRMLSListing_Combined_Residential.csv"

sold.to_csv(sold_output_file, index=False)
listings.to_csv(listing_output_file, index=False)

print("\nSaved output files:")
print(" -", sold_output_file.name)
print(" -", listing_output_file.name)

print("\nWeek 1 aggregation complete.")