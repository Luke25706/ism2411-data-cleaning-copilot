"""
data_cleaning.py

Small data-cleaning script for ISM2411.
- Load raw sales data from data/raw/sales_data_raw.csv
- Clean it
- Save cleaned data to data/processed/sales_data_clean.csv
"""

import pandas as pd


# WHAT: Load the raw CSV into a pandas DataFrame.
# WHY: This separates file I/O from the cleaning logic.
def load_data(file_path: str) -> pd.DataFrame:
    df = pd.read_csv(file_path)
    return df


# WHAT: Standardize column names (strip spaces, lowercase, underscores).
# WHY: The raw file has names like "ProdName " and " CATEGORY ".
#      After this step we get: prodname, category, price, qty, date_sold.
def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (
        df.columns
        .str.strip()            # remove spaces at start/end
        .str.lower()            # lowercase
        .str.replace(" ", "_")  # spaces -> underscores
    )
    return df


# WHAT: Clean and normalize category values so duplicates are merged.
# WHY: The raw data has categories like "Office", " office ", and
#      "Office furniture" that should all be treated as the same category.
def clean_categories(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    if "category" in df.columns:
        cat = (
            df["category"]
            .astype(str)
            .str.strip()
            .str.strip('"')
            .str.replace(r"\s+", " ", regex=True)
            .str.lower()
        )

        # function to normalize similar categories
        def normalize_category(c: str) -> str:
            if "office" in c:
                return "office"
            if "electronic" in c:
                return "electronics"
            if "kitchen" in c:
                return "kitchen"
            if "fitness" in c:
                return "fitness"
            return c

        df["category"] = cat.apply(normalize_category)

    return df

# WHAT: Clean text fields and handle missing prices/quantities.
# WHY: We need product and category names without extra spaces, and we
#      drop rows where price or qty is missing so they don't break totals.
def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # clean product names: strip spaces, collapse multiple spaces, and normalize case
    if "prodname" in df.columns:
        df["prodname"] = (
            df["prodname"]
            .astype(str)
            .str.strip()
            .str.replace(r"\s+", " ", regex=True)  # collapse multiple spaces
            .str.title()                           # "standing desk" -> "Standing Desk"
        )

    # convert price and qty to numeric and drop rows where they are missing
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["qty"] = pd.to_numeric(df["qty"], errors="coerce")
    df = df.dropna(subset=["price", "qty"])

    return df


# WHAT: Remove clearly invalid rows.
# WHY: Negative or zero price/qty are 
# data entry errors and would distort sales.
def remove_invalid_rows(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = df[(df["price"] > 0) & (df["qty"] > 0)]
    return df


# WHAT: Clean the date_sold column by converting it 
# to a proper datetime and removing rows where the date is missing or invalid.
# WHY: The raw file contains at least one row with no date at all,
# which would cause errors in time-based analysis and sorting.
def clean_dates(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    if "date_sold" in df.columns:
        # Convert to datetime; invalid or empty values become NaT
        df["date_sold"] = pd.to_datetime(df["date_sold"], errors="coerce")

        # Drop rows with invalid or missing dates
        df = df.dropna(subset=["date_sold"])

    return df


if __name__ == "__main__":
    raw_path = "data/raw/sales_data_raw.csv"
    cleaned_path = "data/processed/sales_data_clean.csv"

    df_raw = load_data(raw_path)
    df_clean = clean_column_names(df_raw)
    df_clean = clean_categories(df_clean)
    df_clean = clean_dates(df_clean)
    df_clean = handle_missing_values(df_clean)
    df_clean = remove_invalid_rows(df_clean)

    # WHAT: Remove exact duplicate rows.
    # WHY: Some products (like Pen Set and Standing Desk)
    # appear twice with the same values after cleaning, which we treat as duplicates.
    df_clean = df_clean.drop_duplicates()

    df_clean.to_csv(cleaned_path, index=False)
    print("Cleaning complete. First few rows:")
    print(df_clean.head())