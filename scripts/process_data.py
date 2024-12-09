import pandas as pd
import os
import logging
from pandas import Timestamp

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/process_data.log"),
        logging.StreamHandler()
    ]
)

def load_raw_data(input_path):
    """
    Load raw data from a CSV file.

    Args:
        input_path (str): Path to the raw data CSV file.

    Returns:
        pd.DataFrame: Raw data as a DataFrame.
    """
    try:
        logging.info(f"Loading raw data from {input_path}...")
        return pd.read_csv(input_path)
    except FileNotFoundError as e:
        logging.error(f"Error loading raw data: {e}")
        raise FileNotFoundError("Error loading raw data: File not found.")

def process_data(df):
    """
    Process the raw data by cleaning, transforming, adding derived fields, 
    and normalizing the popularity score.

    Args:
        df (pd.DataFrame): Raw data.

    Returns:
        pd.DataFrame: Processed data with derived fields and normalized popularity score.
    """
    logging.info("Processing data...")

    # Handle missing values
    df["language"] = df["language"].fillna("Unknown")

    # Convert dates to datetime
    df["creation_date"] = pd.to_datetime(df["creation_date"])
    df["last_commit_date"] = pd.to_datetime(df["last_commit_date"])

    # Use timezone-aware current time
    now = Timestamp.now(tz="UTC")

    # Add derived fields
    df["repo_age_years"] = (now - df["creation_date"]).dt.days / 365
    df["repo_age_days"] = (now - df["creation_date"]).dt.days
    df["days_since_last_commit"] = (now - df["last_commit_date"]).dt.days
    df["is_active"] = df["days_since_last_commit"] <= 180
    df["stars_per_year"] = df["stars"] / df["repo_age_years"]
    df["forks_per_year"] = df["forks"] / df["repo_age_years"]
    df["popularity_score"] = df["stars"] + (2 * df["forks"])
    df["engagement_rate"] = (df["stars"] + df["forks"]) / df["repo_age_years"]
    df["star_to_fork_ratio"] = df["stars"] / df["forks"].replace(0, 1)
    df["language_known"] = df["language"] != "Unknown"
    df["category"] = pd.cut(
        df["stars"],
        bins=[0, 10000, 50000, float("inf")],
        labels=["Low Popularity", "Moderate Popularity", "High Popularity"]
    )

    # Normalize popularity_score (Min-Max normalization to range [0, 100])
    min_val = df["popularity_score"].min()
    max_val = df["popularity_score"].max()
    if min_val == max_val:  # Avoid division by zero
        df["popularity_score_normalized"] = df["popularity_score"].apply(lambda x: 100 if x == max_val else 0)
    else:
        df["popularity_score_normalized"] = 100 * (df["popularity_score"] - min_val) / (max_val - min_val)

    logging.info("Data processing complete.")
    return df

def save_processed_data(df, output_path):
    """
    Save the processed data to a CSV file.

    Args:
        df (pd.DataFrame): Processed data.
        output_path (str): Path to save the processed data.
    """
    try:
        logging.info(f"Saving processed data to {output_path}...")
        df.to_csv(output_path, index=False)
        logging.info("Processed data saved successfully.")
    except OSError as e:
        logging.error(f"Error saving processed data: {e}")
        raise OSError("Error saving processed data: Invalid file path.")

def _main():
    input_path = "data/raw/github_repos.csv"
    output_path = "data/processed/github_repos_processed.csv"

    try:
        # Ensure directories exist
        os.makedirs("logs", exist_ok=True)
        os.makedirs("data/processed", exist_ok=True)

        # Load, process, and save data
        raw_data = load_raw_data(input_path)
        processed_data = process_data(raw_data)
        save_processed_data(processed_data, output_path)
    except Exception as e:
        logging.exception("An error occurred during data processing.")

if __name__ == "__main__":
    _main()
