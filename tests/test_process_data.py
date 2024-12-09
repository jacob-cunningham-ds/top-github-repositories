import sys
import pytest
import pandas as pd
import os
import logging
from unittest.mock import patch

# Add the project root directory to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from scripts.process_data import load_raw_data, process_data, save_processed_data

@pytest.fixture
def sample_raw_data():
    """
    Provide sample raw data for testing.
    """
    data = {
        "name": ["repo1", "repo2"],
        "language": ["Python", None],
        "stars": [100, 50],
        "forks": [10, 5],
        "creation_date": ["2020-01-01T00:00:00Z", "2019-01-01T00:00:00Z"],
        "last_commit_date": ["2022-01-01T00:00:00Z", "2021-01-01T00:00:00Z"]
    }
    return pd.DataFrame(data)

@pytest.fixture
def temp_file_path(tmpdir):
    """
    Provide a temporary file path for testing save functionality.
    """
    return tmpdir.join("output.csv")

def test_load_raw_data(tmpdir):
    """
    Test loading raw data from a valid CSV file.
    """
    # Create a temporary CSV file
    test_file = tmpdir.join("test.csv")
    data = {"name": ["repo1"], "stars": [100]}
    pd.DataFrame(data).to_csv(test_file, index=False)

    # Test loading the data
    df = load_raw_data(test_file)
    assert not df.empty, "Loaded DataFrame should not be empty."
    assert list(df.columns) == ["name", "stars"], "Column names do not match expected."

def test_load_raw_data_file_not_found_logging(caplog):
    """
    Test logging when raw data file is not found.
    """
    with caplog.at_level(logging.ERROR):
        with pytest.raises(FileNotFoundError):
            load_raw_data("non_existent_file.csv")

    # Check for the appropriate log message
    assert "Error loading raw data:" in caplog.text

def test_process_data(sample_raw_data):
    """
    Test processing raw data, including derived fields and normalization.
    """
    # Convert dates to datetime (handling already timezone-aware timestamps)
    sample_raw_data["creation_date"] = pd.to_datetime(sample_raw_data["creation_date"])
    sample_raw_data["last_commit_date"] = pd.to_datetime(sample_raw_data["last_commit_date"])

    processed_df = process_data(sample_raw_data)

    # Assertions for derived fields
    assert "popularity_score" in processed_df.columns, "Missing popularity_score column."
    assert "popularity_score_normalized" in processed_df.columns, "Missing normalized popularity_score column."
    assert processed_df["language"].iloc[1] == "Unknown", "Missing values in language were not handled correctly."
    assert processed_df["popularity_score"].iloc[0] == 120, "Incorrect calculation of popularity_score."
    assert processed_df["popularity_score_normalized"].iloc[0] == 100, "Normalization failed for popularity_score."


def test_process_data_logging(sample_raw_data, caplog):
    """
    Test logging messages during data processing.
    """
    # Convert dates to datetime
    sample_raw_data["creation_date"] = pd.to_datetime(sample_raw_data["creation_date"]).dt.tz_convert("UTC")
    sample_raw_data["last_commit_date"] = pd.to_datetime(sample_raw_data["last_commit_date"]).dt.tz_convert("UTC")

    with caplog.at_level(logging.INFO):
        process_data(sample_raw_data)

    # Check for log messages
    assert "Processing data..." in caplog.text
    assert "Data processing complete." in caplog.text

def test_save_processed_data(sample_raw_data, temp_file_path):
    """
    Test saving processed data to a CSV file.
    """
    save_processed_data(sample_raw_data, temp_file_path)

    # Verify the file was saved correctly
    assert os.path.exists(temp_file_path), "Processed data file was not saved."
    saved_df = pd.read_csv(temp_file_path)
    assert list(saved_df.columns) == list(sample_raw_data.columns), "Saved DataFrame columns do not match input."

def test_save_processed_data_invalid_path_logging(sample_raw_data, caplog):
    """
    Test logging messages when saving data to an invalid path.
    """
    with caplog.at_level(logging.ERROR):
        with pytest.raises(OSError):
            save_processed_data(sample_raw_data, "/invalid/path/output.csv")

    # Check for the appropriate log message
    assert "Error saving processed data:" in caplog.text
