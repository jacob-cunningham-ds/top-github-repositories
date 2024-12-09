import sys
import os
import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
import logging

# Add the project root directory to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from scripts.fetch_data import fetch_github_repos

@pytest.fixture
def mock_github_token():
    """
    Mock the GITHUB_TOKEN environment variable.
    """
    os.environ["GITHUB_TOKEN"] = "fake_token"
    yield
    del os.environ["GITHUB_TOKEN"]

def test_fetch_github_repos_success(mock_github_token):
    """
    Test successful API response and ensure DataFrame is returned with correct data.
    """
    # Mock API response for multiple pages of data
    mock_response_page_1 = {
        "items": [
            {
                "name": "repo1",
                "language": "Python",
                "stargazers_count": 15000,
                "forks_count": 300,
                "created_at": "2021-01-01T00:00:00Z",
                "pushed_at": "2022-01-01T00:00:00Z",
                "html_url": "www.some_example.com",
            },
            {
                "name": "repo2",
                "language": "JavaScript",
                "stargazers_count": 12000,
                "forks_count": 200,
                "created_at": "2020-01-01T00:00:00Z",
                "pushed_at": "2021-01-01T00:00:00Z",
                "html_url": "www.some_other_example.com",
            }
        ]
    }

    mock_response_page_2 = {
        "items": [
            {
                "name": "repo3",
                "language": "Ruby",
                "stargazers_count": 11000,
                "forks_count": 150,
                "created_at": "2022-01-01T00:00:00Z",
                "pushed_at": "2023-01-01T00:00:00Z",
                "html_url": "www.third_example.com",
            }
        ]
    }

    # Mocking an empty page for the last iteration to stop pagination
    mock_response_empty = {
        "items": []
    }

    with patch("scripts.fetch_data.requests.get") as mock_get:
        # Configure the mock to return paginated responses
        mock_get.return_value.status_code = 200
        mock_get.return_value.json = MagicMock(side_effect=[mock_response_page_1, mock_response_page_2, mock_response_empty])

        # Call the function
        df = fetch_github_repos()

        # Assertions
        assert not df.empty, "DataFrame should not be empty."
        assert list(df.columns) == ["name", "language", "stars", "forks", "creation_date", "last_commit_date", "repo_url"]
        assert len(df) == 3  # Two repos from page 1 and one from page 2
        assert df.iloc[0]["name"] == "repo1"
        assert df.iloc[2]["name"] == "repo3"  # Ensure the repo from the second page is included

def test_fetch_github_repos_missing_token():
    """
    Test if the function raises an error when GITHUB_TOKEN is missing.
    """
    # Remove token from environment
    if "GITHUB_TOKEN" in os.environ:
        del os.environ["GITHUB_TOKEN"]

    # Assert EnvironmentError is raised
    with pytest.raises(EnvironmentError, match="GitHub token not found"):
        fetch_github_repos()

def test_fetch_github_repos_api_error(mock_github_token):
    """
    Test if the function handles API errors correctly.
    """
    with patch("scripts.fetch_data.requests.get") as mock_get:
        # Configure the mock to return an error response
        mock_get.return_value.status_code = 403
        mock_get.return_value.text = "API rate limit exceeded"

        # Call the function
        df = fetch_github_repos()

        # Assertions
        assert df.empty, "DataFrame should be empty on API error."

def test_fetch_github_repos_logging(mock_github_token, caplog):
    """
    Test if the function logs appropriate messages during execution.
    """
    with patch("scripts.fetch_data.requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json = MagicMock(return_value={"items": []})

        with caplog.at_level(logging.INFO):
            fetch_github_repos()

        assert "Fetching page 1..." in caplog.text  # Updated to reflect the log message
        assert "No more data to fetch." in caplog.text  # Updated to reflect the log message
        assert "No data fetched." in caplog.text

def test_fetch_github_repos_empty_response(mock_github_token):
    """
    Test if the function handles an empty API response gracefully.
    """
    with patch("scripts.fetch_data.requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json = MagicMock(return_value={"items": []})

        df = fetch_github_repos()
        assert df.empty, "DataFrame should be empty for an empty API response."
