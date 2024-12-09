import os
import requests
import pandas as pd
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/fetch_data.log"),
        logging.StreamHandler()
    ]
)

def ensure_directories():
    """
    Ensures that the required directories for logs and data exist.

    This function creates the `logs` and `data/raw` directories if they don't already exist.
    It is idempotent and will not raise an error if the directories already exist.

    Arguments:
        None

    Returns:
        None
    """
    os.makedirs("logs", exist_ok=True)
    os.makedirs("data/raw", exist_ok=True)

def fetch_github_repos():
    """
    Fetches GitHub repositories with more than 10,000 stars, handling pagination.
    This function will fetch a maximum of 1000 results by fetching 10 pages of 100 results each.

    Arguments:
        None

    Returns:
        pandas.DataFrame: A DataFrame containing the fetched repository data with the following columns:
            - name (str): Name of the repository.
            - language (str): Primary language of the repository.
            - stars (int): Number of stars.
            - forks (int): Number of forks.
            - creation_date (str): Repository creation date.
            - last_commit_date (str): Date of the last commit.
            - repo_url (str): Link to the repository.
    """
    github_token = os.getenv("GITHUB_TOKEN")
    if not github_token:
        logging.error("GitHub token not found. Please set the GITHUB_TOKEN environment variable.")
        raise EnvironmentError("GitHub token not found.")

    url = "https://api.github.com/search/repositories"
    params = {
        "q": "stars:>27500",
        "sort": "stars",
        "order": "desc",
        "per_page": 100
    }
    headers = {"Authorization": f"Bearer {github_token}"}

    repos = []  # List to store all the fetched repositories
    page = 1

    while page <= 10:  # Fetching 10 pages, which gives you 1000 results
        logging.info(f"Fetching page {page}...")
        params["page"] = page  # Increment the page number for each request

        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 200:
            data = response.json()

            if "items" not in data or not data["items"]:
                logging.info(f"No more data to fetch.")
                break  # No more items, exit the loop

            repos.extend([{
                "name": repo["name"],
                "language": repo["language"],
                "stars": repo["stargazers_count"],
                "forks": repo["forks_count"],
                "creation_date": repo["created_at"],
                "last_commit_date": repo["pushed_at"],
                "repo_url": repo["html_url"],
            } for repo in data["items"]])

            logging.info(f"Fetched {len(data['items'])} repositories from page {page}.")
        else:
            logging.error(f"Error fetching page {page}: {response.status_code} - {response.text}")
            break  # Stop fetching if there is an error

        page += 1  # Increment the page for the next request

    if not repos:
        logging.warning("No data fetched.")
        return pd.DataFrame()

    # Convert the list of repositories to a DataFrame
    return pd.DataFrame(repos)

def _main():
    """
    Main function to orchestrate fetching and saving GitHub repository data.

    This function ensures necessary directories exist, calls `fetch_github_repos()`
    to retrieve repository data, and saves the data to a CSV file if available.
    Logs are generated for all actions, including success, warnings, and errors.

    Arguments:
        None

    Returns:
        None
    """
    try:
        ensure_directories()
        df = fetch_github_repos()

        if not df.empty:
            output_path = "data/raw/github_repos.csv"
            df.to_csv(output_path, index=False)
            logging.info(f"Data saved to {output_path}")
        else:
            logging.warning("No data fetched.")
    except Exception as e:
        logging.exception("An error occurred during execution.")

if __name__ == "__main__":
    _main()
