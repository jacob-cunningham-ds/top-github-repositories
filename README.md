# Top GitHub Repositories

This project fetches GitHub repository data using the GitHub API and processes the data.  A threshold is applied to the star count (i.e., greater than 27.5K stars). This results in about 1000 records. 

## Dashboard

[A Tableau dashboard is available](https://public.tableau.com/app/profile/jacob.cunningham3882/viz/PopularGitHubRepositories/TopGitHubRepositories?publish=yes). **The dashboard is optimized for viewing on tablet and desktop**.

![Dashboard Image](https://github.com/jacob-cunningham-ds/top-github-repositories/raw/main/images/dashboard.png)


The dashboard provides various filters to explore the data and direct links to the repositories to get to the source.

![Link to Repo Image](https://github.com/jacob-cunningham-ds/top-github-repositories/raw/main/images/link_to_github_repo.png)

## Folder Structure

- `data/raw/`: Raw data fetched from the GitHub API.
- `data/processed/`: Processed data ready for loading.
- `logs/`: Logs for tracking pipeline execution.
- `scripts/`: Python scripts for fetching, processing, and loading data.
- `tests/`: Unit tests for the pipeline.

## Requirements
- Python 3.x
- Install dependencies:

    ```bash
    pip install -r requirements.txt
    ```

## Content Overview

### Scripts
The `scripts/` folder contains Python scripts to:
- **Fetch Data**: Script to fetch repository data from the GitHub API (`fetch_data.py`).
- **Process Data**: Script to clean and process the fetched data (`process_data.py`).

### Data
The `data/` folder contains the following subdirectories:
- `data/raw/`: Stores raw CSV files fetched from the GitHub API.
- `data/processed/`: Stores cleaned and transformed data ready for analysis.

## Setup Instructions

### Clone the Repository

To get started, clone this repository to your local machine:

```bash
git clone https://github.com/jacob-cunningham-ds/top-github-repositories.git
cd top-github-repositories
```

### Set Up a GitHub Personal Access Token

This project requires a GitHub Personal Access Token to authenticate with the GitHub API.

1. **Log in to GitHub**:
    Go to [GitHub](https://www.github.com) and log in to your account

2. **Access Developer Settings**:
    Navigate to your account settings and click **Developer settings**.

3. **Generate a New Token**:
    - Click **Personal access tokens** > **Tokens (classic)** > **Generate new token (classic)**.
    - Set an appropriate expiration date (e.g., 30 days or more)
    - Under **Scopes** select **repo**.

4. **Copy the Token**:
    Once the token is generated, copy it immediately, as you won't be able to see it again.

### Store the Token as an Environment Variable

1. Open Command Prompt as Administrator.

2. Set the environment variable:c
   
    ```bash
    setx GITHUB_TOKEN "your_personal_access_token"
    ```

## Usage

1. **Fetch Data**:
    Run the `fetch_data.py` script to fetch GitHub repository data:
    
    ```bash
    python scripts/fetch_data.py
    ```

    Output: Raw data saved as a CSV file in `data/raw/`.

2. **Process Data**:
    Run the `process_data.py` script to clean and transform the data:

    ```bash
    python scripts/process_data.py
    ```

    Output: Processed data saved as a CSV file in `data/processed/`.

4. **Visualize in Tableau**: Connect Tableau (or equivalent) to the `github_repos_processed.csv` file to build dashboards and visualize the data.

## Limitations

- GitHub API Pagination:
    The script currently fetches up to 100 repositories per run due to API pagination limits.

- API Rate Limits:
    Ensure your GitHub Personal Access Token has sufficient quota to avoid rate-limiting issues.

- Language Detection:
    Some repositories may have `null` for the `language` field if the primary language is not detected.

