# Last.fm Top Artists Scraper

This script fetches the top artists from Last.fm, gets their top tags, and saves the data to a CSV file.

## Setup

1.  **Create a virtual environment:**

    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```

2.  **Install the dependencies:**

    ```bash
    pip install -r requirements.txt
    ```

3.  **Create a `.env` file:**

    Create a `.env` file in the root of the project and add your Last.fm API key and user agent:

    ```
    API_KEY=your_api_key
    USER_AGENT=your_user_agent
    ```

## Usage

To run the script, simply execute the `main.py` file:

```bash
python3 main.py
```

The script will create an `artists.csv` file in the root of the project with the top artists and their tags.
