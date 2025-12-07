import logging
import os
import time
from typing import List, Dict, Any

import pandas as pd
import requests
import requests_cache
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Install cache for requests
requests_cache.install_cache()

# Load environment variables
load_dotenv()


def lastfm_get(payload: Dict[str, Any]) -> requests.Response | None:
    """
    Makes a GET request to the Last.fm API.

    Args:
        payload: The parameters for the API request.

    Returns:
        The response from the API, or None if an error occurred.
    """
    headers = {"user-agent": os.getenv("USER_AGENT")}
    url = "https://ws.audioscrobbler.com/2.0/"
    payload["api_key"] = os.getenv("API_KEY")
    payload["format"] = "json"

    try:
        response = requests.get(url, headers=headers, params=payload)
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as e:
        logging.error(f"Error making LastFM request: {e}")
        return None


def get_top_artists(limit: int = 500) -> List[Dict[str, Any]]:
    """
    Fetches the top artists from the Last.fm API.

    Args:
        limit: The number of artists to fetch per page.

    Returns:
        A list of artist data.
    """
    page = 1
    total_pages = 1
    all_artists = []

    while page <= total_pages:
        logging.info(f"Requesting page {page}/{total_pages or '...'}")
        payload = {"method": "chart.gettopartists", "limit": limit, "page": page}
        response = lastfm_get(payload)

        if not response or response.status_code != 200:
            logging.error(f"Failed to fetch data: {response.text if response else 'No response'}")
            break

        data = response.json()
        all_artists.extend(data["artists"]["artist"])

        if page == 1:
            total_pages = int(data['artists']['@attr']['totalPages'])

        if not getattr(response, "from_cache", False):
            time.sleep(0.30)

        page += 1

    return all_artists


def process_artist_data(artists: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Processes the raw artist data into a pandas DataFrame.

    Args:
        artists: A list of artist data.

    Returns:
        A DataFrame with the processed artist data.
    """
    df = pd.DataFrame(artists)
    df = df.drop("image", axis=1)
    df = df.drop_duplicates().reset_index(drop=True)
    df[["playcount", "listeners"]] = df[["playcount", "listeners"]].astype(int)
    return df.sort_values("listeners", ascending=False)


def lookup_tags(artist: str) -> str | None:
    """
    Looks up the top tags for an artist.

    Args:
        artist: The name of the artist.

    Returns:
        A string with the top three tags, or None if an error occurred.
    """
    response = lastfm_get({"method": "artist.getTopTags", "artist": artist})

    if not response or response.status_code != 200:
        return None

    try:
        tags = [t["name"] for t in response.json()["toptags"]["tag"][:3]]
        return ", ".join(tags)
    except KeyError:
        return None
    finally:
        if response and not getattr(response, "from_cache", False):
            time.sleep(0.30)


def main():
    """
    Main function to fetch, process, and save artist data.
    """
    top_artists = get_top_artists()
    if not top_artists:
        logging.info("No artist data fetched. Exiting.")
        return

    artists_df = process_artist_data(top_artists)

    logging.info("Looking up tags for artists...")
    artists_df["tags"] = artists_df["name"].apply(lookup_tags)

    artists_df.to_csv("artists.csv", index=False)
    logging.info("Artist data saved to artists.csv")
    print(artists_df.head(10))


if __name__ == "__main__":
    main()
