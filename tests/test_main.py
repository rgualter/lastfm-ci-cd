import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import requests
from main import (
    lastfm_get,
    get_top_artists,
    process_artist_data,
    lookup_tags,
)

class TestLastFm(unittest.TestCase):
    @patch("main.requests.get")
    def test_lastfm_get_success(self, mock_get):
        """Test that lastfm_get returns a response object on success."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_get.return_value = mock_response

        response = lastfm_get({"method": "chart.gettopartists"})
        self.assertIsNotNone(response)
        self.assertEqual(response.status_code, 200)

    @patch("main.requests.get")
    def test_lastfm_get_failure(self, mock_get):
        """Test that lastfm_get returns None on failure."""
        mock_get.side_effect = requests.exceptions.RequestException("API error")

        response = lastfm_get({"method": "chart.gettopartists"})
        self.assertIsNone(response)

    @patch("main.lastfm_get")
    def test_get_top_artists(self, mock_lastfm_get):
        """Test that get_top_artists returns a list of artists."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "artists": {
                "artist": [{"name": "Artist 1"}, {"name": "Artist 2"}],
                "@attr": {"totalPages": "1"},
            }
        }
        mock_lastfm_get.return_value = mock_response

        artists = get_top_artists()
        self.assertEqual(len(artists), 2)
        self.assertEqual(artists[0]["name"], "Artist 1")

    def test_process_artist_data(self):
        """Test that process_artist_data correctly processes a list of artists."""
        artists = [
            {"name": "Artist 1", "playcount": "100", "listeners": "50", "image": "url1"},
            {"name": "Artist 2", "playcount": "200", "listeners": "75", "image": "url2"},
        ]
        df = process_artist_data(artists)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 2)
        self.assertEqual(df.iloc[0]["name"], "Artist 2")  # Sorted by listeners

    @patch("main.lastfm_get")
    def test_lookup_tags(self, mock_lastfm_get):
        """Test that lookup_tags returns a string of tags."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "toptags": {"tag": [{"name": "tag1"}, {"name": "tag2"}]}
        }
        mock_lastfm_get.return_value = mock_response

        tags = lookup_tags("some_artist")
        self.assertEqual(tags, "tag1, tag2")

if __name__ == "__main__":
    unittest.main()
