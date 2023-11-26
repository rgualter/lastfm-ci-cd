
import pytest
import unittest
from unittest.mock import patch, MagicMock

import requests
from main import lastfm_get, lookup_tags



@pytest.mark.parametrize(
    "method",
    [
        ("chart.gettopartists"),
        ("chart.getTopTracks"),
        ("chart.getTopTags"),
    ]
)
def test_lastfm_get(method):
    payload = {"method": method, "limit": 500, "page": 1}
    response = lastfm_get(payload)
    assert response != None 
    #assert response.status_code == 200




