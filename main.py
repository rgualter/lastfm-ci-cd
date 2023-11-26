# %%
import requests, requests_cache, logging, time, dotenv, os, pandas as pd
from IPython.display import clear_output
from tqdm import tqdm

requests_cache.install_cache()

dotenv.load_dotenv(dotenv.find_dotenv())

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

#%%
def lastfm_get(payload):
    # get enviroment variables and define headers and URL
    headers = {"user-agent": os.getenv("USER_AGENT")}
    url = "https://ws.audioscrobbler.com/2.0/"
    payload["api_key"] = os.getenv("API_KEY")
    payload["format"] = "json"

    try:
        response = requests.get(url, headers=headers, params=payload)
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as e:
        logger.error(f"Error making LastFM request: {e}")
        return None

#%%
def lookup_tags(artist):
    response = lastfm_get({"method": "artist.getTopTags", "artist": artist})

    # if there's an error, just return nothing
    if response.status_code != 200:
        return None

    # extract the top three tags and turn them into a string
    tags = [t["name"] for t in response.json()["toptags"]["tag"][:3]]
    tags_str = ", ".join(tags)

    # rate limiting
    if not getattr(response, "from_cache", False):
        time.sleep(0.30)
    return tags_str

# %%

responses = []

page = 1
total_pages = 10  # this is just a dummy number to start the loop

while page <= total_pages:
    payload = {"method": "chart.gettopartists", "limit": 500, "page": page}

    # print some output so we can see the status
    print(f"Requesting page {page}/{total_pages}")
    # clear the output to make things neater
    clear_output(wait=True)

    # make the API call
    response = lastfm_get(payload)

    # if we get an error, print the response and halt the loop
    if response.status_code != 200:
        print(response.text)
        break

    # extract pagination info
    page = int(response.json()["artists"]["@attr"]["page"])
    total_pages = 1
    # total_pages = int(response.json()['artists']['@attr']['totalPages'])

    # append response
    responses.append(response)

    # if it's not a cached result, sleep
    if not getattr(response, "from_cache", False):
        time.sleep(0.30)

    # increment the page number
    page += 1

# %%
frames = [pd.DataFrame(r.json()["artists"]["artist"]) for r in responses]

artists = pd.concat(frames)

artists = artists.drop("image", axis=1)

artists = artists.drop_duplicates().reset_index(drop=True)

artists[["playcount", "listeners"]] = artists[["playcount", "listeners"]].astype(int)

artists = artists.sort_values("listeners", ascending=False)

artists.to_csv("artists.csv", index=False)

#%%
# progress_apply function is used to apply the lookup_tags function to each element (artist name) in the 'name' column.
tqdm.pandas()
artists["tags"] = artists["name"].progress_apply(lookup_tags)
#%%
artists.head(10)

# %%
