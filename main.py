# %%
import os
import time
import logging
import dotenv
import requests
import requests_cache
import datetime
import pandas as pd
from IPython.display import clear_output
from tqdm import tqdm
import boto3
from io import StringIO

requests_cache.install_cache()

dotenv.load_dotenv(dotenv.find_dotenv())

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


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


def get_responses(method, limit):
    responses = []
    page = 1
    total_pages = 10

    while page <= total_pages:
        payload = {"method": method, "limit": limit, "page": page}

        # Imprimir alguma saída para visualizar o status
        print(f"Requesting page {page}/{total_pages}")
        # Limpar a saída para organizar as coisas
        clear_output(wait=True)

        # Fazer a chamada da API
        response = lastfm_get(payload)

        # Se ocorrer um erro, imprimir a resposta e interromper o loop
        if response is None or response.status_code != 200:
            print("Error in API response.")
            break

        # Extrair informações de paginação
        page = int(response.json()["artists"]["@attr"]["page"])
        total_pages = 1
        # total_pages = int(response.json()['artists']['@attr']['totalPages'])

        # Anexar a resposta
        responses.append(response)

        # Se não for um resultado em cache, aguardar um curto período
        if not getattr(response, "from_cache", False):
            time.sleep(0.30)

        # Incrementar o número da página
        page += 1

    return responses


def create_artists_dataframe(responses):
    frames = [pd.DataFrame(r.json()["artists"]["artist"]) for r in responses]
    return pd.concat(frames)


def process_artists_dataframe(artists):
    artists = artists.drop("image", axis=1)
    artists = artists.drop_duplicates().reset_index(drop=True)
    artists[["playcount", "listeners"]] = artists[["playcount", "listeners"]].astype(
        int
    )
    artists = artists.sort_values("listeners", ascending=False)
    tqdm.pandas()
    artists["tags"] = artists["name"].progress_apply(
        lookup_tags
    )  # progress_apply function is used to apply the lookup_tags function to each element (artist name) in the 'name' column.
    return artists


def save_artists_to_csv(artists, filename):
    artists.to_csv(filename, index=False)


def save_artists_to_s3(artists, bucket_name, filename):
    s3 = boto3.client("s3")
    csv_buffer = StringIO()
    artists.to_csv(csv_buffer, index=False)
    csv_data = csv_buffer.getvalue()
    s3.put_object(Body=csv_data, Bucket=bucket_name, Key=filename)


responses = get_responses(method="chart.gettopartists", limit=500)
artists = create_artists_dataframe(responses)
artists = process_artists_dataframe(artists)

save_artists_to_s3(
    artists,
    bucket_name="lastfm-raw",
    filename=f"Artists/extracted_at={datetime.datetime.now().date()}/Artists_{datetime.datetime.now()}.csv",
)

save_artists_to_csv(
    artists, "/home/ricardo/Documentos/Desenvolvimento/LastfmAPI/artists.csv"
)
artists.head(10)
