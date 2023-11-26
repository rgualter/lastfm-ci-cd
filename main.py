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

def get_top_artists_data():
    responses = []
    page = 1
    total_pages=10 # dummy variable to start 
    while page <= total_pages:
        payload = {"method": "chart.gettopartists", "limit": 500, "page": page}

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


#%%
# Criar DataFrames a partir das respostas

responses = get_top_artists_data()

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
