import logging, os, time, hashlib, requests, azure.functions as func
from azure.cosmos import CosmosClient
from azure.storage.blob import BlobServiceClient
from bs4 import BeautifulSoup

app = func.FunctionApp()

@app.schedule(schedule="0 0 5 * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False) 
def legislative_scraper(myTimer: func.TimerRequest) -> None:
    logging.info('🚀 Legislative Scraper started.')
    
    # Env Vars
    COSMOS_ENDPOINT = os.environ.get("COSMOS_ENDPOINT")
    COSMOS_KEY = os.environ.get("COSMOS_KEY")
    COSMOS_DB = os.environ.get("COSMOS_DATABASE_NAME", "LegislativeDB")
    COSMOS_CONTAINER = os.environ.get("COSMOS_CONTAINER_NAME", "Metadata")
    BLOB_CONN = os.environ.get("BLOB_CONNECTION_STRING")
    BLOB_CONTAINER = os.environ.get("BLOB_CONTAINER_NAME", "legislation-text")

    # Clients
    cosmos_client = CosmosClient(COSMOS_ENDPOINT, credential=COSMOS_KEY)
    container = cosmos_client.get_database_client(COSMOS_DB).get_container_client(COSMOS_CONTAINER)
    blob_container = BlobServiceClient.from_connection_string(BLOB_CONN).get_container_client(BLOB_CONTAINER)
    if not blob_container.exists(): blob_container.create_container()

    themes = [
        "https://www.europarl.europa.eu/legislative-train/theme-a-european-green-deal",
        "https://www.europarl.europa.eu/legislative-train/theme-a-europe-fit-for-the-digital-age",
        "https://www.europarl.europa.eu/legislative-train/theme-an-economy-that-works-for-people",
        "https://www.europarl.europa.eu/legislative-train/theme-a-stronger-europe-in-the-world",
        "https://www.europarl.europa.eu/legislative-train/theme-promoting-our-european-way-of-life",
        "https://www.europarl.europa.eu/legislative-train/theme-a-new-push-for-european-democracy",
    ]
    base_url = "https://www.europarl.europa.eu"

    for theme in themes:
        try:
            r = requests.get(theme, headers={"User-Agent": "Mozilla/5.0"})
            soup = BeautifulSoup(r.text, "html.parser")
            links = [base_url + a['href'] for a in soup.select("a[href*='/file-']") if a['href'].startswith("/legislative-train/theme-")]
            
            for link in set(links):
                try:
                    f_r = requests.get(link, headers={"User-Agent": "Mozilla/5.0"})
                    f_soup = BeautifulSoup(f_r.text, "html.parser")
                    
                    title_el = f_soup.find("h1")
                    title = title_el.get_text(strip=True) if title_el else "Untitled"
                    
                    content_div = f_soup.select_one(".content") or f_soup
                    text = content_div.get_text("\n", strip=True)
                    
                    doc_id = hashlib.md5(link.encode('utf-8')).hexdigest()
                    blob_name = f"{doc_id}.txt"
                    
                    # Upload
                    blob_container.upload_blob(name=blob_name, data=text, overwrite=True)
                    container.upsert_item({
                        "id": doc_id,
                        "url": link,
                        "title": title,
                        "theme": theme,
                        "blob_url": f"{blob_container.url}/{blob_name}",
                        "scraped_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                    })
                    logging.info(f"Saved: {title[:30]}")
                    time.sleep(0.5)
                except Exception as e:
                    logging.error(f"Error on file {link}: {e}")
        except Exception as e:
            logging.error(f"Error on theme {theme}: {e}")
