import logging
import os
import time
import re
import hashlib
import json
import azure.functions as func
from azure.cosmos import CosmosClient, PartitionKey
from azure.storage.blob import BlobServiceClient
import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
# These should be set in your Azure Function Configuration (Environment Variables)
COSMOS_ENDPOINT = os.environ.get("COSMOS_ENDPOINT")
COSMOS_KEY = os.environ.get("COSMOS_KEY")
COSMOS_DATABASE_NAME = os.environ.get("COSMOS_DATABASE_NAME", "LegislativeDB")
COSMOS_CONTAINER_NAME = os.environ.get("COSMOS_CONTAINER_NAME", "Metadata")

BLOB_CONNECTION_STRING = os.environ.get("BLOB_CONNECTION_STRING")
BLOB_CONTAINER_NAME = os.environ.get("BLOB_CONTAINER_NAME", "scraped-text")

BASE_URL = "https://www.europarl.europa.eu"
ROOT_URL = f"{BASE_URL}/legislative-train"

THEMES = [
    "https://www.europarl.europa.eu/legislative-train/theme-a-european-green-deal",
    "https://www.europarl.europa.eu/legislative-train/theme-a-europe-fit-for-the-digital-age",
    "https://www.europarl.europa.eu/legislative-train/theme-an-economy-that-works-for-people",
    "https://www.europarl.europa.eu/legislative-train/theme-a-stronger-europe-in-the-world",
    "https://www.europarl.europa.eu/legislative-train/theme-promoting-our-european-way-of-life",
    "https://www.europarl.europa.eu/legislative-train/theme-a-new-push-for-european-democracy",
]

app = func.FunctionApp()

# ---------------------------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------------------------

def get_soup(url):
    """Fetches URL and returns BeautifulSoup object."""
    time.sleep(0.5) # Polite delay
    try:
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0 AzureFunction/1.0"})
        if r.status_code != 200:
            logging.warning(f"⚠️ Failed to fetch {url} (status {r.status_code})")
            return None
        return BeautifulSoup(r.text, "html.parser")
    except Exception as e:
        logging.error(f"Error fetching {url}: {e}")
        return None

def get_files_from_theme(theme_url):
    """Extracts individual file links from a theme page."""
    soup = get_soup(theme_url)
    if not soup:
        return []
    file_links = []
    for link in soup.select("a[href*='/file-']"):
        href = link.get("href")
        if href and href.startswith("/legislative-train/theme-"):
            full_link = BASE_URL + href
            file_links.append(full_link)
    return list(set(file_links))

def generate_id(url):
    """Generates a deterministic ID based on the URL."""
    return hashlib.md5(url.encode('utf-8')).hexdigest()

# ---------------------------------------------------------------------------
# MAIN FUNCTION
# ---------------------------------------------------------------------------

@app.schedule(schedule="0 0 5 * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def legislative_scraper(myTimer: func.TimerRequest) -> None:
    logging.info('🚀 Legislative Train Scraper started.')

    # 1. Initialize Azure Clients
    try:
        # Cosmos Client
        cosmos_client = CosmosClient(COSMOS_ENDPOINT, credential=COSMOS_KEY)
        database = cosmos_client.get_database_client(COSMOS_DATABASE_NAME)
        container = database.get_container_client(COSMOS_CONTAINER_NAME)

        # Blob Client
        blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
        blob_container_client = blob_service_client.get_container_client(BLOB_CONTAINER_NAME)
        
        # Ensure blob container exists (optional, good for first run)
        if not blob_container_client.exists():
            blob_container_client.create_container()

    except Exception as e:
        logging.error(f"❌ Failed to initialize Azure Clients: {e}")
        return

    # 2. Start Scraping
    total_processed = 0
    
    for theme in THEMES:
        logging.info(f"📂 Crawling theme: {theme}")
        file_urls = get_files_from_theme(theme)
        
        for file_url in file_urls:
            try:
                soup = get_soup(file_url)
                if not soup:
                    continue

                # --- Extract Data ---
                title_el = soup.select_one("h1")
                title = title_el.text.strip() if title_el else "Untitled"

                oeil_link = None
                for a in soup.find_all("a", href=True):
                    if "oeil.secure.europarl.europa.eu" in a["href"]:
                        oeil_link = a["href"]
                        break

                status_el = soup.find("div", class_="train-status")
                status = status_el.text.strip() if status_el else "Unknown"

                content_blocks = soup.select("section, div.text-block, p")
                text_content = "\n".join(
                    [c.get_text(separator=" ", strip=True) for c in content_blocks if c.get_text(strip=True)]
                )

                # --- Prepare Identifiers ---
                doc_id = generate_id(file_url)
                safe_filename = re.sub(r"[^a-zA-Z0-9_-]+", "_", title)[:100] + f"_{doc_id[:6]}.txt"

                # --- 3. Upload Text to Blob Storage ---
                blob_client = blob_container_client.get_blob_client(safe_filename)
                blob_client.upload_blob(text_content, overwrite=True)
                blob_url = blob_client.url

                # --- 4. Upsert Metadata to Cosmos DB ---
                metadata_item = {
                    "id": doc_id,
                    "title": title,
                    "original_url": file_url,
                    "oeil_link": oeil_link,
                    "status": status,
                    "blob_storage_url": blob_url,
                    "blob_filename": safe_filename,
                    "theme_source": theme,
                    "scraped_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                }

                # Upsert (Create or Replace)
                container.upsert_item(metadata_item)
                
                total_processed += 1
                logging.info(f"✅ Processed: {title[:30]}...")

            except Exception as e:
                logging.error(f"❌ Error processing {file_url}: {e}")
                continue

    logging.info(f"🏁 Scraper finished. Total items processed: {total_processed}")