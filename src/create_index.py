# %%
import os
import time
import pandas as pd
from tqdm import tqdm
from dotenv import load_dotenv
import chromadb

# Environnement variables
load_dotenv("../.env.local", override=True)

# Data folder
data_folder = os.path.join("../data")

# Create a Chroma client
chroma_client = chromadb.PersistentClient(path=f"{data_folder}/chroma_db")

# Create or get a collection
collection = chroma_client.get_or_create_collection(
    name="alphasignal-ai-news-index",
    configuration={
        "hnsw": {
            "space": "cosine",
        },
    },
)

# Load documents
news_data = pd.read_csv(f"{data_folder}/news_data.csv")
news_logs = pd.read_csv(f"{data_folder}/news_logs.csv")

# Filter documents
mask = news_logs["status"] == "ok"
news_data = news_data[mask]

# Create embeddings
documents = []
metadatas = []
ids = []

print("Embedding news...")
for _, news in news_data.iterrows():
    print(
        f"- ID (start): {news["id"].split("-")[0]}",
        f"- UID: {news["uid"]}",
        f" - Date: {news["date"][:19]}",
    )
    documents.append(news["news"])
    metadatas.append(
        {
            "date": news["date"],
            "link": news["link"],
        }
    )
    ids.append(str(news["id"]))

print("News embedded.")

# Add documents to the collection
collection.add(
    documents=documents,
    metadatas=metadatas,
    ids=ids,
)
