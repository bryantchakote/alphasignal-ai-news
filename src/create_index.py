# %%
import os
import time
import pandas as pd
from tqdm import tqdm
from dotenv import load_dotenv
from pinecone import Pinecone, ServerlessSpec


# Environnement variables
load_dotenv("../.env.local", override=True)

PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")

# Initialize Pinecone client
pc = Pinecone(api_key=PINECONE_API_KEY)

# Create a serverless index
index_name = "alphasignal-ai-news-index"

pc.create_index(
    name=index_name,
    dimension=1024,  # Replace with your model dimensions
    metric="cosine",  # Replace with your model metric
    spec=ServerlessSpec(cloud="aws", region="us-east-1"),
)

# Create vector embeddings and upsert them into the index
data = pd.read_csv("news_data.csv")
embeddings = []
batch_size = 96
index = pc.Index(index_name)

for i in tqdm(range(0, len(data), batch_size)):
    data_batch = data.iloc[i : i + batch_size, :]

    embeddings = pc.inference.embed(
        model="multilingual-e5-large",
        inputs=data_batch["news"].to_list(),
        parameters={"input_type": "passage", "truncate": "END"},
    )

    # Wait for the index to be ready
    while not pc.describe_index(index_name).status["ready"]:
        time.sleep(1)

    vectors = []
    for (_, d), e in zip(data_batch.iterrows(), embeddings):
        vectors.append(
            {
                "id": str(d["id"]),
                "values": e["values"],
                "metadata": {k: d[k] for k in ["date", "news", "link"]},
            }
        )

    index.upsert(vectors=vectors, namespace="ns1")
