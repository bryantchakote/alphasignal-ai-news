# %%
import os
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime, timedelta
from imap_tools import MailBox, AND
import bs4


# Environnement variables
load_dotenv(".env.local", override=True)

USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")


def extract_news(raw_content):
    # Store logging info
    logs = {}

    # Parse the raw HTML content
    try:
        soup = bs4.BeautifulSoup(raw_content, "html.parser")
    except:
        print("Error parsing HTML content")
        logs["content_extracted"] = False
        return None

    # Store extracted news and links
    news_and_links = {
        "news": [],
        "link": [],
    }

    # Locate the table taht contain the news
    try:
        news = soup.find("table").find_all("table")[15]
    except:
        print("Error locating news table")
        logs["news_located"] = False
        return None

    # Get the topics
    topics = news.find_all("ul")
    logs["n_topics"] = len(topics)
    logs["n_news"] = []

    # Extract news items
    for i, topic in enumerate(topics):
        news = topic.find_all("li")
        logs["n_news"].append(len(news))

        for info in news:
            if info.text != "":
                # Append news text
                news_and_links["news"].append(info.text)

                # Check for and extract link if available
                link_tag = info.find("a")
                if link_tag:
                    news_and_links["link"].append(link_tag.get("href"))

                else:
                    news_and_links["link"].append("")

    # Return extracted news and links
    return {"news_and_links": news_and_links, "logs": logs}


# Initialize data structures to hold extracted data and logs
data = {
    "uid": [],
    "date": [],
    "news": [],
    "link": [],
}
data_logs = {
    "uid": [],
    "date": [],
    "content_extracted": [],
    "news_located": [],
    "n_topics": [],
    "n_news": [],
    "not_a_news": [],
}


# Load previously saved data and set starting date accordingly
try:
    saved_data = pd.read_csv("alphasignal.csv")
    saved_logs = pd.read_csv("alphasignal_logs.csv")

    # If data available, start from the day after the last recording
    start_date = datetime.strptime(
        saved_data["date"].max()[:10], "%Y-%m-%d"
    ).date() + timedelta(days=1)
except:
    saved_data = None
    saved_logs = None

    # If no data available, default start date is my subscription date to AlphaSignal
    start_date = datetime.strptime("2024-12-01", "%Y-%m-%d").date()

# Connect to Gmail inbox and fetch relevant emails
with MailBox("imap.gmail.com").login(
    USERNAME, PASSWORD, initial_folder="INBOX"
) as mailbox:
    for msg in mailbox.fetch(
        AND(
            from_="news@alphasignal.ai",
            date_gte=start_date,
        ),
        mark_seen=False,
    ):
        # Extract email id and date
        data_logs["uid"].append(msg.uid)
        data_logs["date"].append(msg.date)

        # Process emails that contain the newsletter
        if "IN TODAY'S SIGNAL" in msg.html:
            data_logs["not_a_news"].append(None)

            try:
                # Extract news from email content
                res = extract_news(raw_content=msg.html)
                news_and_links = res["news_and_links"]

                # Record log information
                logs = res["logs"]
                data_logs["content_extracted"].append(logs.get("content_extracted"))
                data_logs["news_located"].append(logs.get("news_located"))
                data_logs["n_topics"].append(logs.get("n_topics"))
                data_logs["n_news"].append(logs.get("n_news"))

                # Store each news item with its link
                for i in range(len(news_and_links["news"])):
                    data["uid"].append(msg.uid)
                    data["date"].append(msg.date)
                    data["news"].append(news_and_links["news"][i])
                    data["link"].append(news_and_links["link"][i])

            except Exception as e:
                print(f"Error processing message UID {msg.uid}: {e}")
        else:
            # If the email doesn't is irrelevant
            data_logs["content_extracted"].append(None)
            data_logs["news_located"].append(None)
            data_logs["n_topics"].append(None)
            data_logs["n_news"].append(None)
            data_logs["not_a_news"].append(True)


# Convert collected data and logs to DataFrames
data = pd.DataFrame(data)
data_logs = pd.DataFrame(data_logs)

# Mark rows as "not handled" if any of the following conditions are True:
data_logs["handled"] = None
data_logs.loc[
    (
        # - HTML content couldn't be extracted
        (data_logs["content_extracted"] == False)
        |
        # - News table wasn't located
        (data_logs["news_located"] == False)
        |
        # - No topics were found (n_topics == 0)
        (data_logs["n_topics"] == 0)
        |
        # - No news items were found for a given topic
        (data_logs["n_news"].apply(lambda x: 0 in x if isinstance(x, list) else False))
        |
        # - Email was identified as not containing actual news
        (data_logs["not_a_news"] == True)
    ),
    "handled",
] = False

# Append new data to existing data if available
if saved_data is not None:
    data = pd.concat([saved_data, data], ignore_index=True)

if saved_logs is not None:
    logs = pd.concat([saved_logs, logs], ignore_index=True)

# Save final results to CSV files
data.to_csv("alphasignal.csv", index=False)
data_logs.to_csv("alphasignal_logs.csv", index=False)
