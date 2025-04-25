# %%
import os
import bs4
import uuid
import pandas as pd
from dotenv import load_dotenv
from imap_tools import MailBox, AND
from datetime import datetime, timedelta

# Environnement variables
load_dotenv(".env.local", override=True)

USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")

# Load previously saved data and set starting date accordingly
try:
    saved_news_data = pd.read_csv("alphasignal.csv")
    saved_mail_logs = pd.read_csv("mail_logs.csv")
    saved_topic_logs = pd.read_csv("topic_logs.csv")
    saved_news_logs = pd.read_csv("news_logs.csv")

    # If data available, start from the day after the last recording
    start_date = datetime.strptime(
        saved_news_data["date"].max()[:10], "%Y-%m-%d"
    ).date() + timedelta(days=1)
except:
    saved_news_data = None
    saved_mail_logs = None
    saved_topic_logs = None
    saved_news_logs = None

    # If no data available, default start date is my subscription date to AlphaSignal
    start_date = datetime.strptime("2024-12-01", "%Y-%m-%d").date()

# Initialize data structures to hold extracted data and logs
news_data = []
mail_logs = []
topic_logs = []
news_logs = []

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
        print(
            f"Processing email UID: {msg.uid} - Date: {datetime.strftime(msg.date, format="%Y-%m-%d %H:%M:%S")}"
        )
        this_mail_logs = {
            "uid": msg.uid,
            "date": msg.date,
            "not_a_news": True,
            "html_content_extracted": False,
            "html_all_news_located": False,
            "n_topics": 0,
            "status": "ok",
            "comment": "",
        }

        if "IN TODAY'S SIGNAL" in msg.html:
            this_mail_logs["not_a_news"] = False

            # Parse the raw HTML content
            try:
                soup = bs4.BeautifulSoup(msg.html, "html.parser")
                this_mail_logs["html_content_extracted"] = True
            except Exception as e:
                print(f"  - Error parsing HTML content: {e}")

            # Locate the table that contain the news
            try:
                all_news_table = soup.find("table").find_all("table")[15]
                this_mail_logs["html_all_news_located"] = True
            except Exception as e:
                print(f"  - Error locating all news table: {e}")

            try:
                # Get the topics
                topics = all_news_table.find_all("ul")
                this_mail_logs["n_topics"] = len(topics)

                for i, topic in enumerate(topics):
                    # Get the news related to the topic
                    news = topic.find_all("li")

                    # Create and save the topic logs
                    this_topic_logs = {
                        "uid": msg.uid,
                        "topic_id": i + 1,
                        "n_news": len(news),
                        "status": "ok",
                        "comment": "",
                    }

                    if this_topic_logs["n_news"] == 0:
                        this_topic_logs["status"] = "not handled"

                    topic_logs.append(this_topic_logs)

                    # Extract news items
                    for j, info in enumerate(news):
                        this_news_data = {
                            "id": str(uuid.uuid4()),
                            "uid": msg.uid,
                            "date": msg.date,
                            "news": "no news",
                            "link": "no link",
                        }
                        this_news_logs = {
                            "uid": msg.uid,
                            "topic_id": i + 1,
                            "news_id": j + 1,
                            "has_news": False,
                            "has_link": False,
                            "status": "ok",
                            "comment": "",
                        }

                        # Check for and extract news if available
                        if info.text != "":
                            this_news_data["news"] = info.text
                            this_news_logs["has_news"] = True

                            # Check for and extract link if available
                            link_tag = info.find("a")

                            if link_tag and link_tag.get("href") != "":
                                this_news_data["link"] = link_tag.get("href")
                                this_news_logs["has_link"] = True

                        if (this_news_logs["has_news"] == False) | (
                            this_news_logs["has_link"] == False
                        ):
                            this_news_logs["status"] = "not handled"

                        # Append news data and save news logs
                        news_data.append(this_news_data)
                        news_logs.append(this_news_logs)

            except Exception as e:
                print(f"  - Error extracting news items : {e}")

        # Save the email logs
        if (
            (this_mail_logs["not_a_news"] == True)
            | (this_mail_logs["html_content_extracted"] == False)
            | (this_mail_logs["html_all_news_located"] == False)
            | (this_mail_logs["n_topics"] == 0)
        ):
            this_mail_logs["status"] = "not handled"

        mail_logs.append(this_mail_logs)

# Convert collected data and logs to DataFrames
news_data = pd.DataFrame(news_data)
mail_logs = pd.DataFrame(mail_logs)
topic_logs = pd.DataFrame(topic_logs)
news_logs = pd.DataFrame(news_logs)

# Append new data to existing data if available
if saved_news_data is not None:
    news_data = pd.concat([saved_news_data, news_data], ignore_index=True)
    mail_logs = pd.concat([saved_mail_logs, mail_logs], ignore_index=True)
    topic_logs = pd.concat([saved_topic_logs, topic_logs], ignore_index=True)
    news_logs = pd.concat([saved_news_logs, news_logs], ignore_index=True)

# Save final results to CSV files
news_data.to_csv("news_data.csv", index=False)
mail_logs.to_csv("mail_logs.csv", index=False)
topic_logs.to_csv("topic_logs.csv", index=False)
news_logs.to_csv("news_logs.csv", index=False)
