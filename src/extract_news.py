# %%
import os
import re
import bs4
import uuid
import pandas as pd
from dotenv import load_dotenv
from imap_tools import MailBox, AND
from datetime import datetime, timedelta

# Environnement variables
load_dotenv("../.env.local", override=True)

USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")


# Function to clean strings by removing non-alphanumeric characters
def clean_string(text):
    # Remove all non-alphanumeric characters except spaces
    text = re.sub(r"[^a-zA-Z0-9 ]", "", text)

    # Replace multiple spaces with a single space and strip
    text = re.sub(r"\s+", " ", text).strip()

    return text


# Load previously saved data and set starting date accordingly
data_folder = os.path.join("../data")

try:
    saved_news_data = pd.read_csv(f"{data_folder}/news_data.csv")
    saved_mail_logs = pd.read_csv(f"{data_folder}/mail_logs.csv")
    saved_topic_logs = pd.read_csv(f"{data_folder}/topic_logs.csv")
    saved_news_logs = pd.read_csv(f"{data_folder}/news_logs.csv")

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
with MailBox("imap.gmail.com").login(USERNAME, PASSWORD) as mailbox:
    print("Extracting emails...")
    for msg in mailbox.fetch(
        AND(
            from_="news@alphasignal.ai",
            date_gte=start_date,
        ),
        mark_seen=False,
    ):
        print(
            f"- UID: {msg.uid}",
            f"- Date: {datetime.strftime(msg.date, format="%Y-%m-%d %H:%M:%S")}",
        )
        this_mail_logs = {
            "uid": msg.uid,
            "date": msg.date,
            "not_a_news": True,
            "html_content_extracted": False,
            "html_news_located": False,
            "n_topics": 0,
            "status": "ok",
            "comment": "",
        }

        if "IN TODAY'S SIGNAL" in msg.html:
            this_mail_logs["not_a_news"] = False

            # Parse the raw HTML content
            try:
                soup = bs4.BeautifulSoup(msg.html, "html.parser")
                for tag in soup.find_all(True):
                    if tag.name not in [
                        "a",
                        "body",
                        "html",
                        "li",
                        "p",
                        "table",
                        "td",
                        "tr",
                        "ul",
                    ]:
                        tag.unwrap()
                    if tag.name != "a":
                        tag.attrs = {}
                    else:
                        tag.attrs = {
                            k: v for k, v in tag.attrs.items() if k in ["href"]
                        }
                this_mail_logs["html_content_extracted"] = True
            except Exception as e:
                print(f"  - Error parsing HTML content: {e}")

            # Locate the table that contain the news
            try:
                all_news_tables = soup.find("td").find_all("table", recursive=False)
                all_news_tables = [
                    table for table in all_news_tables if table.text.strip() != ""
                ]

                header_news_cell = all_news_tables[2].find_all("td")[-1]

                news_tables = all_news_tables[4:-4]

                this_mail_logs["html_news_located"] = True

            except Exception as e:
                print(f"  - Error locating all news table: {e}")

            try:
                # Get the topics
                topics = header_news_cell.find_all("p", recursive=False)
                topics = [
                    clean_string(tn.text) for tn in topics if tn.text.strip() != ""
                ]

                topic_titles_list = header_news_cell.find_all("ul", recursive=False)
                topic_titles_list = [
                    tc for tc in topic_titles_list if tc.text.strip() != ""
                ]

                this_mail_logs["n_topics"] = len(topics)

                for i, (topic, topic_titles, news_table) in enumerate(
                    zip(topics, topic_titles_list, news_tables)
                ):
                    # Get the news related to the topic
                    titles = topic_titles.find_all("li")
                    titles = [nt for nt in titles if nt.text.strip() != ""]

                    # Create and save the topic logs
                    this_topic_logs = {
                        "uid": msg.uid,
                        "topic": topic,
                        "n_news": len(titles),
                        "status": "ok",
                        "comment": "",
                    }

                    contents = []
                    links = [
                        title.find("a").get("href") if title.find("a") else ""
                        for title in titles
                    ]
                    tags = []

                    if this_topic_logs["n_news"] == 0:
                        this_topic_logs["status"] = "not handled"

                    elif this_topic_logs["n_news"] == 1:
                        link = (
                            news_table.find_all("a")[-1].get("href")
                            if news_table.find("a")
                            else ""
                        )

                        if links[0] == "":
                            links[0] = link

                        for a in news_table.find_all("a"):
                            a.decompose()

                        for li in news_table.find_all("li"):
                            text = li.text
                            li.clear()
                            li.append(f"- {text}")

                        if topic in [
                            "Top News",
                            "Top Lecture",
                            "How To",
                            "Top of YouTube",
                        ]:
                            news = news_table.get_text(separator="\n", strip=True)
                            news = re.sub(r"\n{2,}", "\n", news)

                            news = news.splitlines()[1:]
                            tags = [news[0]]
                            contents = [
                                "\n".join(
                                    [n for i, n in enumerate(news) if i not in [0, 2]]
                                )
                            ]
                        else:
                            news = news_table.get_text(separator="\n", strip=True)
                            news = re.sub(r"\n{2,}", "\n", news)

                            tags = [titles[0].text]
                            contents = [news]
                    else:
                        all_news = news_table.find_all("tr")[2].find_all("tr")
                        # break
                        all_news = [
                            (
                                re.sub(
                                    r"\n{2,}",
                                    "\n",
                                    news.get_text(separator="\n", strip=True),
                                )
                            )
                            for news in all_news
                        ]

                        all_news = "\n".join(all_news[1:])
                        all_news = [
                            news.split("\n")
                            for news in re.split(r"\n{2,}", all_news)
                            if len(news.split("\n")) >= 3
                        ]

                        for news in all_news:
                            tags.append(news[0])
                            contents.append(
                                "\n".join(
                                    [n for i, n in enumerate(news) if i not in [0, 2]]
                                )
                            )

                    topic_logs.append(this_topic_logs)

                    # Extract news items
                    for title, content, link, tag in zip(titles, contents, links, tags):
                        news_id = str(uuid.uuid4())

                        this_news_data = {
                            "id": news_id,
                            "uid": msg.uid,
                            "date": msg.date,
                            "topic": topic,
                            "title": title.text,
                            "content": content,
                            "link": link,
                            "tag": tag,
                        }

                        this_news_logs = {
                            "id": news_id,
                            "uid": msg.uid,
                            "date": msg.date,
                            "topic": topic,
                            "title": title,
                            "has_link": True,
                            "status": "ok",
                            "comment": "",
                        }

                        if this_news_data["link"] == "":
                            this_news_logs["has_link"] = False
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
            | (this_mail_logs["html_news_located"] == False)
            | (this_mail_logs["n_topics"] == 0)
        ):
            this_mail_logs["status"] = "not handled"

        mail_logs.append(this_mail_logs)

    print("Emails extracted.")

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

# Mark duplicates in news logs
mask = (news_data["title"] != "no title") & news_data["title"].duplicated()
news_logs.loc[mask, "status"] = "duplicated news headline"

# Save final results to CSV files
news_data.to_csv(f"{data_folder}/news_data.csv", index=False)
mail_logs.to_csv(f"{data_folder}/mail_logs.csv", index=False)
topic_logs.to_csv(f"{data_folder}/topic_logs.csv", index=False)
news_logs.to_csv(f"{data_folder}/news_logs.csv", index=False)
