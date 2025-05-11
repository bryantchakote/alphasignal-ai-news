# %%
import re
import bs4
import uuid
from bs4.element import Tag
from imap_tools import MailMessage
from utils import (
    clean_string,
    OK,
    KO,
    SECTION_TITLE_FIX,
)


class Email:
    def __init__(self, msg: MailMessage):
        self.msg = msg

        self.soup = None

        self.header = None
        self.all_articles = None

        self.sections = []

        self.article_data = []

        self.email_logs = [
            {
                "email_uid": self.msg.uid,
                "email_date": self.msg.date,
                "email_contains_news": "IN TODAY'S SIGNAL" in self.msg.html,
                "n_sections": 0,
                "status": KO,
                "comment": "",
            }
        ]
        self.section_logs = []
        self.article_logs = []

    def extract_html_content(self):
        self.soup = bs4.BeautifulSoup(self.msg.html, "html.parser")

        for tag in self.soup.find_all(True):
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
                tag.attrs = {k: v for k, v in tag.attrs.items() if k in ["href"]}

    def locate_news_tags(self):
        news_tags = self.soup.find("td").find_all("table", recursive=False)
        news_tags = [tag for tag in news_tags if tag.text.strip() != ""]

        self.header = news_tags[2].find_all("td")[-1]
        self.all_articles = news_tags[4:-4]

    def extract_section_info_from_header(self):
        header_tags = [tag for tag in self.header.children if isinstance(tag, Tag)]

        section_titles = [tag for tag in header_tags if tag.name == "p"]

        if self.msg.uid in list(SECTION_TITLE_FIX["email_uid"]):
            email_uid = self.msg.uid
            email_uid
            row = SECTION_TITLE_FIX.query("email_uid == @email_uid")
            insert_pos = list(row["insert_pos"])[0]
            section_title = list(row["section_title"])[0]
            section_titles[insert_pos].append(section_title)

        section_titles = [
            clean_string(tag.text) for tag in section_titles if tag.text.strip() != ""
        ]

        section_infos = [tag for tag in header_tags if tag.name == "ul"]

        assert len(section_titles) == len(section_infos)
        assert len(section_titles) == len(self.all_articles)

        for title, infos in zip(section_titles, section_infos):
            items = infos.find_all("li")
            n_articles = len(items)
            print(n_articles, "-", title)
            article_titles = [tag.text for tag in items]
            article_links = [
                tag.find("a").get("href") if tag.find("a") else "" for tag in items
            ]

            section = {
                "section_title": title,
                "article_titles": article_titles,
                "article_links": article_links,
            }
            self.sections.append(section)

            section_log = {
                "email_uid": self.msg.uid,
                "section_title": title,
                "n_articles": n_articles,
                "status": KO if n_articles == 0 else OK,
                "comment": "",
            }
            self.section_logs.append(section_log)

        self.email_logs[0]["n_sections"] = len(self.sections)
        self.email_logs[0]["status"] = OK

    def extract_news_one_article(self, section, article):
        if (section["article_links"][0] == "") & (article.find("a") is not None):
            link = article.find_all("a")[-1].get("href")
            if link != "":
                section["article_links"][0] = link

        link = section["article_links"][0]

        news_id = str(uuid.uuid4())

        news = {
            "id": news_id,
            "email_uid": self.msg.uid,
            "email_date": self.msg.date,
            "article_section": section["section_title"],
            "article_title": section["article_titles"][0],
            "article_link": link,
        }

        news_logs = {
            "id": news_id,
            "email_uid": self.msg.uid,
            "email_date": self.msg.date,
            "article_section": section["section_title"],
            "article_title": section["article_titles"][0],
            "article_has_link": link != "",
            "article_title_equals_tag": False,
            "status": OK if link != "" else KO,
            "comment": "",
        }

        for tag in article.find_all("a"):
            tag.decompose()

        for tag in article.find_all("li"):
            text = tag.text
            tag.clear()
            tag.append(f"- {text}")

        if section["section_title"] in [
            "Top News",
            "Top Lecture",
            "How To",
            "Top of YouTube",
        ]:
            article_text = article.get_text(separator="\n", strip=True)
            article_text = re.sub(r"\n{2,}", "\n", article_text)
            article_lines = article_text.splitlines()[1:]

            news["article_tag"] = article_lines[0]
            news["article_content"] = "\n".join(
                [line for i, line in enumerate(article_lines) if i not in [0, 2]]
            )

        else:
            article_text = article.get_text(separator="\n", strip=True)
            article_text = re.sub(r"\n{2,}", "\n", article_text)

            news["article_tag"] = section["article_titles"][0]
            news["article_content"] = article_text

            news_logs["article_title_equals_tag"] = True
            news_logs["status"] = KO

        self.article_data.append(news)
        self.article_logs.append(news_logs)

    def extract_news_many_articles(self, section, articles):
        section_articles = articles.find_all("tr")[2].find_all("tr")
        section_articles = [
            re.sub(r"\n{2,}", "\n", articles.get_text(separator="\n", strip=True))
            for articles in section_articles
        ]
        section_articles = "\n".join(section_articles[1:])
        section_articles = [
            articles.split("\n")
            for articles in re.split(r"\n{2,}", section_articles)
            if len(articles.split("\n")) >= 3
        ]

        for i, articles in enumerate(section_articles):
            news_id = str(uuid.uuid4())

            tag = articles[0]
            content = "\n".join(
                [line for j, line in enumerate(articles) if j not in [0, 2]]
            )

            news = {
                "id": news_id,
                "email_uid": self.msg.uid,
                "email_date": self.msg.date,
                "article_section": section["section_title"],
                "article_title": section["article_titles"][i],
                "article_content": content,
                "article_link": section["article_links"][i],
                "article_tag": tag,
            }

            news_logs = {
                "id": news_id,
                "email_uid": self.msg.uid,
                "email_date": self.msg.date,
                "article_section": section["section_title"],
                "article_title": section["article_titles"][i],
                "has_link": section["article_links"][i] != "",
                "status": OK if section["article_links"][i] != "" else KO,
                "comment": "",
            }

            self.article_data.append(news)
            self.article_logs.append(news_logs)
