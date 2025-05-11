import re
import pandas as pd


# Function to clean strings by removing non-alphanumeric characters
def clean_string(text):
    # Remove all non-alphanumeric characters except spaces
    text = re.sub(r"[^a-zA-Z0-9 ]", "", text)

    # Replace multiple spaces with a single space and strip
    text = re.sub(r"\s+", " ", text).strip()

    return text


OK = "OK"
KO = "KO"

section_title_fix = {
    4: {
        "Top Papers": ["525", "584"],
        "Top Lectures": ["581"],
    },
    5: {
        "Top Lectures": ["500", "510", "519", "547", "573", "616"],
        "Top Papers": ["501", "537", "567", "574"],
        "Top Repos": ["507", "532", "564"],
        "Top Models": ["509", "534", "543", "565", "608"],
        "Trending Repos": ["517"],
        "Top Tutorials": ["522", "530", "559", "569", "576"],
    },
    6: {
        "Top Models": ["572"],
    },
}

SECTION_TITLE_FIX = pd.DataFrame()

for insert_pos in section_title_fix.keys():
    for section_title in section_title_fix[insert_pos]:
        for email_uid in section_title_fix[insert_pos][section_title]:
            row = {
                "email_uid": email_uid,
                "insert_pos": insert_pos,
                "section_title": section_title,
            }
            row = pd.DataFrame([row])
            SECTION_TITLE_FIX = pd.concat([SECTION_TITLE_FIX, row], ignore_index=True)
