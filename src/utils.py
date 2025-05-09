import re


# Function to clean strings by removing non-alphanumeric characters
def clean_string(text):
    # Remove all non-alphanumeric characters except spaces
    text = re.sub(r"[^a-zA-Z0-9 ]", "", text)

    # Replace multiple spaces with a single space and strip
    text = re.sub(r"\s+", " ", text).strip()

    return text


OK = "ok"
NOT_HANDLED = "not handled"
