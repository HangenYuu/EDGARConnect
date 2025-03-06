import requests
from dotenv import dotenv_values

config = dotenv_values(".env")

QUERY_ENDPOINT = f"https://api.sec-api.io?token={config['SEC_API_TOKEN']}"
EXTRACT_ENDPOINT = f"https://api.sec-api.io/extractor"


def query_files(query: str = 'formType:"10-K"', start: int = 0, size: int = 50):
    """
    Query the SEC API for filings.

    Args:
        query (str): The query string.

    Returns:
        dict: The response from the SEC API.
    """
    data = {
        "query": query,
        "from": str(start),
        "size": str(size),
    }
    response = requests.post(QUERY_ENDPOINT, json=data)

    if response.status_code != 200:
        raise Exception(f"Error: {response.status_code} - {response.text}")

    return response.json()


def extract_item(url: str, item: str = "1A", response_type: str = "text"):
    """
    Extract a specific item from a filing.

    Args:
        url (str): The URL of the filing.
        item (str): The item to extract.
        response_type (str): The type of response to return.

    Returns:
        dict: The extracted item.
    """

    data = {
        "url": url,
        "item": item,
        "type": response_type,
        "token": config["SEC_API_TOKEN"],
    }
    response = requests.get(EXTRACT_ENDPOINT, params=data)

    if response.status_code != 200:
        raise Exception(f"Error: {response.status_code} - {response.text}")

    return response.text
