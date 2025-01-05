import requests
from bs4 import BeautifulSoup
import polars as pl

class TableExtractionError(Exception):
    """Custom exception for table extraction errors."""
    pass

def fetch_webpage(url):
    """
    Fetches the HTML content of a webpage.

    Args:
        url (str): The URL of the webpage.

    Returns:
        str: The HTML content of the webpage.

    Raises:
        TableExtractionError: If the webpage cannot be fetched.
    """
    try:
        response = requests.get(url)
        if response.status_code != 200:
            raise TableExtractionError(f"Failed to fetch page. Status code: {response.status_code}")
        return response.text
    except requests.RequestException as e:
        raise TableExtractionError(f"Failed to fetch webpage: {e}")


def parse_table_columns(html, table_id):
    """
    Parses the HTML content to extract table columns by table ID.

    Args:
        html (str): The HTML content of the webpage.
        table_id (str): The ID of the target HTML table.

    Returns:
        list: A list of column names (strings) from the table.

    Raises:
        TableExtractionError: If the table is not found or parsing fails.
    """
    try:
        soup = BeautifulSoup(html, 'html.parser')
        table_data = soup.find(id=table_id)
        if not table_data:
            raise TableExtractionError(f"Table with id '{table_id}' not found.")
        
        # Extract column names
        return [
            th.text.strip() for th in table_data.find_all('th')
            if th.get('scope') != 'row'
        ]
    except Exception as e:
        raise TableExtractionError(f"Failed to parse HTML: {e}")


def extract_table_cols(url, table_id):
    """
    Extracts column names from an HTML table by fetching the webpage and parsing the table.

    Args:
        url (str): The URL of the webpage containing the table.
        table_id (str): The ID of the target HTML table.

    Returns:
        list: A list of column names (strings) from the table.
    """
    html = fetch_webpage(url)
    return parse_table_columns(html, table_id)