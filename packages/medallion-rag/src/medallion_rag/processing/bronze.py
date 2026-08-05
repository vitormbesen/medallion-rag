"""Data extraction and processing for persistence on the Bronze Layer."""

import hashlib
import json
from typing import TYPE_CHECKING
import urllib.parse
import urllib.request

if TYPE_CHECKING:
    from datetime import date


def generate_document_id(text: str) -> str:
    """Generate unique document_id based on its content alone."""
    return hashlib.sha256(text.encode()).hexdigest()


def fetch_document_from_api(title: str, logical_date: date, user_agent: str) -> dict | None:
    """
    Data extraction function.
    Fetch text from an existing Wikipedia article using Wikipedia Open API.
    Formats extract API data to bronze layer model.
    """
    url = (
        'https://en.wikipedia.org/w/api.php?action=query&prop=extracts|'
        f'info&explaintext=1&inprop=url&titles={urllib.parse.quote(title)}&format=json'
    )

    headers = {'User-Agent': user_agent}  # required
    req = urllib.request.Request(url, headers=headers)

    try:
        with urllib.request.urlopen(req) as response:
            data = json.loads(response.read().decode('utf-8'))
            pages = data.get('query', {}).get('pages', {})
            for page_id, page_data in pages.items():
                if page_id == '-1':  # Page not found
                    return None

                # Metadata + text
                raw_payload = {
                    'page_id': page_id,
                    'title': page_data.get('title'),
                    'text': page_data.get('extract', ''),
                    'fullurl': page_data.get('fullurl'),
                    'touched': page_data.get('touched'),
                }

                return {
                    'document_id': generate_document_id(raw_payload['text']),
                    'payload': raw_payload,
                    'logical_date': logical_date,
                }

    except Exception as e:
        print(f'Failed to fetch {title}: {e}')
        return None
