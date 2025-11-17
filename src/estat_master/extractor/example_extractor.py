import requests
from bs4 import BeautifulSoup

JSIC_REVISION_RELEASE_DATE_MAPPING = {
    "04": "2023-07-01",
    "03": "2017-10-01",
    "02": "2013-10-01",
    "01": "2007-10-01",
}


def extract_examples_for_code(code: str, revision: str, revision_release_date_mapping: dict) -> dict:
    """Fetch examples, unsuitable examples, and release date for a class code.

    Args:
        code: The class code to fetch
        revision: The revision code (default: 04)

    Returns:
        A dictionary containing examples, unsuitable examples, and release date.

    """
    url = f"https://www.e-stat.go.jp/classifications/terms/10/{revision}/{code}"

    response = requests.get(url, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.content, "html.parser")
    dict_raw = {
        th.get_text(strip=True): td.get_text(strip=True)
        for th, td in zip(soup.find_all("th"), soup.find_all("td"), strict=True)
    }

    return {
        "code": code,
        "example": dict_raw.get("事例"),
        "unsuitable_example": dict_raw.get("不適合事例"),
        "release_date": revision_release_date_mapping.get(revision),
    }
