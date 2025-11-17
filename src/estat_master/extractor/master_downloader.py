import io
from typing import Final

import pandas as pd
import pandera.pandas as pa
import requests

DOWNLOAD_URL: Final[str] = "https://www.e-stat.go.jp/term/download"
ENCODING: Final[str] = "UTF-8"
TIMEOUT: Final[int] = 60


class EStatClassificationMasterSchema(pa.DataFrameModel):
    """Schema for E-Stat Classification Master DataFrame.
    Raw data from e-stat download. The dataframe is long format with three columns: code, code_name, desc."""

    code: str = pa.Field(nullable=False, unique=True, description="Classification code")
    code_name: str = pa.Field(nullable=False, description="Classification name")
    desc: str = pa.Field(nullable=True, description="Classification description")


class EStatDownloadError(Exception):
    """Exception raised when downloading E-Stat data fails."""


def download_estat_classification_master(classification_type: str, revision_code: str) -> pd.DataFrame:
    params = {
        "bKbn": classification_type,
        "kaiteiCode": revision_code,
        "charset": ENCODING,
    }
    try:
        response = requests.get(DOWNLOAD_URL, params=params, timeout=TIMEOUT)
        response.raise_for_status()
    except requests.RequestException as e:
        msg = (
            f"Failed to download data for classification_type={classification_type}, revision_code={revision_code}: {e}"
        )
        raise EStatDownloadError(msg) from e

    # Check if response contains valid data
    if not response.text.strip():
        msg = "Received empty response from E-Stat server"
        raise EStatDownloadError(msg)

    with io.StringIO(response.text) as csv_io:
        df = pd.read_csv(csv_io, skiprows=[0, 1, 2], dtype=str, names=["code", "code_name", "desc"])

    return EStatClassificationMasterSchema.validate(df)
