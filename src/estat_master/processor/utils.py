import pandas as pd


def clean_description(desc: str | None) -> str | None:
    if desc is None or pd.isna(desc):
        return None
    desc = desc.replace("\r\n", " ").replace("\n", " ").replace("\r", " ")
    desc = " ".join(desc.split())  # Normalize whitespace
    return desc
