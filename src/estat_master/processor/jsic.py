import pandera as pa
from pandera.typing.pandas import DataFrame

from estat_master.schema.jsic import JSICFlatMasterSchema, RawJSICTableSchema


def determine_hierarchy(code: str) -> int:
    """ArithmeticErrorDetermine hierarchy level based on code pattern:
    - Single letter (A, B, etc.) = Level 1
    - 2 digits (01, 02, etc.) = Level 2
    - 3 digits (010, 011, etc.) = Level 3
    - 4 digits (0100, 0109, etc.) = Level 4
    """
    return 1 if code.isalpha() else len(code)


@pa.check_types
def create_jsic_flat_master_table(df: DataFrame[RawJSICTableSchema]) -> DataFrame[JSICFlatMasterSchema]:
    """Create flat master table from hierarchical classification DataFrame."""
    # Determine hierarchy levels
    df["level"] = df["code"].apply(determine_hierarchy)

    # Forward fill parent codes
    df["division_code"] = df["code"].where(df["level"] == 1).ffill()
    df["major_group_code"] = df["code"].where(df["level"] == 2).ffill()
    df["group_code"] = df["code"].where(df["level"] == 3).ffill()

    df_class = (
        df.query("level == 4")[["code", "code_name", "desc", "group_code", "major_group_code", "division_code"]]
        .rename(columns={"code": "class_code", "code_name": "class_code_name", "desc": "class_desc"})
        .reset_index(drop=True)
    )

    dim_code_names = df[["code", "code_name", "desc"]]

    # Merge all levels into a flat master table
    flat_master_df = (
        df_class.merge(dim_code_names, left_on="group_code", right_on="code", how="left")
        .drop(columns=["code"])
        .rename(columns={"code_name": "group_code_name", "desc": "group_desc"})
        .merge(dim_code_names, left_on="major_group_code", right_on="code", how="left")
        .drop(columns=["code"])
        .rename(columns={"code_name": "major_group_code_name", "desc": "major_group_desc"})
        .merge(dim_code_names, left_on="division_code", right_on="code", how="left")
        .drop(columns=["code"])
        .rename(columns={"code_name": "division_code_name", "desc": "division_desc"})
    )
    return DataFrame[JSICFlatMasterSchema](flat_master_df)
