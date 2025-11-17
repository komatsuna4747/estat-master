import pandera.pandas as pa


class RawJSICTableSchema(pa.DataFrameModel):
    """Schema for E-Stat Classification Master DataFrame.
    Raw data from e-stat download. The dataframe is long format with three columns: code, code_name, desc."""

    code: str = pa.Field(
        nullable=False,
        unique=True,
        description="Classification code",
        str_matches=r"^([A-Z]|[0-9]{2}|[0-9]{3}|[0-9]{4})$",
    )
    code_name: str = pa.Field(nullable=False, description="Classification name")
    desc: str = pa.Field(nullable=True, description="Classification description")


class JSICExampleSchema(pa.DataFrameModel):
    """Schema for JSIC Classification Example DataFrame."""

    code: str = pa.Field(
        nullable=False,
        unique=True,
        description="Classification code",
        str_matches=r"^[0-9]{4}$",
    )
    example: str = pa.Field(nullable=True, description="Example of the class")
    unsuitable_example: str = pa.Field(nullable=True, description="Unsuitable example of the class")
    release_date: str = pa.Field(
        nullable=True, description="Release date of the example", str_matches=r"^\d{4}-\d{2}-\d{2}$"
    )


class JSICFlatMasterSchema(pa.DataFrameModel):
    """Schema for JSIC Classification Master DataFrame."""

    division_code: str = pa.Field(description="Division code (大分類コード)", str_matches=r"^[A-Z]$")
    division_code_name: str = pa.Field(description="Division name (大分類名)")
    division_desc: str = pa.Field(description="Division description (大分類説明)")
    major_group_code: str = pa.Field(description="Major group code (中分類コード)", str_matches=r"^[0-9]{2}$")
    major_group_code_name: str = pa.Field(description="Major group name (中分類名)")
    major_group_desc: str = pa.Field(description="Major group description (中分類説明)")
    group_code: str = pa.Field(description="Group code (小分類コード)", str_matches=r"^[0-9]{3}$")
    group_code_name: str = pa.Field(description="Group name (小分類名)")
    group_desc: str = pa.Field(nullable=True, description="Group description (小分類説明)")
    class_code: str = pa.Field(unique=True, description="Class code (細分類コード)", str_matches=r"^[0-9]{4}$")
    class_code_name: str = pa.Field(description="Class name (細分類名)")
    class_desc: str = pa.Field(nullable=True, description="Class description (細分類説明)")


class JSICMasterSchema(JSICFlatMasterSchema):
    """Schema for JSIC Classification Master DataFrame with examples."""

    example: str = pa.Field(nullable=True, description="Example of the class")
    unsuitable_example: str = pa.Field(nullable=True, description="Unsuitable example of the class")
    release_date: str = pa.Field(nullable=True, description="Release date of the example")
