"""Unit tests for estat_master.processor.jsic module."""

import pandas as pd
import pytest
from pandera.typing.pandas import DataFrame

from estat_master.processor.jsic import create_jsic_flat_master_table, determine_hierarchy
from estat_master.schema.jsic import RawJSICTableSchema


class TestDetermineHierarchy:
    """Tests for determine_hierarchy function."""

    def test_division_level(self) -> None:
        """Test division level codes (single letter)."""
        assert determine_hierarchy("A") == 1
        assert determine_hierarchy("Z") == 1

    def test_major_group_level(self) -> None:
        """Test major group level codes (2 digits)."""
        assert determine_hierarchy("01") == 2
        assert determine_hierarchy("99") == 2

    def test_group_level(self) -> None:
        """Test group level codes (3 digits)."""
        assert determine_hierarchy("011") == 3
        assert determine_hierarchy("999") == 3

    def test_class_level(self) -> None:
        """Test class level codes (4 digits)."""
        assert determine_hierarchy("0111") == 4
        assert determine_hierarchy("9999") == 4


class TestCreateJSICFlatMasterTable:
    """Tests for create_jsic_flat_master_table function."""

    @pytest.fixture
    def sample_raw_data(self) -> DataFrame[RawJSICTableSchema]:
        """Create sample raw JSIC data for testing."""
        data = {
            "code": ["A", "01", "011", "0111", "0112"],
            "code_name": [
                "農業、林業",
                "農業",
                "耕種農業",
                "米作農業",
                "麦類作農業",
            ],
            "desc": [
                "農業と林業の説明",
                "農業の説明",
                "耕種農業の説明",
                "米作農業の説明",
                "麦類作農業の説明",
            ],
        }
        df = pd.DataFrame(data)
        return DataFrame[RawJSICTableSchema](df)

    def test_creates_flat_table(self, sample_raw_data: DataFrame[RawJSICTableSchema]) -> None:
        """Test that flat table is created with correct structure."""
        result = create_jsic_flat_master_table(sample_raw_data)

        # Check that only class-level codes are included
        assert len(result) == 2  # Only 0111 and 0112

        # Check that all hierarchy columns exist
        expected_columns = [
            "class_code",
            "class_code_name",
            "class_desc",
            "group_code",
            "group_code_name",
            "group_desc",
            "major_group_code",
            "major_group_code_name",
            "major_group_desc",
            "division_code",
            "division_code_name",
            "division_desc",
        ]
        for col in expected_columns:
            assert col in result.columns

    def test_hierarchy_values_correct(self, sample_raw_data: DataFrame[RawJSICTableSchema]) -> None:
        """Test that hierarchy values are correctly assigned."""
        result = create_jsic_flat_master_table(sample_raw_data)

        first_row = result.iloc[0]

        # Check class level
        assert first_row["class_code"] == "0111"
        assert first_row["class_code_name"] == "米作農業"

        # Check group level
        assert first_row["group_code"] == "011"
        assert first_row["group_code_name"] == "耕種農業"

        # Check major group level
        assert first_row["major_group_code"] == "01"
        assert first_row["major_group_code_name"] == "農業"

        # Check division level
        assert first_row["division_code"] == "A"
        assert first_row["division_code_name"] == "農業、林業"

    def test_multiple_classes_same_group(self, sample_raw_data: DataFrame[RawJSICTableSchema]) -> None:
        """Test that multiple classes in the same group are handled correctly."""
        result = create_jsic_flat_master_table(sample_raw_data)

        # Both classes should have the same parent hierarchy
        assert result.iloc[0]["group_code"] == result.iloc[1]["group_code"]
        assert result.iloc[0]["major_group_code"] == result.iloc[1]["major_group_code"]
        assert result.iloc[0]["division_code"] == result.iloc[1]["division_code"]

        # But different class codes
        assert result.iloc[0]["class_code"] != result.iloc[1]["class_code"]
