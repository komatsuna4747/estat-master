"""Unit tests for estat_master.processor.utils module."""

import pandas as pd

from estat_master.processor.utils import clean_description


class TestCleanDescription:
    """Tests for clean_description function."""

    def test_clean_description_with_newlines(self) -> None:
        """Test cleaning description with various newline characters."""
        desc = "Line 1\r\nLine 2\nLine 3\rLine 4"
        result = clean_description(desc)
        assert result == "Line 1 Line 2 Line 3 Line 4"

    def test_clean_description_with_extra_whitespace(self) -> None:
        """Test normalizing extra whitespace."""
        desc = "Text  with   multiple    spaces"
        result = clean_description(desc)
        assert result == "Text with multiple spaces"

    def test_clean_description_with_tabs(self) -> None:
        """Test cleaning description with tabs."""
        desc = "Text\twith\ttabs"
        result = clean_description(desc)
        assert result == "Text with tabs"

    def test_clean_description_with_none(self) -> None:
        """Test handling None input."""
        result = clean_description(None)
        assert result is None

    def test_clean_description_with_nan(self) -> None:
        """Test handling NaN input."""
        result = clean_description(float("nan"))
        assert result is None

    def test_clean_description_with_pd_na(self) -> None:
        """Test handling pandas NA."""
        result = clean_description(pd.NA)
        assert result is None

    def test_clean_description_empty_string(self) -> None:
        """Test handling empty string."""
        result = clean_description("")
        assert result == ""

    def test_clean_description_normal_text(self) -> None:
        """Test with normal text without special characters."""
        desc = "Normal description text"
        result = clean_description(desc)
        assert result == "Normal description text"

    def test_clean_description_mixed_issues(self) -> None:
        """Test with mixed formatting issues."""
        desc = "Text\r\nwith  \nmultiple   \rissues   and\textra    whitespace"
        result = clean_description(desc)
        assert result == "Text with multiple issues and extra whitespace"
