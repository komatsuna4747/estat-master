"""Unit tests for estat_master.extractor.master_downloader module."""

from unittest.mock import Mock, patch

import pandas as pd
import pytest
import requests

from estat_master.extractor.master_downloader import EStatDownloadError, download_estat_classification_master


class TestDownloadEstatClassificationMaster:
    """Tests for download_estat_classification_master function."""

    @pytest.fixture
    def mock_csv_response(self) -> str:
        """Create mock CSV response from e-stat."""
        return """Title Line
Header Line
Note Line
A,農業、林業,農業と林業の説明
01,農業,農業の説明
011,耕種農業,耕種農業の説明
0111,米作農業,米作農業の説明
"""

    @patch("estat_master.extractor.master_downloader.requests.get")
    def test_download_success(self, mock_get: Mock, mock_csv_response: str) -> None:
        """Test successful download and parsing of classification master."""
        # Setup mock response
        mock_response = Mock()
        mock_response.text = mock_csv_response
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        # Call function
        result = download_estat_classification_master("10", "04")

        # Assertions
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 4
        assert list(result.columns) == ["code", "code_name", "desc"]

        # Check first row
        assert result.iloc[0]["code"] == "A"
        assert result.iloc[0]["code_name"] == "農業、林業"

        # Verify correct parameters were sent
        mock_get.assert_called_once()
        _, kwargs = mock_get.call_args
        assert kwargs["params"]["bKbn"] == "10"
        assert kwargs["params"]["kaiteiCode"] == "04"
        assert kwargs["params"]["charset"] == "UTF-8"

    @patch("estat_master.extractor.master_downloader.requests.get")
    def test_download_with_different_revision(self, mock_get: Mock, mock_csv_response: str) -> None:
        """Test download with different revision code."""
        # Setup mock response
        mock_response = Mock()
        mock_response.text = mock_csv_response
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        # Call function with revision 03
        download_estat_classification_master("10", "03")

        # Verify correct revision was sent
        _, kwargs = mock_get.call_args
        assert kwargs["params"]["kaiteiCode"] == "03"

    @patch("estat_master.extractor.master_downloader.requests.get")
    def test_download_request_exception(self, mock_get: Mock) -> None:
        """Test handling of request exceptions."""
        # Setup mock to raise exception
        mock_get.side_effect = requests.RequestException("Network error")

        # Call function and expect exception
        with pytest.raises(EStatDownloadError) as exc_info:
            download_estat_classification_master("10", "04")

        assert "Failed to download data" in str(exc_info.value)
        assert "classification_type=10" in str(exc_info.value)
        assert "revision_code=04" in str(exc_info.value)

    @patch("estat_master.extractor.master_downloader.requests.get")
    def test_download_empty_response(self, mock_get: Mock) -> None:
        """Test handling of empty response."""
        # Setup mock response with empty text
        mock_response = Mock()
        mock_response.text = "   "
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        # Call function and expect exception
        with pytest.raises(EStatDownloadError) as exc_info:
            download_estat_classification_master("10", "04")

        assert "Received empty response" in str(exc_info.value)

    @patch("estat_master.extractor.master_downloader.requests.get")
    def test_download_timeout_parameter(self, mock_get: Mock, mock_csv_response: str) -> None:
        """Test that timeout parameter is set correctly."""
        # Setup mock response
        mock_response = Mock()
        mock_response.text = mock_csv_response
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        # Call function
        download_estat_classification_master("10", "04")

        # Verify timeout was set
        _, kwargs = mock_get.call_args
        assert kwargs.get("timeout") == 60

    @patch("estat_master.extractor.master_downloader.requests.get")
    def test_download_all_columns_present(self, mock_get: Mock) -> None:
        """Test that all expected columns are present in downloaded data."""
        # Setup mock response
        valid_csv = """Title
Header
Note
A,農業、林業,説明
"""
        mock_response = Mock()
        mock_response.text = valid_csv
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        # Call function
        result = download_estat_classification_master("10", "04")

        # Verify all columns are present
        assert "code" in result.columns
        assert "code_name" in result.columns
        assert "desc" in result.columns
