"""Unit tests for estat_master.extractor.example_extractor module."""

from unittest.mock import Mock, patch

import pytest

from estat_master.extractor.example_extractor import extract_examples_for_code


class TestExtractExamplesForCode:
    """Tests for extract_examples_for_code function."""

    @pytest.fixture
    def mock_html_response(self) -> str:
        """Create mock HTML response from e-stat."""
        return """
        <html>
            <body>
                <table>
                    <tr>
                        <th>事例</th>
                        <td>稲作農業；水稲栽培</td>
                    </tr>
                    <tr>
                        <th>不適合事例</th>
                        <td>米の販売；米の加工</td>
                    </tr>
                </table>
            </body>
        </html>
        """  # noqa: RUF001

    @pytest.fixture
    def revision_mapping(self) -> dict[str, str]:
        """Create sample revision mapping."""
        return {
            "04": "2023-07-01",
            "03": "2013-10-01",
        }

    @patch("estat_master.extractor.example_extractor.requests.get")
    def test_extract_examples_success(
        self, mock_get: Mock, mock_html_response: str, revision_mapping: dict[str, str]
    ) -> None:
        """Test successful extraction of examples."""
        # Setup mock response
        mock_response = Mock()
        mock_response.content = mock_html_response.encode("utf-8")
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        # Call function
        result = extract_examples_for_code("0111", "04", revision_mapping)

        # Assertions
        assert result["code"] == "0111"
        assert result["example"] == "稲作農業；水稲栽培"  # noqa: RUF001
        assert result["unsuitable_example"] == "米の販売；米の加工"  # noqa: RUF001
        assert result["release_date"] == "2023-07-01"

        # Verify correct URL was called
        mock_get.assert_called_once()
        call_args = mock_get.call_args
        assert "https://www.e-stat.go.jp/classifications/terms/10/04/0111" in str(call_args)

    @patch("estat_master.extractor.example_extractor.requests.get")
    def test_extract_examples_no_examples(self, mock_get: Mock, revision_mapping: dict[str, str]) -> None:
        """Test extraction when no examples are found."""
        # Setup mock response with no examples
        mock_response = Mock()
        mock_response.content = b"<html><body><table></table></body></html>"
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        # Call function
        result = extract_examples_for_code("9999", "04", revision_mapping)

        # Assertions
        assert result["code"] == "9999"
        assert result["example"] is None
        assert result["unsuitable_example"] is None
        assert result["release_date"] == "2023-07-01"

    @patch("estat_master.extractor.example_extractor.requests.get")
    def test_extract_examples_different_revision(
        self, mock_get: Mock, mock_html_response: str, revision_mapping: dict[str, str]
    ) -> None:
        """Test extraction with different revision code."""
        # Setup mock response
        mock_response = Mock()
        mock_response.content = mock_html_response.encode("utf-8")
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        # Call function with revision 03
        result = extract_examples_for_code("0111", "03", revision_mapping)

        # Assertions
        assert result["release_date"] == "2013-10-01"

        # Verify correct URL was called
        call_args = mock_get.call_args
        assert "https://www.e-stat.go.jp/classifications/terms/10/03/0111" in str(call_args)

    @patch("estat_master.extractor.example_extractor.requests.get")
    def test_extract_examples_timeout(self, mock_get: Mock, revision_mapping: dict[str, str]) -> None:
        """Test that timeout parameter is passed to requests."""
        # Setup mock response
        mock_response = Mock()
        mock_response.content = b"<html><body></body></html>"
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        # Call function
        extract_examples_for_code("0111", "04", revision_mapping)

        # Verify timeout was set
        mock_get.assert_called_once()
        _, kwargs = mock_get.call_args
        assert kwargs.get("timeout") == 30
