"""Export helpers for CSV and Excel downloads."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
from typing import Tuple
from zoneinfo import ZoneInfo

import pandas as pd

CHICAGO_TZ = ZoneInfo("America/Chicago")


@dataclass(frozen=True)
class ExportPayload:
    """Represents a generated export file."""

    content: BytesIO
    mimetype: str
    filename: str


class ExportService:
    """Generate clean CSV and XLSX files for query results."""

    def export_dataframe(self, dataframe: pd.DataFrame, ticker: str, query_type: str, file_format: str) -> ExportPayload:
        """Create an in-memory export payload for the requested file format."""
        clean_frame = dataframe.copy()
        timestamp = datetime.now(CHICAGO_TZ)
        extension = file_format.lower()
        filename = self.build_filename(ticker=ticker, query_type=query_type, timestamp=timestamp, extension=extension)

        if extension == "csv":
            content = self._to_csv(clean_frame)
            mimetype = "text/csv"
        elif extension == "xlsx":
            content = self._to_excel(clean_frame)
            mimetype = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        else:
            raise ValueError(f"Unsupported export format: {file_format}")

        return ExportPayload(content=content, mimetype=mimetype, filename=filename)

    @staticmethod
    def build_filename(ticker: str, query_type: str, timestamp: datetime, extension: str) -> str:
        """Build a descriptive export filename."""
        safe_ticker = ticker.replace("^", "").upper()
        safe_query = query_type.replace(" ", "_").replace("/", "_").lower()
        stamped = timestamp.strftime("%Y%m%d_%H%M%S_%Z")
        return f"{safe_ticker}_{safe_query}_{stamped}.{extension}"

    @staticmethod
    def _to_csv(dataframe: pd.DataFrame) -> BytesIO:
        """Convert a DataFrame to UTF-8 CSV for Excel-friendly downloads."""
        buffer = BytesIO()
        csv_data = dataframe.to_csv(index=False)
        buffer.write(csv_data.encode("utf-8-sig"))
        buffer.seek(0)
        return buffer

    @staticmethod
    def _to_excel(dataframe: pd.DataFrame) -> BytesIO:
        """Convert a DataFrame to XLSX using pandas."""
        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            dataframe.to_excel(writer, index=False, sheet_name="MarketData")
        buffer.seek(0)
        return buffer
