import time
from dataclasses import dataclass
from logging import getLogger
from typing import Final

import pandas as pd
from pandera.typing.pandas import DataFrame

from estat_master.etl.base import BaseETL, BaseETLConfig
from estat_master.extractor.example_extractor import extract_examples_for_code
from estat_master.extractor.master_downloader import download_estat_classification_master
from estat_master.processor.jsic import create_jsic_flat_master_table
from estat_master.processor.utils import clean_description
from estat_master.schema.jsic import JSICExampleSchema, JSICMasterSchema, RawJSICTableSchema

logger = getLogger(__name__)

JSIC_CLASSIFICATION_TYPE: Final[str] = "10"

JSIC_REVISION_RELEASE_DATE_MAPPING: Final[dict[str, str]] = {
    "04": "2023-07-01",
    "03": "2013-10-01",
    "02": "2007-11-01",
    "01": "2002-03-01",
}

JSIC_REVISION_CODES: Final[list[str]] = list(JSIC_REVISION_RELEASE_DATE_MAPPING.keys())


@dataclass
class JSICMasterETLIn:
    raw_jsic_master: DataFrame[RawJSICTableSchema]
    class_examples: DataFrame[JSICExampleSchema]


@dataclass
class JSICMasterETLOut:
    jsic_master: DataFrame[JSICMasterSchema]


class JSICMasterETL(BaseETL[JSICMasterETLIn, JSICMasterETLOut]):
    _etl_in: JSICMasterETLIn | None = None
    _etl_out: JSICMasterETLOut | None = None

    """
    ETL process for JSIC Classification Master Data with examples.

    Args:
        output_path: Path to save the output file.
        revision: Revision code to process (default: latest).
        output_format: Output file format, either 'json' or 'csv' (default: 'json').
        debug_code_limit: If set, limits the number of class codes to process for debugging.
    """

    def __init__(self, config: BaseETLConfig) -> None:
        self.config = config

    def extract(self) -> JSICMasterETLIn:
        logger.info("Starting JSIC Master ETL for revision %s", self.config.revision)
        df_raw_master = download_estat_classification_master(
            classification_type=JSIC_CLASSIFICATION_TYPE, revision_code=self.config.revision
        )
        logger.info("Extracted raw JSIC master data")

        logger.info("Starting extraction of examples for class codes")
        class_codes = df_raw_master[df_raw_master["code"].str.len() == 4]["code"].tolist()

        if self.config.debug_code_limit is not None:
            class_codes = class_codes[: self.config.debug_code_limit]
        example_data = []
        for code in class_codes:
            example = extract_examples_for_code(
                code=code,
                revision=self.config.revision,
                revision_release_date_mapping=JSIC_REVISION_RELEASE_DATE_MAPPING,
            )
            time.sleep(0.5)  # Be polite to the server
            example_data.append(example)
            if len(example_data) % 50 == 0:
                logger.info(
                    "JSIC Master Extractor: Extracted examples for %d/%d class codes",
                    len(example_data),
                    len(class_codes),
                )
        df_examples = pd.DataFrame(example_data)
        logger.info("Completed extraction of examples for class codes")
        # Reorder columns to match schema
        self._etl_in = JSICMasterETLIn(
            raw_jsic_master=DataFrame[RawJSICTableSchema](df_raw_master),
            class_examples=DataFrame[JSICExampleSchema](df_examples),
        )
        return self._etl_in

    def transform(self, data: JSICMasterETLIn) -> JSICMasterETLOut:
        logger.info("Starting transformation of JSIC master data")

        df_raw_master = data.raw_jsic_master
        df_raw_master["desc"] = df_raw_master["desc"].apply(clean_description)
        df_flat_master = create_jsic_flat_master_table(data.raw_jsic_master)
        df_output = df_flat_master.merge(data.class_examples, left_on="class_code", right_on="code", how="inner").drop(
            columns=["code"]
        )
        logger.info("Completed transformation of JSIC master data")
        # Reorder columns to match output schema
        column_order = list(JSICMasterSchema.to_schema().columns)
        df_output = DataFrame[JSICMasterSchema](df_output[column_order])
        self._etl_out = JSICMasterETLOut(jsic_master=df_output)
        return self._etl_out

    def load(self, data: JSICMasterETLOut) -> None:
        logger.info("Loading JSIC master data to %s as %s", self.config.output_path, self.config.output_format)
        if self.config.output_format == "json":
            data.jsic_master.to_json(self.config.output_path, orient="records", force_ascii=False, indent=2)
            return

        if self.config.output_format == "csv":
            data.jsic_master.to_csv(self.config.output_path, index=False)
            return

        msg = f"Unsupported output format: {self.config.output_format}"
        raise ValueError(msg)
