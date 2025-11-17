import logging

import click

from estat_master.etl.base import BaseETL, BaseETLConfig
from estat_master.etl.jsic import JSICMasterETL
from estat_master.types import MasterDataType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def create_etl(data_type: MasterDataType, config: BaseETLConfig) -> BaseETL:
    """Factory function to create ETL instances based on data type."""
    if data_type == MasterDataType.JSIC:
        return JSICMasterETL(config=config)

    msg = f"Unsupported data type for ETL: {data_type}"
    raise ValueError(msg)


@click.command()
@click.option(
    "--data-type",
    type=str,
    required=True,
    help="Path to save the output file.",
)
@click.option(
    "--output-path",
    type=click.Path(),
    required=True,
    help="Path to save the output file.",
)
@click.option(
    "--revision",
    type=str,
    default=None,
    help="Revision code to process. If None, the latest revision will be used.",
)
@click.option(
    "--output-format",
    type=click.Choice(["json", "csv"]),
    default="json",
    help="Output file format, either 'json' or 'csv' (default: 'json').",
)
@click.option(
    "--debug-code-limit",
    type=int,
    default=None,
    help="If set, limits the number of class codes to process for debugging.",
)
def run_etl(
    data_type: MasterDataType, output_path: str, revision: str, output_format: str, debug_code_limit: int | None
) -> None:
    """Run the ETL process for the specified data type."""

    config = BaseETLConfig(
        output_path=output_path, revision=revision, output_format=output_format, debug_code_limit=debug_code_limit
    )
    etl_instance: BaseETL = create_etl(data_type=data_type, config=config)
    etl_instance.run()


if __name__ == "__main__":
    run_etl()
