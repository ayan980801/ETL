# Databricks notebook source
from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
import logging
from functools import wraps
import re
import sys
import traceback
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    current_timestamp,
    lit,
    concat_ws,
    col,
    udf,
)
from pyspark.sql.types import StringType
import hashlib
from delta.tables import DeltaTable
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    RetryError,
)

# ========== CONFIGURATION: SET ETL MODE HERE ==============
ETL_MODE = "historical"   # Set to "historical" or "incremental"
# ==========================================================

def log_exceptions(default=None, exit_on_error=False):
    """Decorator for standardized exception logging."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as exc:  # pragma: no cover - dynamic environments
                logging.error(f"Error in {func.__name__}: {exc}")
                logging.error(traceback.format_exc())
                if exit_on_error:
                    sys.exit(1)
                return default
        return wrapper
    return decorator


@udf(StringType())
def sha3_512_udf(val: str) -> Optional[str]:
    if val is None:
        return None
    return hashlib.sha3_512(val.encode("utf-8")).hexdigest()

@dataclass
class ServerDetails:
    server_url: str
    username: str
    password: str

@dataclass
class AzureDetails:
    base_path: str
    stage: str

@dataclass
class SnowflakeConfig:
    sfURL: str
    sfUser: str
    sfPassword: str
    sfDatabase: str
    sfWarehouse: str
    sfSchema: str
    sfRole: str

class LeadDepotETL:
    # EXPLICIT PIPELINE NAMESPACING (do not mix with LCR)
    ADLS_BASE_PATH: str = "abfss://dataarchitecture@quilitydatabricks.dfs.core.windows.net/RAW/LeadDepot"
    METADATA_BASE_PATH: str = "dbfs:/FileStore/DataProduct/DataArchitecture/Pipelines/LeadDepot/Metadata"

    log_exceptions = staticmethod(log_exceptions)
    # FULL TABLE MAPPING WITH NO ELISION
    SNOWFLAKE_TABLES: Dict[str, Dict[str, str]] = {
        "County": {
            "snowflake_table": "STG_LDP_COUNTY",
            "hash_column_name": "STG_LDP_COUNTY_KEY",
            "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_COUNTY",
        },
        "DataMAILCO": {
            "snowflake_table": "STG_LDP_DATAMAILCO",
            "hash_column_name": "STG_LDP_DATAMAILCO_KEY",
            "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_DATAMAILCO",
        },
        "Delivery": {
            "snowflake_table": "STG_LDP_DELIVERY",
            "hash_column_name": "STG_LDP_DELIVERY_KEY",
            "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_DELIVERY",
        },
        "DeliveryArea": {
            "snowflake_table": "STG_LDP_DELIVERY_AREA",
            "hash_column_name": "STG_LDP_DELIVERY_AREA_KEY",
            "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_DELIVERY_AREA",
        },
        "DeliveryFormat": {
            "snowflake_table": "STG_LDP_DELIVERY_FORMAT",
            "hash_column_name": "STG_LDP_DELIVERY_FORMAT_KEY",
            "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_DELIVERY_FORMAT",
        },
        "DeliveryLeadType": {
            "snowflake_table": "STG_LDP_DELIVERY_LEAD_TYPE",
            "hash_column_name": "STG_LDP_DELIVERY_LEAD_TYPE_KEY",
            "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_DELIVERY_LEAD_TYPE",
        },
        "DeliveryResponse": {
            "snowflake_table": "STG_LDP_DELIVERY_RESPONSE",
            "hash_column_name": "STG_LDP_DELIVERY_RESPONSE_KEY",
            "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_DELIVERY_RESPONSE",
        },
        "DeliverySourceType": {
            "snowflake_table": "STG_LDP_DELIVERY_SOURCE_TYPE",
            "hash_column_name": "STG_LDP_DELIVERY_SOURCE_TYPE_KEY",
            "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_DELIVERY_SOURCE_TYPE",
        },
        "DeliveryStatus": {
            "snowflake_table": "STG_LDP_DELIVERY_STATUS",
            "hash_column_name": "STG_LDP_DELIVERY_STATUS_KEY",
            "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_DELIVERY_STATUS",
        },
        "DeliveryType": {
            "snowflake_table": "STG_LDP_DELIVERY_TYPE",
            "hash_column_name": "STG_LDP_DELIVERY_TYPE_KEY",
            "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_DELIVERY_TYPE",
        },
        "IntakeTemp": {
            "snowflake_table": "STG_LDP_INTAKE_TEMP",
            "hash_column_name": "STG_LDP_INTAKE_TEMP_KEY",
            "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_INTAKE_TEMP",
        },
        "LCRResponse": {
            "snowflake_table": "STG_LDP_LCR_RESPONSE",
            "hash_column_name": "STG_LDP_LCR_RESPONSE_KEY",
            "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_LCR_RESPONSE",
        },
        "LeadAllocation": {
            "snowflake_table": "STG_LDP_LEAD_ALLOCATION",
            "hash_column_name": "STG_LDP_LEAD_ALLOCATION_KEY",
            "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_LEAD_ALLOCATION",
        },
        "LeadControl": {
            "snowflake_table": "STG_LDP_LEAD_CONTROL",
            "hash_column_name": "STG_LDP_LEAD_CONTROL_KEY",
            "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_LEAD_CONTROL",
        },
        "LeadFBMC": {
            "snowflake_table": "STG_LDP_LEAD_FBMC",
            "hash_column_name": "STG_LDP_LEAD_FBMC_KEY",
            "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_LEAD_FBMC",
        },
        "LeadIntake": {
            "snowflake_table": "STG_LDP_LEAD_INTAKE",
            "hash_column_name": "STG_LDP_LEAD_INTAKE_KEY",
            "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_LEAD_INTAKE",
        },
        "LeadLEADCO": {
            "snowflake_table": "STG_LDP_LEAD_LEADCO",
            "hash_column_name": "STG_LDP_LEAD_LEADCO_KEY",
            "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_LEAD_LEADCO",
        },
        "LeadLevel": {
            "snowflake_table": "STG_LDP_LEAD_LEVEL",
            "hash_column_name": "STG_LDP_LEAD_LEVEL_KEY",
            "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_LEAD_LEVEL",
        },
        "LeadMAILCO": {
            "snowflake_table": "STG_LDP_LEAD_MAILCO",
            "hash_column_name": "STG_LDP_LEAD_MAILCO_KEY",
            "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_LEAD_MAILCO",
        },
        "LeadPlum": {
            "snowflake_table": "STG_LDP_LEAD_PLUM",
            "hash_column_name": "STG_LDP_LEAD_PLUM_KEY",
            "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_LEAD_PLUM",
        },
        "LeadSFG": {
            "snowflake_table": "STG_LDP_LEAD_SFG",
            "hash_column_name": "STG_LDP_LEAD_SFG_KEY",
            "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_LEAD_SFG",
        },
        "LeadType": {
            "snowflake_table": "STG_LDP_LEAD_TYPE",
            "hash_column_name": "STG_LDP_LEAD_TYPE_KEY",
            "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_LEAD_TYPE",
        },
        "Logs": {
            "snowflake_table": "STG_LDP_LOGS",
            "hash_column_name": "STG_LDP_LOGS_KEY",
            "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_LOGS",
        },
        "LORLeads": {
            "snowflake_table": "STG_LDP_LOR_LEADS",
            "hash_column_name": "STG_LDP_LOR_LEADS_KEY",
            "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_LOR_LEADS",
        },
        "PhoneBlacklist": {
            "snowflake_table": "STG_LDP_PHONE_BLACKLIST",
            "hash_column_name": "STG_LDP_PHONE_BLACKLIST_KEY",
            "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_PHONE_BLACKLIST",
        },
        "ResponseType": {
            "snowflake_table": "STG_LDP_RESPONSE_TYPE",
            "hash_column_name": "STG_LDP_RESPONSE_TYPE_KEY",
            "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_RESPONSE_TYPE",
        },
        "RGILead": {
            "snowflake_table": "STG_LDP_RGI_LEAD",
            "hash_column_name": "STG_LDP_RGI_LEAD_KEY",
            "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_RGI_LEAD",
        },
        "SourceType": {
            "snowflake_table": "STG_LDP_SOURCE_TYPE",
            "hash_column_name": "STG_LDP_SOURCE_TYPE_KEY",
            "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_SOURCE_TYPE",
        },
        "SourceTypeChannelFunction": {
            "snowflake_table": "STG_LDP_SOURCE_TYPE_CHANNEL_FUNCTION",
            "hash_column_name": "STG_LDP_SOURCE_TYPE_CHANNEL_FUNCTION_KEY",
            "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_SOURCE_TYPE_CHANNEL_FUNCTION",
        },
        "UTMSource": {
            "snowflake_table": "STG_LDP_UTM_SOURCE",
            "hash_column_name": "STG_LDP_UTM_SOURCE_KEY",
            "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_UTM_SOURCE",
        },
        "Vendor": {
            "snowflake_table": "STG_LDP_VENDOR",
            "hash_column_name": "STG_LDP_VENDOR_KEY",
            "staging_table_name": "DEV.QUILITY_EDW_STAGE.STG_LDP_VENDOR",
        },
    }

    EXCLUDED_TABLES: set[str] = {
        "MI_LeadIntakeAnalysis",
        "MI_LeadIntakeMortgageAmounts",
    }

    def __init__(self, dbutils=None, etl_mode: Optional[str]=None) -> None:
        self.dbutils = dbutils or self._get_dbutils()
        self._configure_logging()
        self.spark = self._create_spark_session()
        self.server_details = self._load_server_details()
        self.azure_details = AzureDetails(
            base_path=self.ADLS_BASE_PATH,
            stage="RAW",
        )
        self.snowflake_config = self._load_snowflake_config()
        self.etl_mode = (etl_mode or ETL_MODE).lower()
        if self.etl_mode not in ("historical", "incremental"):
            raise ValueError("etl_mode must be either 'historical' or 'incremental'")
        logging.info(f"ETL run mode is set to: {self.etl_mode}")

    @staticmethod
    def _configure_logging() -> None:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s:%(message)s",
            handlers=[logging.StreamHandler()],
        )

    @staticmethod
    def _log_retry(retry_state) -> None:
        exc = retry_state.outcome.exception()
        attempt = retry_state.attempt_number
        func_name = retry_state.fn.__name__ if hasattr(retry_state, "fn") else "operation"
        logging.warning(
            f"Retry {attempt} for {func_name} due to {type(exc).__name__}" if exc else f"Retry {attempt} for {func_name}"
        )

    @staticmethod
    def _create_spark_session() -> SparkSession:
        return (
            SparkSession.builder.appName("LeadDepotETL")
            .config(
                "spark.jars.packages",
                ",".join(
                    [
                        "com.microsoft.sqlserver:mssql-jdbc:9.4.1.jre8",
                        "net.snowflake:snowflake-jdbc:3.13.8",
                        "net.snowflake:spark-snowflake_2.12:2.9.3",
                    ]
                ),
            )
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.databricks.delta.properties.defaults.columnMapping.mode", "name")
            .getOrCreate()
        )

    @staticmethod
    def _get_dbutils():
        try:
            import IPython
            return IPython.get_ipython().user_ns.get("dbutils")
        except Exception:
            logging.warning("dbutils not available in this environment")
            return None

    @log_exceptions(exit_on_error=True)
    def _load_server_details(self) -> ServerDetails:
        if not self.dbutils:
            raise ValueError("dbutils is required to fetch SQL Server secrets")
        return ServerDetails(
            server_url="sqlsrv-leaddepot-prd.database.windows.net:1433",
            username=self.dbutils.secrets.get(scope="dba-key-vault-secret", key="LDE-PROD-databricks-username"),
            password=self.dbutils.secrets.get(scope="dba-key-vault-secret", key="LDE-PROD-databricks-password"),
        )

    @log_exceptions(exit_on_error=True)
    def _load_snowflake_config(self) -> SnowflakeConfig:
        if not self.dbutils:
            raise ValueError("dbutils is required to fetch Snowflake secrets")
        return SnowflakeConfig(
            sfURL="https://hmkovlx-nu26765.snowflakecomputing.com",
            sfUser=self.dbutils.secrets.get(scope="key-vault-secret", key="DataProduct-SF-EDW-User"),
            sfPassword=self.dbutils.secrets.get(scope="key-vault-secret", key="DataProduct-SF-EDW-Pass"),
            sfDatabase="DEV",
            sfWarehouse="INTEGRATION_COMPUTE_WH",
            sfSchema="QUILITY_EDW_STAGE",
            sfRole="SG-SNOWFLAKE-DEVELOPERS",
        )

    @staticmethod
    def clean_column_names(df: DataFrame) -> DataFrame:
        original_cols = df.columns
        cleaned_cols = [re.sub(r"\W+", "_", c) for c in original_cols]
        if len(set(cleaned_cols)) != len(cleaned_cols):
            raise ValueError(f"Column name collision detected after cleaning: {cleaned_cols}")
        return df.toDF(*cleaned_cols)

    @staticmethod
    def add_hash_column(df: DataFrame, table_name: str) -> DataFrame:
        hash_column = LeadDepotETL.SNOWFLAKE_TABLES.get(table_name, {}).get("hash_column_name")
        if not hash_column:
            logging.info(f"No hash column configured for table {table_name}. Skipping hash generation.")
            return df

        exclude_cols = {
            "ETL_CREATED_DATE",
            "ETL_LAST_UPDATE_DATE",
            "CREATED_BY",
            "TO_PROCESS",
            "EDW_EXTERNAL_SOURCE_SYSTEM",
        }
        columns_to_hash = [c for c in df.columns if c not in exclude_cols and c != hash_column]
        concatenated = concat_ws("||", *[col(c).cast("string") for c in columns_to_hash]) if columns_to_hash else lit("")
        df = df.withColumn(hash_column, sha3_512_udf(concatenated))

        new_col_order = [hash_column] + [c for c in df.columns if c != hash_column]
        df = df.select(new_col_order)
        sample_values = [r[hash_column] for r in df.select(hash_column).limit(5).collect()]
        logging.info(f"Hash column {hash_column} created for table {table_name}. Samples: {sample_values}")
        return df


    @staticmethod
    def add_metadata_columns(df: DataFrame) -> DataFrame:
        for col_name in [
            "ETL_CREATED_DATE",
            "ETL_LAST_UPDATE_DATE",
            "CREATED_BY",
            "TO_PROCESS",
            "EDW_EXTERNAL_SOURCE_SYSTEM",
        ]:
            if col_name in df.columns:
                logging.warning(f"Column {col_name} already exists and will be overwritten in add_metadata_columns().")
        return (
            df.withColumn("ETL_CREATED_DATE", current_timestamp())
            .withColumn("ETL_LAST_UPDATE_DATE", current_timestamp())
            .withColumn("CREATED_BY", lit("ETL_PROCESS"))
            .withColumn("TO_PROCESS", lit(True))
            .withColumn("EDW_EXTERNAL_SOURCE_SYSTEM", lit("LeadDepot"))
        )

    @staticmethod
    def _retry() -> Dict[str, object]:
        return dict(
            stop=stop_after_attempt(3),
            wait=wait_exponential(min=1, max=8, multiplier=1),
            before_sleep=LeadDepotETL._log_retry,
            reraise=True,
        )

    def _read_sql_server_with_retry(self, jdbc_url: str, query: str) -> DataFrame:
        @retry(**self._retry())
        def _read() -> DataFrame:
            return (
                self.spark.read.format("jdbc")
                .option("url", jdbc_url)
                .option("query", query)
                .option("user", self.server_details.username)
                .option("password", self.server_details.password)
                .option("tableLock", "true")
                .load()
            )

        try:
            return _read()
        except RetryError as exc:
            logging.error("SQL Server read failed after retries")
            raise exc.last_attempt.exception()

    def _write_snowflake_with_retry(self, df: DataFrame, table: str, mode: str = "overwrite") -> None:
        @retry(**self._retry())
        def _write() -> None:
            (
                df.write.format("snowflake")
                .options(**self.snowflake_config.__dict__)
                .option("dbtable", table)
                .mode(mode)
                .save()
            )

        try:
            _write()
        except RetryError as exc:
            logging.error("Snowflake write failed after retries")
            raise exc.last_attempt.exception()

    def _read_snowflake_with_retry(self, query: str) -> DataFrame:
        @retry(**self._retry())
        def _read() -> DataFrame:
            return (
                self.spark.read.format("snowflake")
                .options(**self.snowflake_config.__dict__)
                .option("query", query)
                .load()
            )

        try:
            return _read()
        except RetryError as exc:
            logging.error("Snowflake read failed after retries")
            raise exc.last_attempt.exception()


    @log_exceptions(default=[])
    def discover_all_tables(self) -> List[str]:
        logging.info("Discovering all tables from the database...")
        query = """
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE LOWER(TABLE_SCHEMA) = 'dbo'
        """
        jdbc_url = (
            f"jdbc:sqlserver://{self.server_details.server_url};"
            "databaseName=LeadDepot;"
            "encrypt=true;"
            "trustServerCertificate=true;"
        )
        logging.info(f"JDBC URL: {jdbc_url}")
        df = self._read_sql_server_with_retry(jdbc_url, query)
        tables = [row.TABLE_NAME for row in df.collect()]
        logging.info(f"Discovered tables: {tables}")
        if not tables:
            logging.warning("No tables found in the schema 'dbo'.")
        return tables

    @log_exceptions(default=(None, False))
    def extract_table(self, table_name: str) -> Tuple[Optional[DataFrame], bool]:
        if table_name in self.EXCLUDED_TABLES:
            logging.info(f"Skipping excluded table: {table_name}")
            return None, False
        logging.info(f"Extracting data from table: {table_name}")
        jdbc_url = (
            f"jdbc:sqlserver://{self.server_details.server_url};"
            "databaseName=LeadDepot;"
            "encrypt=true;"
            "trustServerCertificate=true;"
        )
        schema_query = f"""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{table_name}'
            AND LOWER(TABLE_SCHEMA) = 'dbo'
        """
        schema_df = self._read_sql_server_with_retry(jdbc_url, schema_query)
        columns = [(row.COLUMN_NAME, row.DATA_TYPE) for row in schema_df.collect()]
        money_cols = [col for col, dtype in columns if dtype in ("money", "sql_variant")]
        if money_cols:
            logging.warning(
                f"Columns {money_cols} in table {table_name} will be replaced with NULLs due to unsupported data type."
            )
        count_query = f"SELECT COUNT(*) AS total_count FROM dbo.{table_name}"
        count_df = self._read_sql_server_with_retry(jdbc_url, count_query)
        total_count_in_source = count_df.collect()[0].total_count
        logging.info(f"Total records in source table {table_name}: {total_count_in_source}")
        select_parts = [
            f"CAST(NULL AS VARCHAR(50)) AS [{c}]" if dt in ("money", "sql_variant") else f"[{c}]"
            for c, dt in columns
        ]
        query = f"SELECT {', '.join(select_parts)} FROM dbo.{table_name}"
        logging.info(f"Extract query for {table_name}: {query}")

        df = self._read_sql_server_with_retry(jdbc_url, query)
        df = self.clean_column_names(df)
        logging.info(f"Data extracted from table: {table_name}")
        if not df.columns or df.rdd.isEmpty():
            logging.warning(f"No data found in table {table_name}. Skipping.")
            return None, False
        return df, False

    @log_exceptions()
    def write_to_adls(self, df: DataFrame, table_name: str) -> None:
        path = f"{self.ADLS_BASE_PATH}/{table_name}"
        logging.info(f"Writing data to ADLS for table: {table_name} -> {path}")
        hash_column = self.SNOWFLAKE_TABLES.get(table_name, {}).get("hash_column_name")
        if self.etl_mode == "historical" or not hash_column:
            mode = "overwrite"
        else:
            if DeltaTable.isDeltaTable(self.spark, path):
                existing = (
                    self.spark.read.format("delta").load(path).select(hash_column).distinct()
                )
                df = df.join(existing, on=hash_column, how="left_anti")
                if df.rdd.isEmpty():
                    logging.info(f"No new records to append for table: {table_name}")
                    return
                mode = "append"
            else:
                mode = "overwrite"
        logging.info(f"ADLS write mode for table {table_name}: {mode}")
        (
            df.write.format("delta")
            .option("delta.columnMapping.mode", "name")
            .option("delta.minReaderVersion", "2")
            .option("delta.minWriterVersion", "5")
            .mode(mode)
            .option("mergeSchema", "true")
            .option("overwriteSchema", "true")
            .save(path)
        )
        logging.info(f"Data written to ADLS for table: {table_name}")

    @log_exceptions()
    def load_to_snowflake(self, df: DataFrame, table_config: Dict[str, str]) -> None:
        table = table_config["staging_table_name"]
        hash_column = table_config.get("hash_column_name")
        if self.etl_mode == "historical" or not hash_column:
            mode = "overwrite"
        else:
            try:
                existing = (
                    self.spark.read.format("snowflake")
                    .options(**self.snowflake_config.__dict__)
                    .option("query", f"SELECT {hash_column} FROM {table}")
                    .load()
                )
                df = df.join(existing, on=hash_column, how="left_anti")
                if df.rdd.isEmpty():
                    logging.info(f"No new records to append for Snowflake table: {table}")
                    return
            except Exception as exc:
                logging.warning(f"Could not fetch existing hashes from Snowflake for {table}: {exc}")
            mode = "append"
        logging.info(f"Loading data into Snowflake table {table} with mode {mode}")
        self._write_snowflake_with_retry(df, table, mode=mode)
        validation_query = f"SELECT COUNT(*) FROM {table}"
        snowflake_count_df = self._read_snowflake_with_retry(validation_query)
        snowflake_record_count = snowflake_count_df.collect()[0][0]
        logging.info(f"Validation: Snowflake table {table} contains {snowflake_record_count} records after load")
        logging.info(f"Data successfully loaded into Snowflake table: {table}")

    @log_exceptions()
    def process_table(self, table_name: str) -> None:
        logging.info(f"Starting to process table: {table_name}")
        df, _ = self.extract_table(table_name)
        if df is None or df.rdd.isEmpty():
            logging.warning(f"Table {table_name} was excluded from processing or contains no data.")
            return
        df = self.add_hash_column(df, table_name)
        df = self.add_metadata_columns(df)
        self.write_to_adls(df, table_name)
        if table_name in self.SNOWFLAKE_TABLES:
            self.load_to_snowflake(df, self.SNOWFLAKE_TABLES[table_name])
        else:
            logging.info(f"Table {table_name} is not configured for Snowflake loading.")

    def run(self) -> None:
        logging.info("Starting the LeadDepot ETL process...")
        all_tables = self.discover_all_tables()
        if not all_tables:
            logging.error("No tables to process. Exiting the script.")
            sys.exit(1)
        logging.info(f"Total tables to process: {len(all_tables)}")
        qa_tables = ["LeadSFG", "RGILead", "LeadMAILCO", "LeadFBMC", "LeadLEADCO", "LeadPlum"]
        for table in qa_tables:
            if table in all_tables:
                logging.info(f"QA Note: Table {table} present in source database")
            else:
                logging.warning(f"QA Note: Table {table} NOT found in source database")
        for table_name in all_tables:
            self.process_table(table_name)
        logging.info("LeadDepot ETL process completed successfully.")
        logging.info(f"ETL run completed in '{self.etl_mode}' mode.")

@log_exceptions(exit_on_error=True)
def main() -> None:
    etl = LeadDepotETL()
    etl.run()

if __name__ == "__main__":
    main()