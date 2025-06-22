# Databricks notebook source
from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
import logging
from functools import wraps
import re
import sys
import traceback
import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    current_timestamp,
    lit,
    sha2,
    concat_ws,
    col,
)
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
        if not columns_to_hash:
            logging.warning(f"No columns available for hashing in table {table_name}.")
            df = df.withColumn(hash_column, sha2(lit(""), 512))  # << SHA-512 here
        else:
            df = df.withColumn(hash_column, sha2(concat_ws("||", *[col(c).cast("string") for c in columns_to_hash]), 512))  # << SHA-512 here

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

    def _get_watermark_path(self, table_name: str) -> str:
        return (
            f"{self.METADATA_BASE_PATH}/etl_last_update_{table_name}.txt"
        )

    def _read_watermark(self, table_name: str) -> Optional[str]:
        path = self._get_watermark_path(table_name)
        try:
            df = self.spark.read.text(path)
            watermark = df.first()[0].strip()
            logging.info(f"Read watermark for {table_name}: {watermark}")
            return watermark
        except Exception as ex:
            logging.info(
                f"No watermark found for {table_name} at {path} (likely first run): {ex}"
            )
            return None

    def _write_watermark(self, table_name: str, value: str):
        path = self._get_watermark_path(table_name)
        self.spark.createDataFrame([(value,)], ["watermark"]).coalesce(1).write.mode(
            "overwrite"
        ).text(path)
        logging.info(f"Watermark for {table_name} written to {path}: {value}")

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
        etl_col = "ETL_LAST_UPDATE_DATE"
        watermark = None
        full_sync_needed = False
        if self.etl_mode == "incremental":
            watermark = self._read_watermark(table_name)
            if not watermark:
                logging.info(f"No watermark found for {table_name}; full sync will be performed (truncate and load all data).")
                full_sync_needed = True
        elif self.etl_mode == "historical":
            full_sync_needed = True

        query = f"SELECT {', '.join(select_parts)} FROM dbo.{table_name}"
        if self.etl_mode == "incremental" and watermark and not full_sync_needed:
            query += f" WHERE [{etl_col}] > '{watermark}'"
            logging.info(f"Delta extract for {table_name} using query: {query}")
        else:
            logging.info(f"Full extract for {table_name} using query: {query}")

        df = self._read_sql_server_with_retry(jdbc_url, query)
        df = self.clean_column_names(df)
        logging.info(f"Data extracted from table: {table_name}")

        if not df.columns or df.rdd.isEmpty():
            logging.warning(f"No data found in table {table_name}. Skipping.")
            return None, full_sync_needed
        return df, full_sync_needed

    @log_exceptions()
    def write_to_adls(self, df: DataFrame, table_name: str, full_sync_needed: bool = False) -> None:
        path = f"{self.ADLS_BASE_PATH}/{table_name}"
        logging.info(f"Writing data to ADLS for table: {table_name} -> {path}")
        hash_column = self.SNOWFLAKE_TABLES.get(table_name, {}).get("hash_column_name")
        logging.info(f"ETL_MODE is {self.etl_mode}, full_sync_needed={full_sync_needed}")
        if df.rdd.isEmpty():
            logging.warning(
                f"Source DataFrame for table {table_name} is empty. Full sync will delete all target records if they exist."
            )
            if self.etl_mode == "incremental" and hash_column and DeltaTable.isDeltaTable(self.spark, path):
                delta_table = DeltaTable.forPath(self.spark, path)
                delta_table.delete("true")
                logging.info(f"All records deleted from ADLS Delta table for table: {table_name}")
            else:
                logging.warning(
                    f"No delete performed for table {table_name} (table may not exist or ETL_MODE is historical)."
                )
            return

        if self.etl_mode == "historical" or full_sync_needed or not hash_column:
            df.write.format("delta").option("delta.columnMapping.mode", "name").option(
                "delta.minReaderVersion", "2"
            ).option("delta.minWriterVersion", "5").option("mergeSchema", "true").option(
                "overwriteSchema", "true"
            ).mode("overwrite").save(path)
            logging.info(f"Data overwritten in ADLS for table: {table_name}")
        else:
            if DeltaTable.isDeltaTable(self.spark, path):
                delta_table = DeltaTable.forPath(self.spark, path)
                merge_condition = f"source.{hash_column} = target.{hash_column}"
                delta_table.alias("target").merge(
                    source=df.alias("source"), condition=merge_condition
                ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
                temp_view = f"source_hashes_{table_name}"
                df.select(hash_column).distinct().createOrReplaceTempView(temp_view)
                delete_condition = f"{hash_column} NOT IN (SELECT {hash_column} FROM {temp_view})"
                delta_table.delete(delete_condition)
                logging.info(f"Delta merge and delete completed for table: {table_name}")
            else:
                df.write.format("delta").option("delta.columnMapping.mode", "name").option(
                    "delta.minReaderVersion", "2"
                ).option("delta.minWriterVersion", "5").option("mergeSchema", "true").option(
                    "overwriteSchema", "true"
                ).mode("overwrite").save(path)
                logging.info(f"Initial Delta table created for table: {table_name}")

    @log_exceptions()
    def load_to_snowflake(self, df: DataFrame, table_config: Dict[str, str], full_sync_needed: bool = False) -> None:
        table = table_config["staging_table_name"]
        hash_column = table_config.get("hash_column_name")
        logging.info(f"ETL_MODE is {self.etl_mode}, full_sync_needed={full_sync_needed}")
        if self.etl_mode == "historical" or full_sync_needed or not hash_column:
            logging.info(f"Loading data into Snowflake table (overwrite): {table}")
            self._write_snowflake_with_retry(df, table, mode="overwrite")
        else:
            temp_stage_table = table + "_STAGE"
            logging.info(f"Loading data into temp Snowflake table: {temp_stage_table}")
            self._write_snowflake_with_retry(df, temp_stage_table, mode="overwrite")
            set_clause = ", ".join([f"{c} = source.{c}" for c in df.columns])
            insert_cols = ", ".join(df.columns)
            insert_vals = ", ".join([f"source.{c}" for c in df.columns])
            merge_sql = f"""
                MERGE INTO {table} AS target
                USING {temp_stage_table} AS source
                ON target.{hash_column} = source.{hash_column}
                WHEN MATCHED THEN UPDATE SET {set_clause}
                WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals});
            """
            logging.info("Executing Snowflake MERGE statement")
            self.spark._sc._jvm.net.snowflake.spark.snowflake.Utils.runQuery(self.snowflake_config.__dict__, merge_sql)
            delete_sql = f"DELETE FROM {table} WHERE {hash_column} NOT IN (SELECT {hash_column} FROM {temp_stage_table})"
            logging.info("Executing Snowflake DELETE statement for full sync")
            self.spark._sc._jvm.net.snowflake.spark.snowflake.Utils.runQuery(self.snowflake_config.__dict__, delete_sql)
            drop_sql = f"DROP TABLE IF EXISTS {temp_stage_table}"
            self.spark._sc._jvm.net.snowflake.spark.snowflake.Utils.runQuery(self.snowflake_config.__dict__, drop_sql)
        validation_query = f"SELECT COUNT(*) FROM {table}"
        snowflake_count_df = self._read_snowflake_with_retry(validation_query)
        snowflake_record_count = snowflake_count_df.collect()[0][0]
        logging.info(f"Validation: Snowflake table {table} contains {snowflake_record_count} records after load")
        logging.info(f"Data successfully loaded into Snowflake table: {table}")

    @log_exceptions()
    def process_table(self, table_name: str) -> None:
        logging.info(f"Starting to process table: {table_name}")
        df, full_sync_needed = self.extract_table(table_name)
        if df is None or df.rdd.isEmpty():
            logging.warning(f"Table {table_name} was excluded from processing or contains no data.")
            return
        df = self.add_hash_column(df, table_name)
        df = self.add_metadata_columns(df)
        self.write_to_adls(df, table_name, full_sync_needed=full_sync_needed)
        if table_name in self.SNOWFLAKE_TABLES:
            self.load_to_snowflake(df, self.SNOWFLAKE_TABLES[table_name], full_sync_needed=full_sync_needed)
        else:
            logging.info(f"Table {table_name} is not configured for Snowflake loading.")

        etl_col = "ETL_LAST_UPDATE_DATE"
        if etl_col in df.columns:
            max_val = df.agg({etl_col: "max"}).collect()[0][0]
            if max_val:
                self._write_watermark(table_name, str(max_val))
            else:
                logging.warning(f"No max value for {etl_col} in {table_name}. Watermark not updated.")
        else:
            logging.error(f"{etl_col} not present in data for {table_name}; cannot update watermark.")

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