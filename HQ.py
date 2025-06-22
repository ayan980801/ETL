import logging
import traceback
import functools
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from pyspark.sql.functions import current_timestamp, lit, col, max as spark_max
from dataclasses import dataclass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(message)s",
    handlers=[logging.StreamHandler()],
)


def log_exceptions(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logging.error(f"Exception in {func.__qualname__}: {e}")
            logging.error(traceback.format_exc())
            if func.__name__ == "extract_table":
                self = args[0]
                return self.spark.createDataFrame(
                    self.spark.sparkContext.emptyRDD(), StructType([])
                )
            return None

    return wrapper


@dataclass(frozen=True)
class ServerConfig:
    host: str
    port: str
    database: str
    username: str
    password: str


@dataclass(frozen=True)
class AzureConfig:
    base_path: str
    stage: str


@dataclass(frozen=True)
class SnowflakeConfig:
    sfURL: str
    sfUser: str
    sfPassword: str
    sfDatabase: str
    sfWarehouse: str
    sfSchema: str
    sfRole: str  # Explicitly add role


class TableDiscovery:
    def __init__(self, spark: SparkSession, config: ServerConfig):
        self.spark = spark
        self.config = config

    @log_exceptions
    def discover_all_tables(self) -> List[str]:
        logging.info("Discovering all tables from the database...")
        query = """
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = 'dbo'
        """
        jdbc_url = (
            f"jdbc:sqlserver://{self.config.host}:{self.config.port};"
            f"database={self.config.database};"
            "encrypt=false;"
            "trustServerCertificate=false;"
            "loginTimeout=30;"
            "driver=com.microsoft.sqlserver.jdbc.SQLServerDriver"
        )
        df = (
            self.spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("query", query)
            .option("user", self.config.username)
            .option("password", self.config.password)
            .option("encrypt", "false")
            .option("trustServerCertificate", "false")
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
            .load()
        )
        tables = [row.TABLE_NAME for row in df.collect()]
        logging.info(f"Discovered tables: {tables}")
        if not tables:
            logging.warning("No tables found in the schema 'dbo'.")
        return tables


class DataSync:
    def __init__(
        self,
        spark: SparkSession,
        server_cfg: ServerConfig,
        azure_cfg: AzureConfig,
        load_mode: str = "historical",
    ):
        self.spark = spark
        self.server_cfg = server_cfg
        self.azure_cfg = azure_cfg
        self.load_mode = load_mode
        self.metadata_base_path = (
            "dbfs:/FileStore/DataProduct/DataArchitecture/Pipelines/HQ_QLT/Metadata"
        )

    @staticmethod
    def clean_column_names(df: DataFrame) -> DataFrame:
        columns = df.columns
        for old_col, new_col in (
            (col, col.replace(" ", "_").replace(".", "_")) for col in columns
        ):
            if old_col != new_col:
                df = df.withColumnRenamed(old_col, new_col)
        return df

    @staticmethod
    def add_metadata_columns(df: DataFrame) -> DataFrame:
        return (
            df.withColumn("ETL_CREATED_DATE", current_timestamp())
            .withColumn("ETL_LAST_UPDATE_DATE", current_timestamp())
            .withColumn("CREATED_BY", lit("ETL_PROCESS"))
            .withColumn("TO_PROCESS", lit(True))
            .withColumn("EDW_EXTERNAL_SOURCE_SYSTEM", lit("HQ"))
        )

    def _get_watermark_path(self, table_name: str) -> str:
        return f"{self.metadata_base_path}/etl_last_update_{table_name}.txt"

    def read_watermark(self, table_name: str) -> Optional[str]:
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

    def write_watermark(self, table_name: str, value: str):
        path = self._get_watermark_path(table_name)
        self.spark.createDataFrame([(value,)], ["watermark"]).coalesce(1).write.mode(
            "overwrite"
        ).text(path)
        logging.info(f"Watermark for {table_name} written to {path}: {value}")

    @log_exceptions
    def extract_table(self, table_name: str) -> DataFrame:
        logging.info(f"Extracting data from table: {table_name}")
        jdbc_url = (
            f"jdbc:sqlserver://{self.server_cfg.host}:{self.server_cfg.port};"
            f"database={self.server_cfg.database};"
            "encrypt=false;"
            "trustServerCertificate=false;"
            "loginTimeout=30;"
            "driver=com.microsoft.sqlserver.jdbc.SQLServerDriver"
        )
        etl_col = "ETL_LAST_UPDATE_DATE"
        query = f"SELECT * FROM dbo.{table_name}"

        if self.load_mode == "delta":
            watermark = self.read_watermark(table_name)
            if watermark:
                query = (
                    f"SELECT * FROM dbo.{table_name} "
                    f"WHERE [{etl_col}] > '{watermark}'"
                )
                logging.info(f"Delta extract for {table_name} using query: {query}")
            else:
                logging.info(
                    f"No watermark found for {table_name}, performing full extract."
                )
        df = (
            self.spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("query", query)
            .option("user", self.server_cfg.username)
            .option("password", self.server_cfg.password)
            .option("encrypt", "false")
            .option("trustServerCertificate", "false")
            .option("fetchsize", 10000)
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
            .load()
        )
        df = self.clean_column_names(df)
        record_count = df.count()
        logging.info(f"Extracted {record_count} records from table: {table_name}")
        return df

    @log_exceptions
    def write_to_adls(self, df: DataFrame, table_name: str, is_truncate: bool = False):
        logging.info(f"Writing data to ADLS for table: {table_name}")
        path = f"{self.azure_cfg.base_path}/{self.azure_cfg.stage}/HQ/{table_name}"
        write_mode = (
            "overwrite" if self.load_mode == "historical" or is_truncate else "append"
        )
        logging.info(f"ADLS Path: {path}, write_mode: {write_mode}")
        (
            df.write.format("delta")
            .option("delta.columnMapping.mode", "name")
            .option("delta.minReaderVersion", "2")
            .option("delta.minWriterVersion", "5")
            .mode(write_mode)
            .save(path)
        )
        logging.info(
            f"Data successfully written to ADLS for table: {table_name} in {write_mode} mode."
        )


class SnowflakeLoader:
    def __init__(
        self, spark: SparkSession, config: SnowflakeConfig, load_mode="historical"
    ):
        self.spark = spark
        self.config = config
        self.load_mode = load_mode

    @log_exceptions
    def load_to_snowflake(
        self, df: DataFrame, staging_table_name: str, is_truncate: bool = False
    ):
        write_mode = (
            "overwrite" if self.load_mode == "historical" or is_truncate else "append"
        )
        logging.info(
            f"Loading data into Snowflake table: {staging_table_name} using mode {write_mode}"
        )
        options = {
            "sfURL": self.config.sfURL,
            "sfUser": self.config.sfUser,
            "sfPassword": self.config.sfPassword,
            "sfDatabase": self.config.sfDatabase,
            "sfWarehouse": self.config.sfWarehouse,
            "sfSchema": self.config.sfSchema,
            "sfRole": self.config.sfRole,
        }
        (
            df.write.format("snowflake")
            .options(**options)
            .option("dbtable", staging_table_name)
            .mode(write_mode)
            .save()
        )
        logging.info(
            f"Data successfully loaded into Snowflake: {staging_table_name} with {write_mode} mode."
        )


class ETLPipeline:
    def __init__(self, spark: SparkSession, dbutils, load_mode: str = "historical"):
        server_cfg = ServerConfig(
            host="HQ-SQL-DEV.internal.quility.com",
            port="1435",
            database="HQ_QLT_DEV",
            username=dbutils.secrets.get(
                scope="dba-key-vault-secret", key="HQ-SQL-DEV-username"
            ),
            password=dbutils.secrets.get(
                scope="dba-key-vault-secret", key="HQ-SQL-DEV-password"
            ),
        )
        azure_cfg = AzureConfig(
            base_path="abfss://dataarchitecture@quilitydatabricks.dfs.core.windows.net",
            stage="RAW",
        )
        snowflake_cfg = SnowflakeConfig(
            sfURL="hmkovlx-nu26765.snowflakecomputing.com",
            sfUser=dbutils.secrets.get(
                scope="key-vault-secret", key="DataProduct-SF-EDW-User"
            ),
            sfPassword=dbutils.secrets.get(
                scope="key-vault-secret", key="DataProduct-SF-EDW-Pass"
            ),
            sfDatabase="DEV",
            sfWarehouse="INTEGRATION_COMPUTE_WH",
            sfSchema="QUILITY_EDW_STAGE",
            sfRole="SG-SNOWFLAKE-DEVELOPERS",
        )
        self.spark = spark
        self.load_mode = load_mode
        self.discovery = TableDiscovery(spark, server_cfg)
        self.sync = DataSync(spark, server_cfg, azure_cfg, load_mode)
        self.loader = SnowflakeLoader(spark, snowflake_cfg, load_mode)

    def _process_table(self, table_name: str):
        try:
            logging.info(f"Processing table: {table_name}")
            is_truncate = False
            watermark = None

            if self.load_mode == "delta":
                watermark = self.sync.read_watermark(table_name)
                if not watermark:
                    is_truncate = True
                    logging.info(
                        f"[{table_name}] No watermark found, will perform truncate and load (first incremental run)."
                    )
                else:
                    logging.info(
                        f"[{table_name}] Watermark found: {watermark}, will perform delta load (append only new rows)."
                    )
            elif self.load_mode == "historical":
                is_truncate = True
                logging.info(
                    f"[{table_name}] Historical mode selected: will perform full truncate and load."
                )
            df = self.sync.extract_table(table_name)
            if not df or df.rdd.isEmpty():
                logging.warning(f"No data found in table {table_name}. Skipping.")
                return {"table": table_name, "status": "skipped", "reason": "empty"}
            df = self.sync.add_metadata_columns(df)
            self.sync.write_to_adls(df, table_name, is_truncate=is_truncate)
            staging_table_name = f"DEV.QUILITY_EDW_STAGE.STG_HQ_{table_name.upper()}"
            self.loader.load_to_snowflake(
                df, staging_table_name, is_truncate=is_truncate
            )

            etl_col = "ETL_LAST_UPDATE_DATE"
            if etl_col in df.columns:
                max_val = df.agg(spark_max(col(etl_col))).collect()[0][0]
                if max_val:
                    self.sync.write_watermark(table_name, str(max_val))
                else:
                    logging.warning(
                        f"No max value found for {etl_col} in {table_name}. Watermark not updated."
                    )
            else:
                logging.error(
                    f"{etl_col} not present in data for {table_name}; cannot update watermark."
                )
            return {"table": table_name, "status": "success"}
        except Exception as e:
            logging.error(f"Exception processing table {table_name}: {e}")
            logging.error(traceback.format_exc())
            return {"table": table_name, "status": "failed", "reason": str(e)}

    def run(self, max_workers: int = 32):
        tables = self.discovery.discover_all_tables()
        if not tables:
            logging.error("No tables to process. Exiting.")
            return
        logging.info(f"Total tables to process: {len(tables)}")
        completed, failed, skipped = [], [], []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self._process_table, table): table for table in tables
            }
            for future in as_completed(futures):
                table = futures[future]
                try:
                    result = future.result()
                    if result is None:
                        failed.append(table)
                    elif result.get("status") == "success":
                        completed.append(table)
                    elif result.get("status") == "skipped":
                        skipped.append(table)
                        logging.warning(
                            f"Table {table} skipped: {result.get('reason', '')}"
                        )
                    else:
                        failed.append(table)
                        logging.error(
                            f"Table {table} failed: {result.get('reason', '')}"
                        )
                except Exception as exc:
                    failed.append(table)
                    logging.error(f"Table {table} generated an exception: {exc}")
        logging.info(
            f"ETL run complete. Success: {len(completed)} tables. Skipped: {len(skipped)}. Failed: {len(failed)}."
        )
        if failed:
            logging.error(f"Failed tables: {failed}")
        else:
            logging.info("HQ_QLT ETL process completed successfully with no failures.")


def main(load_mode="historical"):
    logging.info(f"Starting the HQ_QLT ETL process in {load_mode} mode...")
    spark = (
        SparkSession.builder.appName("HQ_QLT_ETL")
        .config(
            "spark.jars.packages",
            "com.microsoft.sqlserver:mssql-jdbc:9.4.1.jre8,"
            "net.snowflake:snowflake-jdbc:3.13.8,"
            "net.snowflake:spark-snowflake_2.12:2.9.3",
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.databricks.delta.properties.defaults.columnMapping.mode", "name")
        .getOrCreate()
    )
    try:
        from pyspark.dbutils import DBUtils

        dbutils = DBUtils(spark)
    except Exception:
        import IPython

        dbutils = IPython.get_ipython().user_ns["dbutils"]
    pipeline = ETLPipeline(spark, dbutils, load_mode=load_mode)
    pipeline.run(max_workers=32)


if __name__ == "__main__":
    main(load_mode="historical")