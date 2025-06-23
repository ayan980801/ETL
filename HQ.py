import logging
import traceback
import functools
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StringType
from pyspark.sql.functions import (
    current_timestamp,
    lit,
    col,
    concat_ws,
    udf,
)
from delta.tables import DeltaTable
import hashlib
from dataclasses import dataclass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(message)s",
    handlers=[logging.StreamHandler()],
)


@udf(StringType())
def sha3_512_udf(val: str) -> Optional[str]:
    if val is None:
        return None
    return hashlib.sha3_512(val.encode("utf-8")).hexdigest()


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
            (col_name, col_name.replace(" ", "_").replace(".", "_"))
            for col_name in columns
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

    @staticmethod
    def add_hash_column(df: DataFrame, table_name: str) -> DataFrame:
        hash_column = f"STG_HQ_{table_name.upper()}_KEY"
        exclude_cols = {
            "ETL_CREATED_DATE",
            "ETL_LAST_UPDATE_DATE",
            "CREATED_BY",
            "TO_PROCESS",
            "EDW_EXTERNAL_SOURCE_SYSTEM",
            hash_column,
        }
        cols_to_hash = [c for c in df.columns if c not in exclude_cols]
        if not cols_to_hash:
            concatenated = lit("")
        else:
            concatenated = concat_ws("||", *[col(c).cast("string") for c in cols_to_hash])
        return df.withColumn(hash_column, sha3_512_udf(concatenated))


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
        query = f"SELECT * FROM dbo.{table_name}"
        logging.info(f"Executing query for {table_name}: {query}")
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
        hash_col = f"STG_HQ_{table_name.upper()}_KEY"
        if (
            self.load_mode == "historical"
            or is_truncate
            or not DeltaTable.isDeltaTable(self.spark, path)
        ):
            df.write.format("delta").option("delta.columnMapping.mode", "name").option(
                "delta.minReaderVersion",
                "2",
            ).option("delta.minWriterVersion", "5").option(
                "mergeSchema",
                "true",
            ).option("overwriteSchema", "true").mode("overwrite").save(path)
            logging.info(f"Data overwritten in ADLS for table: {table_name}")
        else:
            existing = DeltaTable.forPath(self.spark, path).toDF().select(hash_col)
            df_filtered = df.join(existing, on=hash_col, how="left_anti")
            if df_filtered.rdd.isEmpty():
                logging.info(f"No new records to append for table: {table_name}")
                return
            df_filtered.write.format("delta").mode("append").option(
                "delta.columnMapping.mode",
                "name",
            ).option("delta.minReaderVersion", "2").option(
                "delta.minWriterVersion",
                "5",
            ).save(path)
            logging.info(
                f"Appended {df_filtered.count()} new records to ADLS for table: {table_name}"
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
        options = {
            "sfURL": self.config.sfURL,
            "sfUser": self.config.sfUser,
            "sfPassword": self.config.sfPassword,
            "sfDatabase": self.config.sfDatabase,
            "sfWarehouse": self.config.sfWarehouse,
            "sfSchema": self.config.sfSchema,
            "sfRole": self.config.sfRole,
        }
        hash_col = f"{staging_table_name.split('.')[-1]}_KEY"
        if self.load_mode == "historical" or is_truncate:
            logging.info(
                f"Loading data into Snowflake table (overwrite): {staging_table_name}"
            )
            (
                df.write.format("snowflake")
                .options(**options)
                .option("dbtable", staging_table_name)
                .mode("overwrite")
                .save()
            )
        else:
            existing_query = f"SELECT {hash_col} FROM {staging_table_name}"
            existing = (
                self.spark.read.format("snowflake")
                .options(**options)
                .option("query", existing_query)
                .load()
            )
            df_filtered = df.join(existing, on=hash_col, how="left_anti")
            if df_filtered.rdd.isEmpty():
                logging.info(
                    f"No new records to append for Snowflake table: {staging_table_name}"
                )
                return
            (
                df_filtered.write.format("snowflake")
                .options(**options)
                .option("dbtable", staging_table_name)
                .mode("append")
                .save()
            )
        logging.info(f"Data successfully loaded into Snowflake table: {staging_table_name}")


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
            is_truncate = self.load_mode == "historical"
            if is_truncate:
                logging.info(
                    f"[{table_name}] Historical mode selected: table will be overwritten."
                )
            df = self.sync.extract_table(table_name)
            if not df or df.rdd.isEmpty():
                logging.warning(f"No data found in table {table_name}. Skipping.")
                return {"table": table_name, "status": "skipped", "reason": "empty"}
            df = self.sync.add_hash_column(df, table_name)
            df = self.sync.add_metadata_columns(df)
            self.sync.write_to_adls(df, table_name, is_truncate=is_truncate)
            staging_table_name = f"DEV.QUILITY_EDW_STAGE.STG_HQ_{table_name.upper()}"
            self.loader.load_to_snowflake(
                df, staging_table_name, is_truncate=is_truncate
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