from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from spark.utils.logger import get_logger

logger = get_logger(__name__)


class JDBCReader:
    def __init__(
        self,
        spark: SparkSession,
        host: str,
        port: int,
        database: str,
        schema: str,
        user: str,
        password: str
    ):
        self.spark = spark
        self.schema_db = schema
        self.jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"
        self.connection_properties = {
            "user": user,
            "password": password,
            "driver": "org.postgresql.Driver",
            "currentSchema": schema,
            "socketTimeout": "60",
            "connectTimeout": "30",
            "ApplicationName": "SiCooperative-ETL-Spark",
        }
        logger.info(
            f"JDBCReader inicializado: {host}:{port}/{database} schema={schema}"
        )

    def read_table(
        self,
        table_name: str,
        schema: Optional[StructType] = None,
        fetch_size: int = 1000,
    ) -> DataFrame:
        full_table = f"{self.schema_db}.{table_name}"
        props = {
            **self.connection_properties,
            "fetchsize": str(fetch_size),
        }
        logger.info(f"Lendo tabela: {full_table}")
        try:
            df = self.spark.read.jdbc(
                url=self.jdbc_url,
                table=full_table,
                properties=props,
            )
            if schema is not None:
                df = df.select([f.name for f in schema.fields])
            count = df.count()
            logger.info(f"Tabela {full_table} lida: {count:,} registros")
            return df
        except Exception as e:
            logger.error(f"Falha ao ler {full_table}: {type(e).__name__}: {str(e)}")
            raise