from pyspark.sql import SparkSession
from spark.utils.logger import get_logger

logger = get_logger(__name__)


class SparkSessionBuilder:
    def __init__(self, app_name: str):
        self.app_name = app_name
        self._config: dict = {}
        self._master: str = "local[*]"

    def with_master(self, master: str) -> "SparkSessionBuilder":
        self._master = master
        return self

    def with_executor_memory(self, memory: str) -> "SparkSessionBuilder":
        self._config["spark.executor.memory"] = memory
        return self

    def with_driver_memory(self, memory: str) -> "SparkSessionBuilder":
        self._config["spark.driver.memory"] = memory
        return self

    def with_shuffle_partitions(self, partitions: int) -> "SparkSessionBuilder":
        self._config["spark.sql.shuffle.partitions"] = str(partitions)
        return self

    def with_jdbc_jar(self, jar_path: str) -> "SparkSessionBuilder":
        self._config["spark.jars"] = jar_path
        return self

    def with_log_level(self, level: str) -> "SparkSessionBuilder":
        self._config["spark.log.level"] = level
        return self

    def with_adaptive_query_execution(self, enabled: bool = True) -> "SparkSessionBuilder":
        self._config["spark.sql.adaptive.enabled"] = str(enabled).lower()
        self._config["spark.sql.adaptive.coalescePartitions.enabled"] = str(enabled).lower()
        return self

    def build(self) -> SparkSession:
        logger.info(f"Criando SparkSession: app={self.app_name}, master={self._master}")
        builder = SparkSession.builder \
            .appName(self.app_name) \
            .master(self._master)
        for key, value in self._config.items():
            builder = builder.config(key, value)
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel(
            self._config.get("spark.log.level", "WARN")
        )
        logger.info(f"SparkSession criada. Version: {spark.version}")
        return spark


def build_default_session(app_name: str, jdbc_jar_path: str, master: str = "local[*]") -> SparkSession:
    return (
        SparkSessionBuilder(app_name)
        .with_master(master)
        .with_executor_memory("2g")
        .with_driver_memory("1g")
        .with_shuffle_partitions(8)
        .with_jdbc_jar(jdbc_jar_path)
        .with_log_level("WARN")
        .with_adaptive_query_execution(enabled=True)
        .build()
    )