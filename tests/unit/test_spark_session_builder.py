import pytest
from pyspark.sql import SparkSession
from spark.utils.spark_session_builder import SparkSessionBuilder


class TestSparkSessionBuilder:
    @pytest.mark.unit
    def test_cria_spark_session(self):
        spark = (
            SparkSessionBuilder("test-builder")
            .with_master("local[1]")
            .with_shuffle_partitions(1)
            .build()
        )
        try:
            assert isinstance(spark, SparkSession)
            assert spark.version is not None
        finally:
            spark.stop()

    @pytest.mark.unit
    def test_nome_app_correto(self):
        nome = "test-app-verificacao"
        spark = (
            SparkSessionBuilder(nome)
            .with_master("local[1]")
            .with_shuffle_partitions(1)
            .build()
        )
        try:
            assert spark.sparkContext.appName == nome
        finally:
            spark.stop()

    @pytest.mark.unit
    def test_shuffle_partitions_configurado(self):
        spark = (
            SparkSessionBuilder("test-shuffle")
            .with_master("local[1]")
            .with_shuffle_partitions(4)
            .build()
        )
        try:
            assert spark.conf.get("spark.sql.shuffle.partitions") == "4"
        finally:
            spark.stop()