import pytest
from decimal import Decimal
from datetime import date, datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType,
    DecimalType, DateType, TimestampType
)
from spark.transformations.carteira_transformer import CarteiraTransformer
from spark.utils.schema_definitions import CARTEIRA_FLAT_COLUMNS


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("Sicred-Unit-Tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def sample_dataframes(spark):
    ts = datetime(2024, 1, 1, 0, 0, 0)

    schema_emissor = StructType([
        StructField("id_emissor",    IntegerType(), False),
        StructField("nome_emissor",  StringType(),  False),
        StructField("cnpj_emissor",  StringType(),  False),
        StructField("setor",         StringType(),  True),
        StructField("rating",        StringType(),  True),
        StructField("criado_em",     TimestampType(), False),
        StructField("atualizado_em", TimestampType(), False),
    ])
    df_emissor = spark.createDataFrame([
        (1, "Tesouro Nacional",   "00.394.460/0001-41", None,        "AAA",  ts, ts),
        (2, "Petrobras S.A.",     "33.000.167/0001-01", "Energia",   "BBB+", ts, ts),
        (3, "Empresa Sem Rating", "11.111.111/0001-11", "Industrial", None,  ts, ts),
    ], schema=schema_emissor)

    schema_ativo = StructType([
        StructField("id_ativo",         IntegerType(), False),
        StructField("codigo_ativo",     StringType(),  False),
        StructField("nome_ativo",       StringType(),  False),
        StructField("classe_ativo",     StringType(),  False),
        StructField("indexador",        StringType(),  True),
        StructField("data_vencimento",  DateType(),    True),
        StructField("id_emissor",       IntegerType(), False),
        StructField("criado_em",        TimestampType(), False),
        StructField("atualizado_em",    TimestampType(), False),
    ])
    df_ativo = spark.createDataFrame([
        (1, "NTNB-2035",      "Tesouro IPCA+ 2035", "Renda Fixa",    "IPCA", date(2035,5,15), 1, ts, ts),
        (2, "PETR4",          "Petrobras PN",        "Renda Variavel", None,  None,            2, ts, ts),
        (3, "ATIVO-SEM-PRECO","Ativo Sem Preco",     "Renda Fixa",    "CDI",  date(2026,1,1),  3, ts, ts),
    ], schema=schema_ativo)

    schema_fundo = StructType([
        StructField("id_fundo",      IntegerType(), False),
        StructField("cnpj_fundo",    StringType(),  False),
        StructField("nome_fundo",    StringType(),  False),
        StructField("tipo_fundo",    StringType(),  False),
        StructField("data_inicio",   DateType(),    False),
        StructField("status",        StringType(),  False),
        StructField("criado_em",     TimestampType(), False),
        StructField("atualizado_em", TimestampType(), False),
    ])
    df_fundo = spark.createDataFrame([
        (1, "12.345.678/0001-90", "SiCoop RF Conservador", "Renda Fixa", date(2018,3,1),  "ATIVO", ts, ts),
        (2, "23.456.789/0001-01", "SiCoop Acoes Brasil",   "Acoes",      date(2020,1,10), "ATIVO", ts, ts),
    ], schema=schema_fundo)

    schema_posicao = StructType([
        StructField("id_posicao",       IntegerType(),    False),
        StructField("data_posicao",     DateType(),       False),
        StructField("id_fundo",         IntegerType(),    False),
        StructField("id_ativo",         IntegerType(),    False),
        StructField("quantidade",       DecimalType(18,6),False),
        StructField("preco_unitario",   DecimalType(18,6),False),
        StructField("valor_financeiro", DecimalType(18,2),False),
        StructField("percentual_pl",    DecimalType(9,6), False),
        StructField("criado_em",        TimestampType(),  False),
    ])
    df_posicao = spark.createDataFrame([
        (1, date(2024,5,31), 1, 1, Decimal("5000.000000"), Decimal("3542.180000"), Decimal("17710900.00"), Decimal("35.250000"), ts),
        (2, date(2024,5,31), 2, 2, Decimal("120000.000000"), Decimal("38.250000"), Decimal("4590000.00"), Decimal("8.100000"), ts),
        (3, date(2024,5,31), 1, 3, Decimal("1000.000000"), Decimal("1000.000000"), Decimal("1000000.00"), Decimal("2.000000"), ts),
    ], schema=schema_posicao)

    schema_preco = StructType([
        StructField("id_preco",        IntegerType(),    False),
        StructField("data_referencia", DateType(),       False),
        StructField("id_ativo",        IntegerType(),    False),
        StructField("preco_mercado",   DecimalType(18,6),False),
        StructField("fonte_preco",     StringType(),     False),
        StructField("criado_em",       TimestampType(),  False),
    ])
    df_preco = spark.createDataFrame([
        (1, date(2024,5,31), 1, Decimal("3542.180000"), "ANBIMA", ts),
        (2, date(2024,5,31), 1, Decimal("3540.000000"), "B3",     ts),
        (3, date(2024,5,31), 2, Decimal("38.250000"),   "B3",     ts),
    ], schema=schema_preco)

    return {
        "posicao": df_posicao,
        "fundo":   df_fundo,
        "ativo":   df_ativo,
        "emissor": df_emissor,
        "preco":   df_preco,
    }


@pytest.fixture
def transformer(sample_dataframes):
    return CarteiraTransformer(
        df_posicao=sample_dataframes["posicao"],
        df_fundo=sample_dataframes["fundo"],
        df_ativo=sample_dataframes["ativo"],
        df_emissor=sample_dataframes["emissor"],
        df_preco_mercado=sample_dataframes["preco"],
    )


class TestSchema:
    @pytest.mark.unit
    def test_tem_todas_as_colunas(self, transformer):
        df = transformer.transform()
        assert set(CARTEIRA_FLAT_COLUMNS) == set(df.columns)

    @pytest.mark.unit
    def test_todas_colunas_sao_string(self, transformer):
        df = transformer.transform()
        nao_string = [f.name for f in df.schema.fields if not isinstance(f.dataType, StringType)]
        assert not nao_string, f"Colunas nao-String: {nao_string}"


class TestJoins:
    @pytest.mark.unit
    def test_total_registros_igual_posicao(self, transformer, sample_dataframes):
        df = transformer.transform()
        assert df.count() == sample_dataframes["posicao"].count()

    @pytest.mark.unit
    def test_dados_fundo_corretos(self, transformer):
        df = transformer.transform()
        row = df.filter(F.col("codigo_ativo") == "NTNB-2035").first()
        assert row is not None
        assert row["cnpj_fundo"] == "12.345.678/0001-90"
        assert row["nome_fundo"] == "SiCoop RF Conservador"

    @pytest.mark.unit
    def test_dados_emissor_corretos(self, transformer):
        df = transformer.transform()
        row = df.filter(F.col("codigo_ativo") == "NTNB-2035").first()
        assert row["nome_emissor"] == "Tesouro Nacional"


class TestNulos:
    @pytest.mark.unit
    def test_sem_indexador_vira_na(self, transformer):
        df = transformer.transform()
        row = df.filter(F.col("codigo_ativo") == "PETR4").first()
        assert row is not None
        assert row["indexador"] == "N/A"

    @pytest.mark.unit
    def test_sem_vencimento_vira_indefinido(self, transformer):
        df = transformer.transform()
        row = df.filter(F.col("codigo_ativo") == "PETR4").first()
        assert row["data_vencimento"] == "INDEFINIDO"

    @pytest.mark.unit
    def test_sem_setor_vira_na(self, transformer):
        df = transformer.transform()
        row = df.filter(F.col("codigo_ativo") == "NTNB-2035").first()
        assert row["setor_emissor"] == "N/A"

    @pytest.mark.unit
    def test_sem_rating_vira_nr(self, transformer):
        df = transformer.transform()
        row = df.filter(F.col("codigo_ativo") == "ATIVO-SEM-PRECO").first()
        assert row["rating_emissor"] == "NR"

    @pytest.mark.unit
    def test_sem_preco_vira_na(self, transformer):
        df = transformer.transform()
        row = df.filter(F.col("codigo_ativo") == "ATIVO-SEM-PRECO").first()
        assert row is not None, "Posicao sem preco foi perdida — verificar LEFT JOIN"
        assert row["preco_mercado"] == "N/A"


class TestPrecos:
    @pytest.mark.unit
    def test_anbima_tem_prioridade_sobre_b3(self, transformer):
        df = transformer.transform()
        row = df.filter(F.col("codigo_ativo") == "NTNB-2035").first()
        assert "3542" in row["preco_mercado"]
        assert row["fonte_preco"] == "ANBIMA"