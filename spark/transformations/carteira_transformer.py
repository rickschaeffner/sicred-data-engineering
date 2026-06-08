from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
from spark.utils.logger import get_logger
from spark.utils.schema_definitions import CARTEIRA_FLAT_COLUMNS

logger = get_logger(__name__)


class CarteiraTransformer:
    def __init__(
        self,
        df_posicao: DataFrame,
        df_fundo: DataFrame,
        df_ativo: DataFrame,
        df_emissor: DataFrame,
        df_preco_mercado: DataFrame,
    ):
        self.df_posicao       = df_posicao
        self.df_fundo         = df_fundo
        self.df_ativo         = df_ativo
        self.df_emissor       = df_emissor
        self.df_preco_mercado = df_preco_mercado
        logger.info("CarteiraTransformer inicializado com 5 DataFrames")

    def _prepare_preco_mercado(self) -> DataFrame:
        logger.info("Deduplicando precos por fonte prioritaria")
        preco_com_prioridade = self.df_preco_mercado.withColumn(
            "prioridade_fonte",
            F.when(F.col("fonte_preco") == "ANBIMA",   F.lit(1))
             .when(F.col("fonte_preco") == "B3",        F.lit(2))
             .when(F.col("fonte_preco") == "CETIP",     F.lit(3))
             .when(F.col("fonte_preco") == "BLOOMBERG", F.lit(4))
             .when(F.col("fonte_preco") == "BACEN",     F.lit(5))
             .otherwise(F.lit(99))
        )
        window_spec = Window.partitionBy(
            "data_referencia", "id_ativo"
        ).orderBy(F.col("prioridade_fonte").asc())
        preco_ranked = preco_com_prioridade.withColumn(
            "row_num", F.row_number().over(window_spec)
        )
        preco_dedup = preco_ranked.filter(F.col("row_num") == 1).drop(
            "row_num", "prioridade_fonte"
        )
        logger.info("Deduplicacao de precos concluida")
        return preco_dedup

    def _validate_inputs(self) -> None:
        required_columns = {
            "posicao_carteira": {"id_posicao", "data_posicao", "id_fundo", "id_ativo",
                                  "quantidade", "preco_unitario", "valor_financeiro", "percentual_pl"},
            "fundo":            {"id_fundo", "cnpj_fundo", "nome_fundo", "tipo_fundo"},
            "ativo":            {"id_ativo", "codigo_ativo", "nome_ativo", "classe_ativo",
                                  "indexador", "data_vencimento", "id_emissor"},
            "emissor":          {"id_emissor", "nome_emissor", "cnpj_emissor", "setor", "rating"},
            "preco_mercado":    {"data_referencia", "id_ativo", "preco_mercado", "fonte_preco"},
        }
        dataframes = {
            "posicao_carteira": self.df_posicao,
            "fundo":            self.df_fundo,
            "ativo":            self.df_ativo,
            "emissor":          self.df_emissor,
            "preco_mercado":    self.df_preco_mercado,
        }
        for df_name, df in dataframes.items():
            actual_cols = set(df.columns)
            expected_cols = required_columns[df_name]
            missing = expected_cols - actual_cols
            if missing:
                raise ValueError(
                    f"DataFrame '{df_name}' esta faltando colunas: {missing}."
                )
        logger.info("Validacao de schemas dos DataFrames: OK")

    def transform(self) -> DataFrame:
        logger.info("Iniciando transformacao: carteira_flat")
        self._validate_inputs()
        df_preco_dedup = self._prepare_preco_mercado()

        logger.info("JOIN 1/4: posicao_carteira + fundo")
        df_fundo_broadcast = F.broadcast(self.df_fundo)
        df_com_fundo = self.df_posicao.join(df_fundo_broadcast, on="id_fundo", how="left")

        logger.info("JOIN 2/4: + ativo")
        df_ativo_broadcast = F.broadcast(self.df_ativo)
        df_com_ativo = df_com_fundo.join(df_ativo_broadcast, on="id_ativo", how="left")

        logger.info("JOIN 3/4: + emissor")
        df_emissor_broadcast = F.broadcast(self.df_emissor)
        df_com_emissor = df_com_ativo.join(df_emissor_broadcast, on="id_emissor", how="left")

        logger.info("JOIN 4/4: + preco_mercado")
        df_preco_para_join = df_preco_dedup.select(
            F.col("data_referencia"),
            F.col("id_ativo").alias("id_ativo_preco"),
            F.col("preco_mercado"),
            F.col("fonte_preco"),
        )
        df_completo = df_com_emissor.join(
            df_preco_para_join,
            on=(
                (df_com_emissor["data_posicao"] == df_preco_para_join["data_referencia"]) &
                (df_com_emissor["id_ativo"] == df_preco_para_join["id_ativo_preco"])
            ),
            how="left"
        )

        logger.info("Selecionando colunas finais")
        df_carteira_flat = df_completo.select(
            F.date_format(F.col("data_posicao"), "yyyy-MM-dd").alias("data_posicao"),
            F.col("cnpj_fundo"),
            F.col("nome_fundo"),
            F.col("tipo_fundo"),
            F.col("codigo_ativo"),
            F.col("nome_ativo"),
            F.col("classe_ativo"),
            F.coalesce(F.col("indexador"), F.lit("N/A")).alias("indexador"),
            F.coalesce(
                F.date_format(F.col("data_vencimento"), "yyyy-MM-dd"),
                F.lit("INDEFINIDO")
            ).alias("data_vencimento"),
            F.col("nome_emissor"),
            F.col("cnpj_emissor"),
            F.coalesce(F.col("setor"), F.lit("N/A")).alias("setor_emissor"),
            F.coalesce(F.col("rating"), F.lit("NR")).alias("rating_emissor"),
            F.col("quantidade").cast(StringType()),
            F.col("preco_unitario").cast(StringType()),
            F.coalesce(
                F.col("preco_mercado").cast(StringType()),
                F.lit("N/A")
            ).alias("preco_mercado"),
            F.col("valor_financeiro").cast(StringType()),
            F.col("percentual_pl").cast(StringType()),
            F.coalesce(F.col("fonte_preco"), F.lit("N/A")).alias("fonte_preco"),
        )

        df_carteira_flat = df_carteira_flat.orderBy(
            F.col("data_posicao").asc(),
            F.col("cnpj_fundo").asc(),
            F.col("codigo_ativo").asc(),
        )

        record_count = df_carteira_flat.count()
        logger.info(f"Transformacao concluida: {record_count:,} registros")
        return df_carteira_flat

    def validate_output(self, df_flat: DataFrame) -> bool:
        logger.info("Validando DataFrame de saida")
        expected = set(CARTEIRA_FLAT_COLUMNS)
        actual = set(df_flat.columns)
        if expected != actual:
            missing = expected - actual
            extra = actual - expected
            raise ValueError(
                f"Schema incorreto. Faltando: {missing}. Extras: {extra}."
            )
        critical_fields = ["data_posicao", "cnpj_fundo", "nome_fundo",
                           "codigo_ativo", "quantidade", "valor_financeiro", "percentual_pl"]
        for field in critical_fields:
            null_count = df_flat.filter(F.col(field).isNull()).count()
            if null_count > 0:
                raise ValueError(
                    f"Campo critico '{field}' possui {null_count} valores NULL."
                )
        total = df_flat.count()
        if total == 0:
            raise ValueError("DataFrame de saida esta vazio.")
        logger.info(f"Validacao OK. Total: {total:,} registros")
        return True