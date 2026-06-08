import argparse
import sys
import os
from datetime import datetime

from spark.utils.spark_session_builder import build_default_session
from spark.utils.jdbc_reader import JDBCReader
from spark.utils.schema_definitions import (
    SCHEMA_POSICAO_CARTEIRA,
    SCHEMA_FUNDO,
    SCHEMA_ATIVO,
    SCHEMA_EMISSOR,
    SCHEMA_PRECO_MERCADO,
)
from spark.transformations.carteira_transformer import CarteiraTransformer
from spark.utils.logger import ETLLogger

logger = ETLLogger("carteira_flat_etl")


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="ETL Sicred: gera carteira_flat CSV"
    )
    parser.add_argument("--pg-host",      default=os.getenv("PG_HOST",     "localhost"))
    parser.add_argument("--pg-port",      default=int(os.getenv("PG_PORT", "5432")), type=int)
    parser.add_argument("--pg-database",  default=os.getenv("PG_DATABASE", "sicred"))
    parser.add_argument("--pg-schema",    default=os.getenv("PG_SCHEMA",   "sicred"))
    parser.add_argument("--pg-user",      default=os.getenv("PG_USER",     "sicred_user"))
    parser.add_argument("--pg-password",  default=os.getenv("PG_PASSWORD", ""))
    parser.add_argument("--spark-master", default=os.getenv("SPARK_MASTER","local[*]"))
    parser.add_argument("--jdbc-jar",     default=os.getenv("JDBC_JAR",    "/opt/spark/jars/postgresql-42.7.3.jar"))
    parser.add_argument("--output-dir",   default=os.getenv("OUTPUT_DIR",  "/data/output"))
    parser.add_argument("--output-filename", default="carteira_flat")
    parser.add_argument("--append-timestamp", action="store_true")
    return parser.parse_args()


def build_output_path(args: argparse.Namespace) -> str:
    filename = args.output_filename
    if args.append_timestamp:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{filename}_{timestamp}"
    return os.path.join(args.output_dir, filename)


def run_etl(args: argparse.Namespace) -> int:
    logger.info("=" * 60)
    logger.info("Sicred Asset Management - ETL Carteira Flat")
    logger.info("=" * 60)
    logger.info(f"Host PostgreSQL: {args.pg_host}:{args.pg_port}/{args.pg_database}")
    logger.info(f"Output: {args.output_dir}")

    spark = None
    try:
        logger.start_phase("INICIALIZACAO")
        spark = build_default_session(
            app_name="Sicred-ETL-CarteiraFlat",
            jdbc_jar_path=args.jdbc_jar,
            master=args.spark_master,
        )
        jdbc_reader = JDBCReader(
            spark=spark,
            host=args.pg_host,
            port=args.pg_port,
            database=args.pg_database,
            schema=args.pg_schema,
            user=args.pg_user,
            password=args.pg_password,
        )
        logger.end_phase("INICIALIZACAO")

        logger.start_phase("EXTRACT")
        df_posicao      = jdbc_reader.read_table("posicao_carteira", SCHEMA_POSICAO_CARTEIRA)
        logger.log_extraction("posicao_carteira", df_posicao.count())
        df_fundo        = jdbc_reader.read_table("fundo",            SCHEMA_FUNDO)
        logger.log_extraction("fundo", df_fundo.count())
        df_ativo        = jdbc_reader.read_table("ativo",            SCHEMA_ATIVO)
        logger.log_extraction("ativo", df_ativo.count())
        df_emissor      = jdbc_reader.read_table("emissor",          SCHEMA_EMISSOR)
        logger.log_extraction("emissor", df_emissor.count())
        df_preco_mercado = jdbc_reader.read_table("preco_mercado",   SCHEMA_PRECO_MERCADO)
        logger.log_extraction("preco_mercado", df_preco_mercado.count())
        logger.end_phase("EXTRACT")

        logger.start_phase("TRANSFORM")
        transformer = CarteiraTransformer(
            df_posicao=df_posicao,
            df_fundo=df_fundo,
            df_ativo=df_ativo,
            df_emissor=df_emissor,
            df_preco_mercado=df_preco_mercado,
        )
        df_carteira_flat = transformer.transform()
        logger.log_transform("carteira_flat", df_carteira_flat.count())
        logger.end_phase("TRANSFORM")

        logger.start_phase("VALIDATE")
        transformer.validate_output(df_carteira_flat)
        logger.end_phase("VALIDATE")

        logger.start_phase("LOAD")
        output_path = build_output_path(args)
        os.makedirs(args.output_dir, exist_ok=True)
        final_count = df_carteira_flat.count()
        (
            df_carteira_flat
            .coalesce(1)
            .write
            .mode("overwrite")
            .option("header", "true")
            .option("encoding", "UTF-8")
            .option("sep", ",")
            .option("nullValue", "")
            .option("dateFormat", "yyyy-MM-dd")
            .csv(output_path)
        )
        logger.log_load(output_path, final_count, "CSV")
        logger.end_phase("LOAD", records=final_count)

        logger.info("=" * 60)
        logger.info("ETL CONCLUIDO COM SUCESSO")
        logger.info(f"Arquivo: {output_path}")
        logger.info(f"Total de registros: {final_count:,}")
        logger.info("=" * 60)
        return 0

    except Exception as e:
        logger.log_error("ETL_PRINCIPAL", e)
        logger.error("ETL FALHOU")
        return 1
    finally:
        if spark is not None:
            logger.info("Encerrando SparkSession")
            spark.stop()


def main():
    args = parse_arguments()
    exit_code = run_etl(args)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()