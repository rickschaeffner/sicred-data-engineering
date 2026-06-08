from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, DecimalType, DateType, TimestampType
)

SCHEMA_EMISSOR = StructType([
    StructField("id_emissor",       IntegerType(),   nullable=False),
    StructField("nome_emissor",     StringType(),    nullable=False),
    StructField("cnpj_emissor",     StringType(),    nullable=False),
    StructField("setor",            StringType(),    nullable=True),
    StructField("rating",           StringType(),    nullable=True),
    StructField("criado_em",        TimestampType(), nullable=False),
    StructField("atualizado_em",    TimestampType(), nullable=False),
])

SCHEMA_ATIVO = StructType([
    StructField("id_ativo",         IntegerType(),   nullable=False),
    StructField("codigo_ativo",     StringType(),    nullable=False),
    StructField("nome_ativo",       StringType(),    nullable=False),
    StructField("classe_ativo",     StringType(),    nullable=False),
    StructField("indexador",        StringType(),    nullable=True),
    StructField("data_vencimento",  DateType(),      nullable=True),
    StructField("id_emissor",       IntegerType(),   nullable=False),
    StructField("criado_em",        TimestampType(), nullable=False),
    StructField("atualizado_em",    TimestampType(), nullable=False),
])

SCHEMA_FUNDO = StructType([
    StructField("id_fundo",         IntegerType(),   nullable=False),
    StructField("cnpj_fundo",       StringType(),    nullable=False),
    StructField("nome_fundo",       StringType(),    nullable=False),
    StructField("tipo_fundo",       StringType(),    nullable=False),
    StructField("data_inicio",      DateType(),      nullable=False),
    StructField("status",           StringType(),    nullable=False),
    StructField("criado_em",        TimestampType(), nullable=False),
    StructField("atualizado_em",    TimestampType(), nullable=False),
])

SCHEMA_POSICAO_CARTEIRA = StructType([
    StructField("id_posicao",       IntegerType(),    nullable=False),
    StructField("data_posicao",     DateType(),       nullable=False),
    StructField("id_fundo",         IntegerType(),    nullable=False),
    StructField("id_ativo",         IntegerType(),    nullable=False),
    StructField("quantidade",       DecimalType(18,6),nullable=False),
    StructField("preco_unitario",   DecimalType(18,6),nullable=False),
    StructField("valor_financeiro", DecimalType(18,2),nullable=False),
    StructField("percentual_pl",    DecimalType(9,6), nullable=False),
    StructField("criado_em",        TimestampType(),  nullable=False),
])

SCHEMA_OPERACAO = StructType([
    StructField("id_operacao",      IntegerType(),    nullable=False),
    StructField("data_operacao",    DateType(),       nullable=False),
    StructField("id_fundo",         IntegerType(),    nullable=False),
    StructField("id_ativo",         IntegerType(),    nullable=False),
    StructField("tipo_operacao",    StringType(),     nullable=False),
    StructField("quantidade",       DecimalType(18,6),nullable=False),
    StructField("preco_operacao",   DecimalType(18,6),nullable=False),
    StructField("valor_operacao",   DecimalType(18,2),nullable=False),
    StructField("criado_em",        TimestampType(),  nullable=False),
])

SCHEMA_PRECO_MERCADO = StructType([
    StructField("id_preco",         IntegerType(),    nullable=False),
    StructField("data_referencia",  DateType(),       nullable=False),
    StructField("id_ativo",         IntegerType(),    nullable=False),
    StructField("preco_mercado",    DecimalType(18,6),nullable=False),
    StructField("fonte_preco",      StringType(),     nullable=False),
    StructField("criado_em",        TimestampType(),  nullable=False),
])

CARTEIRA_FLAT_COLUMNS = [
    "data_posicao",
    "cnpj_fundo",
    "nome_fundo",
    "tipo_fundo",
    "codigo_ativo",
    "nome_ativo",
    "classe_ativo",
    "indexador",
    "data_vencimento",
    "nome_emissor",
    "cnpj_emissor",
    "setor_emissor",
    "rating_emissor",
    "quantidade",
    "preco_unitario",
    "preco_mercado",
    "valor_financeiro",
    "percentual_pl",
    "fonte_preco",
]