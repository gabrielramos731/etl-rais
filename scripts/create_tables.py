import psycopg2

# Conecte diretamente no banco que já existe
conn = psycopg2.connect(
    host="localhost",
    database="dw_rais2",  # banco já criado no DBeaver
    user="postgres",
    password="230204",
    port="5433"
)

cur = conn.cursor()

# Criação das tabelas
cur.execute("""
    CREATE TABLE IF NOT EXISTS dim_ano (
        ano INTEGER PRIMARY KEY
    );

    CREATE TABLE IF NOT EXISTS dim_secao (
        secao VARCHAR(50) PRIMARY KEY
    );

    CREATE TABLE IF NOT EXISTS dim_divisao (
        divisao VARCHAR(50) PRIMARY KEY,
        secao VARCHAR(50) REFERENCES dim_secao(secao)
    );

    CREATE TABLE IF NOT EXISTS dim_uf (
        id_uf INTEGER PRIMARY KEY,
        nome VARCHAR(100)
    );

    CREATE TABLE IF NOT EXISTS dim_mesorregiao (
        id_mesorregiao INTEGER PRIMARY KEY,
        nome VARCHAR(100),
        id_uf INTEGER REFERENCES dim_uf(id_uf)
    );

    CREATE TABLE IF NOT EXISTS dim_microrregiao (
        id_microrregiao INTEGER PRIMARY KEY,
        nome VARCHAR(100),
        id_mesorregiao INTEGER REFERENCES dim_mesorregiao(id_mesorregiao)
    );

    CREATE TABLE IF NOT EXISTS dim_municipio (
        id_municipio INTEGER PRIMARY KEY,
        nome VARCHAR(100),
        longitude FLOAT,
        latitude FLOAT,
        id_microrregiao INTEGER REFERENCES dim_microrregiao(id_microrregiao)
    );

    CREATE TABLE IF NOT EXISTS fact_sec_muni (
        id_municipio INTEGER REFERENCES dim_municipio(id_municipio),
        ano INTEGER REFERENCES dim_ano(ano),
        secao VARCHAR(50) REFERENCES dim_secao(secao),
        ql_nac FLOAT,
        ql_est FLOAT,
        qe_nac FLOAT,
        qe_est FLOAT
    );

    CREATE TABLE IF NOT EXISTS fact_div_muni (
        id_municipio INTEGER REFERENCES dim_municipio(id_municipio),
        ano INTEGER REFERENCES dim_ano(ano),
        divisao VARCHAR(50) REFERENCES dim_divisao(divisao),
        ql_nac FLOAT,
        ql_est FLOAT,
        qe_nac FLOAT,
        qe_est FLOAT
    );

    CREATE TABLE IF NOT EXISTS fact_sec_micro (
        id_microrregiao INTEGER REFERENCES dim_microrregiao(id_microrregiao),
        ano INTEGER REFERENCES dim_ano(ano),
        secao VARCHAR(50) REFERENCES dim_secao(secao),
        ql_nac FLOAT,
        ql_est FLOAT,
        qe_nac FLOAT,
        qe_est FLOAT
    );

    CREATE TABLE IF NOT EXISTS fact_div_micro (
        id_microrregiao INTEGER REFERENCES dim_microrregiao(id_microrregiao),
        ano INTEGER REFERENCES dim_ano(ano),
        divisao VARCHAR(50) REFERENCES dim_divisao(divisao),
        ql_nac FLOAT,
        ql_est FLOAT,
        qe_nac FLOAT,
        qe_est FLOAT
    );

    CREATE TABLE IF NOT EXISTS fact_sec_meso (
        ano INTEGER REFERENCES dim_ano(ano),
        secao VARCHAR(50) REFERENCES dim_secao(secao),
        id_mesorregiao INTEGER REFERENCES dim_mesorregiao(id_mesorregiao),
        ql_nac FLOAT,
        ql_est FLOAT,
        qe_nac FLOAT,
        qe_est FLOAT
    );

    CREATE TABLE IF NOT EXISTS fact_div_meso (
        ano INTEGER REFERENCES dim_ano(ano),
        divisao VARCHAR(50) REFERENCES dim_divisao(divisao),
        id_mesorregiao INTEGER REFERENCES dim_mesorregiao(id_mesorregiao),
        ql_nac FLOAT,
        ql_est FLOAT,
        qe_nac FLOAT,
        qe_est FLOAT
    );
""")

conn.commit()
cur.close()
conn.close()
