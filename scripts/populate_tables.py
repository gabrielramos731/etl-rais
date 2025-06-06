import pandas as pd
import psycopg2
import time

def load_csv_to_db():
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="230204",
        port="5433"
    )
    conn.autocommit = True
    cur = conn.cursor()

    inicio = time.time()

    anos = [(ano,) for ano in list(range(2007, 2025))]
    cur.executemany(
        "INSERT INTO dim_ano (ano) VALUES (%s)",
        anos
    )


    df = pd.read_csv('C:/dev/ndti/rais/dicionarios/dim_uf.csv', sep=',')
    dados = list(df.itertuples(index=False, name=None))
    cur.executemany(
        "INSERT INTO dim_uf (id_uf, nome) VALUES (%s, %s)",
        dados
    )

    df = pd.read_csv('C:/dev/ndti/rais/dicionarios/dim_mesorregiao.csv', sep=',')
    dados = list(df.itertuples(index=False, name=None))
    cur.executemany(
        "INSERT INTO dim_mesorregiao (id_mesorregiao, nome, id_uf) VALUES (%s, %s, %s)",
        dados
    )

    df = pd.read_csv('C:/dev/ndti/rais/dicionarios/dim_microrregiao.csv', sep=',')
    dados = list(df.itertuples(index=False, name=None))
    cur.executemany(
        "INSERT INTO dim_microrregiao (id_microrregiao, nome, id_mesorregiao) VALUES (%s, %s, %s)",
        dados
    )

    df = pd.read_csv('C:/dev/ndti/rais/dicionarios/dim_municipio.csv', sep=',')
    dados = list(df.itertuples(index=False, name=None))
    cur.executemany(
        "INSERT INTO dim_municipio (id_municipio, nome, id_microrregiao) VALUES (%s, %s, %s)",
        dados
    )

    df = pd.read_csv('C:/dev/ndti/rais/dicionarios/dim_secao.csv', sep=',')
    dados = list(df.itertuples(index=False, name=None))
    cur.executemany(
        "INSERT INTO dim_secao (secao) VALUES (%s)",
        dados
    )

    df = pd.read_csv('C:/dev/ndti/rais/dicionarios/dim_divisao.csv', sep=',')
    dados = list(df.itertuples(index=False, name=None))
    cur.executemany(
        "INSERT INTO dim_divisao (divisao, secao) VALUES (%s, %s)",
        dados
    )

    # ----- TABELAS FATOS -----

    df = pd.read_csv('C:/dev/ndti/rais/indices2/secao/fact_sec_mesorregiao.csv', sep=';')
    dados = list(df.itertuples(index=False, name=None))
    cur.executemany(
        "INSERT INTO fact_sec_meso (ano, id_mesorregiao, secao, ql_est, ql_nac, qe_est, qe_nac) VALUES (%s, %s, %s, %s, %s, %s, %s)",
        dados
    )

    df = pd.read_csv('C:/dev/ndti/rais/indices2/secao/fact_sec_microrregiao.csv', sep=';')
    dados = list(df.itertuples(index=False, name=None))
    cur.executemany(
        "INSERT INTO fact_sec_micro (ano, id_microrregiao, secao, ql_est, ql_nac, qe_est, qe_nac) VALUES (%s, %s, %s, %s, %s, %s, %s)",
        dados
    )

    df = pd.read_csv('C:/dev/ndti/rais/indices2/secao/fact_sec_municipio2.csv', sep=';')
    dados = list(df.itertuples(index=False, name=None))
    cur.executemany(
        "INSERT INTO fact_sec_muni (ano, id_municipio, secao, ql_est, ql_nac, qe_est, qe_nac) VALUES (%s, %s, %s, %s, %s, %s, %s)",
        dados
    )

    df = pd.read_csv('C:/dev/ndti/rais/indices2/divisao/fact_div_mesorregiao.csv', sep=';')
    dados = list(df.itertuples(index=False, name=None))
    cur.executemany(
        "INSERT INTO fact_div_meso (ano, id_mesorregiao, secao, divisao, ql_est, ql_nac, qe_est, qe_nac) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
        dados
    )

    df = pd.read_csv('C:/dev/ndti/rais/indices2/divisao/fact_div_microrregiao.csv', sep=';')
    dados = list(df.itertuples(index=False, name=None))
    cur.executemany(
        "INSERT INTO fact_div_micro (ano, id_microrregiao, secao, divisao, ql_est, ql_nac, qe_est, qe_nac) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
        dados
    )

    df = pd.read_csv('C:/dev/ndti/rais/indices2/divisao/fact_div_municipio2.csv', sep=';')
    dados = list(df.itertuples(index=False, name=None))
    cur.executemany(
        "INSERT INTO fact_div_muni (ano, id_municipio, secao, divisao, ql_est, ql_nac, qe_est, qe_nac) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
        dados
    )

    fim = time.time()
    print(f"Tempo total de execução: {fim - inicio:.2f} segundos")
    
load_csv_to_db()
