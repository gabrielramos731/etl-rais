import streamlit as st
import seaborn as sns
import plotly.graph_objects as go

st.write("# Análise de coeficeintes locacionais e de especialização")
st.write("### Dados obtidos através da base RAIS")
conn = st.connection("postgresql", type="sql")

select_reg = st.sidebar.selectbox('Região',
                                  ('Mesorregiõao', 'Microrregião', 'Município'))
uf = st.selectbox('Selecione uma UF', 
                  conn.query('''SELECT nome 
                             FROM dim_uf''',
                             ttl="10m")['nome'].unique())

meso = st.selectbox('Selecione uma mesorregião', 
                    conn.query('''SELECT DISTINCT dim_mesorregiao.nome 
                               FROM dim_mesorregiao 
                               JOIN dim_uf ON dim_mesorregiao.id_uf = dim_uf.id_uf 
                               WHERE dim_uf.nome = :uf
                               ORDER BY dim_mesorregiao.nome''',
                               params={"uf": uf},
                               ttl='10m'))

if select_reg == 'Mesorregiõao':
    # unique das mesorregioes da UF selecionada
    
    df = conn.query('''SELECT
        f.ano,
        uf.nome AS uf,
        meso.nome AS mesorregiao,
        f.secao,
        f.ql_est,
        f.ql_nac,
        f.qe_est,
        f.qe_nac
        FROM fact_sec_meso f
        JOIN dim_mesorregiao meso ON f.id_mesorregiao = meso.id_mesorregiao
        JOIN dim_uf uf ON meso.id_uf = uf.id_uf
        WHERE uf.nome = :uf''', 
        params={"uf":uf},
        ttl="10m")

    st.dataframe(df)
    secao = st.selectbox('Selecione uma seção', df['secao'].unique())
    df_sec = df.query(f'mesorregiao == "{meso}" and secao == "{secao}"')
    indices = st.multiselect('Selecione os índices',
                             ['ql_est', 'ql_nac', 'qe_est', 'qe_nac'],
                             default=['ql_est', 'ql_nac'],
                             help='Escolha os índices que deseja visualizar no gráfico.')
    st.line_chart(df_sec, x='ano', y=(indices))

elif select_reg == 'Microrregião':
    micro = st.selectbox('Selecione uma microrregião', conn.query('''SELECT DISTINCT dim_microrregiao.nome 
                                                                FROM dim_microrregiao 
                                                                JOIN dim_mesorregiao ON dim_mesorregiao.id_mesorregiao = dim_microrregiao.id_mesorregiao 
                                                                WHERE dim_mesorregiao.nome = :meso
                                                                ORDER BY dim_microrregiao.nome''',
                                                                params={"meso": meso},
                                                                ttl='10m'))
    
    df = conn.query('''SELECT
        f.ano,
        uf.nome AS uf,
        meso.nome AS mesorregiao,
        dmicro.nome AS microrregiao,
        f.secao,
        f.ql_est,
        f.ql_nac,
        f.qe_est,
        f.qe_nac
        FROM fact_sec_micro f
        JOIN dim_microrregiao dmicro ON f.id_microrregiao = dmicro.id_microrregiao
        JOIN dim_mesorregiao meso ON dmicro.id_mesorregiao = meso.id_mesorregiao
        JOIN dim_uf uf ON meso.id_uf = uf.id_uf
        WHERE uf.nome = :uf
        ''', 
        params={"uf":uf},
        ttl="10m")
    
    st.dataframe(df)
    secao = st.selectbox('Selecione uma seção', df['secao'].unique())
    df_sec = df.query(f'microrregiao == "{micro}" and secao == "{secao}"')
    indices = st.multiselect('Selecione os índices',
                                ['ql_est', 'ql_nac', 'qe_est', 'qe_nac'],
                                default=['ql_est', 'ql_nac'],
                                help='Escolha os índices que deseja visualizar no gráfico.')
    st.line_chart(df_sec, x='ano', y=(indices))
    
elif select_reg == 'Município':
    micro = st.selectbox('Selecione uma microrregião', conn.query('''SELECT DISTINCT dim_microrregiao.nome 
                                                                FROM dim_microrregiao 
                                                                JOIN dim_mesorregiao ON dim_mesorregiao.id_mesorregiao = dim_microrregiao.id_mesorregiao 
                                                                WHERE dim_mesorregiao.nome = :meso
                                                                ORDER BY dim_microrregiao.nome''',
                                                                params={"meso": meso},
                                                                ttl='10m'))
    
    muni = st.selectbox('Selecione um município', conn.query('''SELECT DISTINCT dim_municipio.nome 
                                                                FROM dim_municipio 
                                                                JOIN dim_microrregiao on dim_microrregiao.id_microrregiao = dim_municipio.id_microrregiao
                                                                JOIN dim_mesorregiao ON dim_mesorregiao.id_mesorregiao = dim_microrregiao.id_mesorregiao 
                                                                WHERE dim_mesorregiao.nome = :meso
                                                                    AND dim_microrregiao.nome = :micro
                                                                ORDER BY dim_municipio.nome''',
                                                                params={"meso": meso, "micro": micro},
                                                                ttl='10m'))
    
    df = conn.query('''SELECT
        f.ano,
        uf.nome AS uf,
        meso.nome AS mesorregiao,
        dmicro.nome AS microrregiao,
        dmuni.nome AS municipio,
        f.secao,
        f.ql_est,
        f.ql_nac,
        f.qe_est,
        f.qe_nac
        FROM fact_sec_muni f
        JOIN dim_municipio dmuni ON f.id_municipio = dmuni.id_municipio
        JOIN dim_microrregiao dmicro ON dmuni.id_microrregiao = dmicro.id_microrregiao
        JOIN dim_mesorregiao meso ON dmicro.id_mesorregiao = meso.id_mesorregiao
        JOIN dim_uf uf ON meso.id_uf = uf.id_uf
        WHERE uf.nome = :uf
        ''', 
        params={"uf":uf},
        ttl="10m")
    
    st.dataframe(df)
    secao = st.selectbox('Selecione uma seção', sorted(df['secao'].unique()))
    df_sec = df.query(f'municipio == "{muni}" and secao == "{secao}"')
    indices = st.multiselect('Selecione os índices',
                                ['ql_est', 'ql_nac', 'qe_est', 'qe_nac'],
                                default=['ql_est', 'ql_nac'],
                                help='Escolha os índices que deseja visualizar no gráfico.')
    st.line_chart(df_sec, x='ano', y=(indices))

