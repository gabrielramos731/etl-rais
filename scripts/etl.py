import pandas as pd

def read_data(caminho, ano)-> pd.DataFrame:
    '''Read the ESTB data from a file and return a DataFrame.'''
    try: # txt file
        colunas = ['CNAE 2.0 Classe', 'Município']
        df_estb = pd.read_csv(caminho + 'ESTB' + ano + '.txt', sep=';', encoding='latin1', usecols=colunas)
        df_estb['ano'] = ano
        extensao_arquivo = 'txt'
        
    except: # csv file
        colunas = ['ano', 'id_municipio', 'cnae_2']
        df_estb = pd.read_csv(caminho + 'ESTB' + ano + '.csv', usecols=colunas)
        extensao_arquivo = 'csv'

    return df_estb, extensao_arquivo

def transform_data(df, extensao) -> pd.DataFrame:
    '''Normaliza o tipo de cada coluna e obtem a cnae_secao de cnae_2'''
    
    if extensao == 'txt':
        df.rename(columns={'Município': 'id_municipio', 'CNAE 2.0 Classe': 'cnae_2'}, inplace=True)	
        df = df[['ano', 'id_municipio', 'cnae_2']]
    elif extensao == 'csv':
        df.dropna(inplace=True)
        df['id_municipio'] = df['id_municipio'].astype('int')
        df['id_municipio'] = df['id_municipio'].apply(lambda x: int(x/10)) # remove 7'th dígito
    
    # para todos
    df['ano'] = df['ano'].astype('int16')
    
    df['id_municipio'] = df['id_municipio'].astype('int')
    
    df.loc[:, 'cnae_2'] = df['cnae_2'].apply(lambda x: str(x).zfill(5))
    df['cnae_secao'] = df['cnae_2'].apply(lambda x: x[:2])
    df['cnae_secao'] = df['cnae_secao'].astype('int')
    df.drop(columns=['cnae_2'], inplace=True)
    return df

def merge_municipios(df, caminho, loc) -> pd.DataFrame:
    '''Faz o merge do id_municipio com os dados do dicionário'''
    
    if loc == 'meso':
        colunas_muni = ['id_municipio_6', 'id_mesorregiao', 'nome_mesorregiao']
    elif loc == 'micro':
        colunas_muni = ['id_municipio_6', 'id_microrregiao', 'nome_microrregiao', 'id_mesorregiao']
    elif loc == 'muni':
        colunas_muni = ['id_municipio_6', 'centroide', 'nome', 'id_microrregiao']
    dicionario_muni = pd.read_csv(caminho, sep=',', usecols=colunas_muni)
    dicionario_muni.rename(columns={'id_municipio_6': 'id_municipio', 'nome': 'municipio'}, inplace=True)
    
    df = pd.merge(df, dicionario_muni, on='id_municipio', how='left')
    return df

def merge_cnae(df, caminho) -> pd.DataFrame:
    '''Faz o merge do cnae_secao com os dados do dicionário'''
    
    colunas_tmp = ['divisao','descricao_secao', 'descricao_divisao']
    df_cnae = pd.read_csv(caminho, usecols=colunas_tmp)
    df_cnae.rename(columns={'divisao': 'cnae_secao'}, inplace=True)
    df_cnae.drop_duplicates(inplace=True)   
    
    df = pd.merge(df, df_cnae, on='cnae_secao', how='left')
    df.drop(columns=['cnae_secao'], inplace=True)
    return df

def calc_ql(df, loc='meso') -> pd.DataFrame:
    '''
    Calcula o quociente de localização para níveis de meso, microrregião ou município tendo o estado como setor de referência
    loc= 'meso', 'micro' ou 'muni'
    '''
    
    if loc == 'meso':
        numerador = df.groupby(['ano', 'id_mesorregiao', 'descricao_secao', 'descricao_divisao']).size() / df.groupby(['ano', 'id_mesorregiao']).size()
        denominador = df.groupby(['ano', 'descricao_secao', 'descricao_divisao']).size() / df.groupby(['ano']).size()
        ql = numerador / denominador
        ql = ql.reset_index()
        ql.columns = ['ano', 'id_mesorregiao', 'descricao_secao', 'descricao_divisao', 'quociente_localizacao']
        
    elif loc == 'micro':
        numerador = df.groupby(['ano', 'id_microrregiao', 'descricao_secao', 'descricao_divisao']).size() / df.groupby(['ano', 'id_microrregiao']).size()
        denominador = df.groupby(['ano', 'descricao_secao', 'descricao_divisao']).size() / df.groupby(['ano']).size()
        ql = numerador / denominador
        ql = ql.reset_index()
        ql.columns = ['ano', 'id_microrregiao', 'descricao_secao', 'descricao_divisao', 'quociente_localizacao']
        
    elif loc == 'muni':
        numerador = df.groupby(['ano', 'id_municipio', 'descricao_secao', 'descricao_divisao']).size() / df.groupby(['ano', 'id_municipio']).size()
        denominador = df.groupby(['ano', 'descricao_secao', 'descricao_divisao']).size() / df.groupby(['ano']).size()
        ql = numerador / denominador
        ql = ql.reset_index()
        ql.columns = ['ano', 'id_municipio', 'descricao_secao', 'descricao_divisao', 'quociente_localizacao']

    ql['quociente_localizacao'] = ql['quociente_localizacao'].map(lambda x: float(f"{x:.2f}"))

    return ql

def etl_pipeline(caminhos, ano_ini, ano_fim, loc):
    '''
    Executa o pipeline ETL para os dados de estabelecimentos da RAIS.

    Parâmetros:
        caminhos (dict): Dicionário com os caminhos dos arquivos de dados e dicionários.
        ano_ini (int): Ano inicial do processamento.
        ano_fim (int): Ano final do processamento.
        loc (str): Nível de localização para o cálculo do quociente de localização ('meso', 'micro' ou 'muni').

    Retorna:
        pd.DataFrame: DataFrame com o quociente de localização calculado para o(s) ano(s) e nível selecionados.
    '''
    dfs_ql = []
    for ano in range(ano_ini, ano_fim+1):
        print('---', ano, '---')
        df, extensao = read_data(caminhos['rais'], str(ano))
        df = transform_data(df, extensao)
        df = merge_municipios(df, caminhos['municipios'], loc)
        df = merge_cnae(df, caminhos['cnae'])
        
        dfs_ql.append(calc_ql(df, loc))
    df = pd.concat(dfs_ql)
    return df
