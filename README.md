# ETL - RAIS

Este repositório contém um pipeline **ETL** (Extração, Transformação e Carga) para dados da **RAIS** (Relação Anual de Informações Sociais), com o objetivo de carregar, transformar, padronizar e analisar dados do mercado de trabalho brasileiro e das vocações e especializações nos diversos setores econômicos do Brasil por meio de Coeficiente Locacionais e Índices de Especialização.

## Funcionalidades

- **Carregamento de dados**: Leitura de arquivos `.txt` e `.csv` de um diretório.
- **Transformação e padronização**: Limpeza, padronização de variáveis e formatação dos dados.
- **Merge com dicionários**: Junção dos dados principais com dicionários auxiliares para enriquecer as análises.
- **Cálculo do Quociente Locacional (QL)**: O índice QL é calculado para cada seção e para cada divisão de uma seção, permitindo análise setorial detalhada.
- **Preparação para banco de dados**: Os dados processados serão carregados em um banco PostgreSQL (etapa futura).
- **Visualização**: Geração de dashboards no Power BI para análise dos resultados.

## Tecnologias Utilizadas

- **Python**: Para carregamento, tratamento e transformação dos dados.
- **PostgreSQL**: Para armazenamento dos dados processados (planejado para etapas futuras).
- **Power BI**: Para criação de dashboards interativos.

## Como usar

1. **Clone o repositório**  
   ```bash
   git clone https://github.com/seu-usuario/etl-rais.git
   cd etl-rais
   ```

2. **Instale as dependências**  
   Recomenda-se o uso de um ambiente virtual:
   ```bash
   python -m venv venv
   source venv/bin/activate  # ou venv\Scripts\activate no Windows
   pip install -r requirements.txt
   ```

3. **Configure o diretório de dados**
   Coloque seus arquivos `.txt` e `.csv` no diretório de dados especificado em ```caminho``` no código. 
   Todos arquivos devem ter o nome no padrão ```ESTB{ano}```.
   

4. **Execute o pipeline ETL**  
   Os arquivos contendo os índices de Quoeficientes Locacionais de cada divisão e seção serão gerados em formatio ```.csv``` em diretórios próprios na raiz do projeto.


5. **Visualize os resultados no Power BI**  
   Um arquivo ```.pbix``` contendo todos os relatórios e análises a respeito dos índices e localidades para os diversos setores econômicos.

## Exemplo de Dashboards

![Dashboard 1](/dashboards/dash2.png)
![Dashboard 2](/dashboards/dash3.png)

## Estrutura do Projeto

```
etl-rais/
├── dicionarios/         # Dicionários necessários para tradução dos dados
├── notebooks/           # Notebooks de análise e exploração
├── scripts/                                  # Scripts Python de transformação
    ├── indices_ql_estabelecimentos.ipynb     # Script principal de execução do ETL
├── requirements.txt                          # Dependências do projeto
├── dashboards                                # Arquivos Power BI e previews
└── README.md
```

## Contribuição

Contribuições são bem-vindas! Abra uma issue ou um pull request com comentários, sugestões ou melhorias.
