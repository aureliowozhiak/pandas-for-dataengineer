"""
AULA 01: Introdução ao Pandas e DataFrames Básicos
==================================================

Esta é nossa primeira aula sobre pandas para engenharia de dados.
Vamos começar com os conceitos mais fundamentais.

Objetivos:
- Entender o que é pandas e por que é importante
- Conhecer as estruturas básicas: Series e DataFrame
- Criar DataFrames simples
- Operações básicas de visualização
"""

import pandas as pd

print("=" * 60)
print("AULA 01: INTRODUÇÃO AO PANDAS E DATAFRAMES BÁSICOS")
print("=" * 60)

# 1. O QUE É PANDAS?
print("\n1. O QUE É PANDAS?")
print("-" * 30)
print("Pandas é a biblioteca mais importante para manipulação de dados em Python.")
print("Nome vem de 'Panel Data' - dados estruturados multidimensionais.")
print("Essencial para engenharia de dados, análise e ciência de dados.")

# 2. ESTRUTURAS BÁSICAS
print("\n2. ESTRUTURAS BÁSICAS DO PANDAS")
print("-" * 30)

# 2.1 Series - estrutura unidimensional
print("\n2.1 SERIES (1D)")
vendas_series = pd.Series([1000, 1200, 800, 1500, 900])
print("Series simples:")
print(vendas_series)

# Series com índice customizado
vendas_meses = pd.Series([1000, 1200, 800, 1500, 900], 
                        index=['Jan', 'Fev', 'Mar', 'Abr', 'Mai'])
print("\nSeries com índice customizado:")
print(vendas_meses)

# 2.2 DataFrame - estrutura bidimensional
print("\n2.2 DATAFRAME (2D)")
print("DataFrame é como uma tabela do Excel ou planilha:")

# Criando DataFrame simples
dados_vendas = {
    'produto': ['Notebook', 'Mouse', 'Teclado', 'Monitor'],
    'preco': [2500, 85, 320, 899],
    'quantidade': [2, 5, 3, 1]
}

df_vendas = pd.DataFrame(dados_vendas)
print(df_vendas)

# 3. INFORMAÇÕES BÁSICAS SOBRE O DATAFRAME
print("\n3. INFORMAÇÕES BÁSICAS")
print("-" * 30)

print(f"Formato (linhas, colunas): {df_vendas.shape}")
print(f"Número de linhas: {len(df_vendas)}")
print(f"Colunas: {list(df_vendas.columns)}")
print(f"Tipos de dados:")
print(df_vendas.dtypes)

# 4. VISUALIZAÇÃO BÁSICA
print("\n4. VISUALIZAÇÃO BÁSICA")
print("-" * 30)

print("Primeiras linhas (head):")
print(df_vendas.head())

print("\nÚltimas linhas (tail):")
print(df_vendas.tail())

print("\nInformações gerais (info):")
df_vendas.info()

print("\nEstatísticas descritivas (describe):")
print(df_vendas.describe())

# 5. ACESSANDO DADOS
print("\n5. ACESSANDO DADOS")
print("-" * 30)

print("Uma coluna:")
print(df_vendas['produto'])

print("\nMúltiplas colunas:")
print(df_vendas[['produto', 'preco']])

print("\nPrimeira linha:")
print(df_vendas.iloc[0])

print("\nPrimeira linha, coluna 'produto':")
print(df_vendas.iloc[0]['produto'])

# 6. OPERAÇÕES BÁSICAS
print("\n6. OPERAÇÕES BÁSICAS")
print("-" * 30)

# Calculando valor total
df_vendas['valor_total'] = df_vendas['preco'] * df_vendas['quantidade']
print("DataFrame com nova coluna 'valor_total':")
print(df_vendas)

print(f"\nSoma total das vendas: R$ {df_vendas['valor_total'].sum()}")
print(f"Preço médio: R$ {df_vendas['preco'].mean():.2f}")
print(f"Produto mais caro: {df_vendas.loc[df_vendas['preco'].idxmax(), 'produto']}")

# 7. EXERCÍCIO PRÁTICO
print("\n7. EXERCÍCIO PRÁTICO")
print("-" * 30)
print("Vamos criar um DataFrame com dados de funcionários:")

funcionarios = {
    'nome': ['Ana', 'João', 'Maria', 'Pedro'],
    'departamento': ['Vendas', 'TI', 'RH', 'Financeiro'],
    'salario': [5000, 7500, 4500, 6000],
    'anos_empresa': [3, 5, 2, 4]
}

df_funcionarios = pd.DataFrame(funcionarios)
print(df_funcionarios)

print(f"\nSalário médio: R$ {df_funcionarios['salario'].mean():.2f}")
print(f"Funcionário com maior salário: {df_funcionarios.loc[df_funcionarios['salario'].idxmax(), 'nome']}")

# 8. PONTOS IMPORTANTES PARA ENGENHARIA DE DADOS
print("\n8. PONTOS IMPORTANTES PARA ENGENHARIA DE DADOS")
print("-" * 50)
print("✓ Pandas é fundamental para ETL (Extract, Transform, Load)")
print("✓ DataFrames são como tabelas de banco de dados em memória")
print("✓ Pandas facilita limpeza, transformação e análise de dados")
print("✓ Integra bem com outras ferramentas (SQL, Spark, etc.)")
print("✓ Essencial para pipelines de dados e análises exploratórias")

print("\n" + "=" * 60)
print("FIM DA AULA 01")
print("Próxima aula: Leitura e escrita de dados")
print("=" * 60)
