"""
AULA 03: Exploração e Análise Descritiva de Dados
=================================================

Nesta aula, vamos aprender técnicas essenciais para explorar e entender nossos dados.
Esta é uma etapa crucial em qualquer projeto de engenharia de dados.

Objetivos:
- Explorar a estrutura dos dados
- Calcular estatísticas descritivas
- Identificar padrões e anomalias
- Visualizar distribuições básicas
- Analisar correlações entre variáveis
"""

import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')

print("=" * 60)
print("AULA 03: EXPLORAÇÃO E ANÁLISE DESCRITIVA")
print("=" * 60)

# Carregando dados para exploração
try:
    df_vendas = pd.read_csv('data/vendas.csv', parse_dates=['data'])
    df_funcionarios = pd.read_json('data/funcionarios.json')
    print("✓ Dados carregados com sucesso!")
except:
    print("! Criando dados de exemplo para a aula")
    # Dados de exemplo se os arquivos não existirem
    df_vendas = pd.DataFrame({
        'data': pd.date_range('2024-01-01', periods=50, freq='D'),
        'vendedor': np.random.choice(['Ana', 'João', 'Maria'], 50),
        'produto': np.random.choice(['Notebook', 'Mouse', 'Teclado'], 50),
        'quantidade': np.random.randint(1, 10, 50),
        'preco_unitario': np.random.uniform(50, 3000, 50).round(2)
    })
    
    df_funcionarios = pd.DataFrame({
        'nome': ['Ana Silva', 'João Santos', 'Maria Costa'],
        'salario': [5500, 4800, 6200],
        'idade': [28, 32, 35],
        'departamento': ['Vendas', 'Vendas', 'Marketing']
    })

# 1. VISÃO GERAL DOS DADOS
print("\n1. VISÃO GERAL DOS DADOS")
print("-" * 30)

print("DATASET: VENDAS")
print(f"Shape: {df_vendas.shape}")
print(f"Colunas: {list(df_vendas.columns)}")
print(f"Período: {df_vendas['data'].min()} a {df_vendas['data'].max()}")

print("\nPrimeiras linhas:")
print(df_vendas.head())

print("\nInformações gerais:")
df_vendas.info()

# 2. ESTATÍSTICAS DESCRITIVAS
print("\n2. ESTATÍSTICAS DESCRITIVAS")
print("-" * 35)

print("Estatísticas das variáveis numéricas:")
print(df_vendas.describe())

print("\nEstatísticas das variáveis categóricas:")
print(df_vendas.describe(include=['object']))

# Estatísticas personalizadas
print("\nEstatísticas customizadas:")
df_vendas['valor_total'] = df_vendas['quantidade'] * df_vendas['preco_unitario']

stats_custom = {
    'Total de vendas': df_vendas['valor_total'].sum(),
    'Média por venda': df_vendas['valor_total'].mean(),
    'Mediana': df_vendas['valor_total'].median(),
    'Desvio padrão': df_vendas['valor_total'].std(),
    'Coeficiente de variação': df_vendas['valor_total'].std() / df_vendas['valor_total'].mean(),
    'Maior venda': df_vendas['valor_total'].max(),
    'Menor venda': df_vendas['valor_total'].min()
}

for stat, value in stats_custom.items():
    print(f"{stat}: {value:.2f}")

# 3. ANÁLISE DE FREQUÊNCIAS
print("\n3. ANÁLISE DE FREQUÊNCIAS")
print("-" * 35)

print("Vendedores (value_counts):")
print(df_vendas['vendedor'].value_counts())

print("\nProdutos mais vendidos:")
print(df_vendas['produto'].value_counts())

print("\nDistribuição percentual de produtos:")
print(df_vendas['produto'].value_counts(normalize=True) * 100)

# 4. ANÁLISE DE VALORES ÚNICOS E NULOS
print("\n4. VALORES ÚNICOS E NULOS")
print("-" * 35)

print("Valores únicos por coluna:")
for col in df_vendas.columns:
    unique_count = df_vendas[col].nunique()
    print(f"{col}: {unique_count} valores únicos")

print("\nValores nulos:")
print(df_vendas.isnull().sum())

print("\nPercentual de valores nulos:")
print((df_vendas.isnull().sum() / len(df_vendas)) * 100)

# 5. ANÁLISE DE DISTRIBUIÇÕES
print("\n5. ANÁLISE DE DISTRIBUIÇÕES")
print("-" * 35)

print("Quartis da variável 'valor_total':")
quartis = df_vendas['valor_total'].quantile([0.25, 0.5, 0.75, 0.9, 0.95])
for q, value in quartis.items():
    print(f"Q{int(q*100)}: {value:.2f}")

print("\nDetecção de outliers (IQR method):")
Q1 = df_vendas['valor_total'].quantile(0.25)
Q3 = df_vendas['valor_total'].quantile(0.75)
IQR = Q3 - Q1
limite_inferior = Q1 - 1.5 * IQR
limite_superior = Q3 + 1.5 * IQR

outliers = df_vendas[(df_vendas['valor_total'] < limite_inferior) | 
                    (df_vendas['valor_total'] > limite_superior)]
print(f"Outliers encontrados: {len(outliers)}")
if len(outliers) > 0:
    print("Valores outliers:")
    print(outliers['valor_total'].values)

# 6. ANÁLISE TEMPORAL (se dados de data estiverem disponíveis)
print("\n6. ANÁLISE TEMPORAL")
print("-" * 25)

if 'data' in df_vendas.columns:
    df_vendas['dia_semana'] = df_vendas['data'].dt.day_name()
    df_vendas['mes'] = df_vendas['data'].dt.month
    
    print("Vendas por dia da semana:")
    vendas_dia = df_vendas.groupby('dia_semana')['valor_total'].sum().sort_values(ascending=False)
    print(vendas_dia)
    
    print("\nVendas por mês:")
    vendas_mes = df_vendas.groupby('mes')['valor_total'].sum()
    print(vendas_mes)

# 7. ANÁLISE DE CORRELAÇÕES
print("\n7. ANÁLISE DE CORRELAÇÕES")
print("-" * 30)

# Selecionando apenas colunas numéricas
numeric_cols = df_vendas.select_dtypes(include=[np.number])
correlations = numeric_cols.corr()

print("Matriz de correlação:")
print(correlations)

print("\nCorrelações mais fortes (> 0.5 ou < -0.5):")
# Encontrar correlações fortes
for i in range(len(correlations.columns)):
    for j in range(i+1, len(correlations.columns)):
        corr_value = correlations.iloc[i, j]
        if abs(corr_value) > 0.5:
            col1, col2 = correlations.columns[i], correlations.columns[j]
            print(f"{col1} x {col2}: {corr_value:.3f}")

# 8. ANÁLISE POR GRUPOS
print("\n8. ANÁLISE POR GRUPOS")
print("-" * 25)

print("Estatísticas por vendedor:")
stats_vendedor = df_vendas.groupby('vendedor').agg({
    'valor_total': ['count', 'sum', 'mean', 'std'],
    'quantidade': 'sum'
}).round(2)
print(stats_vendedor)

print("\nTop produtos por valor total:")
top_produtos = df_vendas.groupby('produto')['valor_total'].sum().sort_values(ascending=False)
print(top_produtos)

# 9. IDENTIFICAÇÃO DE PADRÕES
print("\n9. IDENTIFICAÇÃO DE PADRÕES")
print("-" * 35)

# Padrões de vendas
print("Análise de padrões:")

# Vendedor mais eficiente (maior valor médio por venda)
vendedor_eficiente = df_vendas.groupby('vendedor')['valor_total'].mean().idxmax()
print(f"Vendedor mais eficiente: {vendedor_eficiente}")

# Produto com maior margem (assumindo que preço alto = maior margem)
produto_premium = df_vendas.groupby('produto')['preco_unitario'].mean().idxmax()
print(f"Produto premium: {produto_premium}")

# Dias com vendas acima da média
if 'data' in df_vendas.columns:
    vendas_diarias = df_vendas.groupby('data')['valor_total'].sum()
    media_diaria = vendas_diarias.mean()
    dias_acima_media = (vendas_diarias > media_diaria).sum()
    print(f"Dias com vendas acima da média: {dias_acima_media} de {len(vendas_diarias)}")

# 10. RELATÓRIO DE QUALIDADE DOS DADOS
print("\n10. RELATÓRIO DE QUALIDADE DOS DADOS")
print("-" * 40)

def relatorio_qualidade(df, nome_dataset):
    print(f"\nRELATÓRIO: {nome_dataset}")
    print("-" * 30)
    
    total_registros = len(df)
    total_colunas = len(df.columns)
    
    print(f"Total de registros: {total_registros:,}")
    print(f"Total de colunas: {total_colunas}")
    
    # Completude
    completude = (1 - df.isnull().sum() / len(df)) * 100
    print(f"Completude média: {completude.mean():.1f}%")
    
    # Colunas com problemas
    colunas_problemas = completude[completude < 95]
    if len(colunas_problemas) > 0:
        print("Colunas com <95% completude:")
        for col, comp in colunas_problemas.items():
            print(f"  {col}: {comp:.1f}%")
    
    # Duplicatas
    duplicatas = df.duplicated().sum()
    print(f"Registros duplicados: {duplicatas}")
    
    # Tipos de dados
    tipos = df.dtypes.value_counts()
    print("Distribuição de tipos:")
    for tipo, count in tipos.items():
        print(f"  {tipo}: {count} colunas")

# Gerando relatórios
relatorio_qualidade(df_vendas, "VENDAS")
relatorio_qualidade(df_funcionarios, "FUNCIONÁRIOS")

# 11. DICAS PARA ENGENHARIA DE DADOS
print("\n11. DICAS PARA ENGENHARIA DE DADOS")
print("-" * 40)

tips = [
    "✓ Sempre comece com .info() e .describe()",
    "✓ Verifique valores nulos e duplicatas",
    "✓ Analise distribuições para detectar outliers",
    "✓ Use value_counts() para variáveis categóricas",
    "✓ Calcule correlações para entender relacionamentos",
    "✓ Documente achados importantes da exploração",
    "✓ Crie visualizações para comunicar insights",
    "✓ Valide suposições sobre os dados",
    "✓ Identifique padrões temporais se aplicável",
    "✓ Monitore qualidade dos dados continuamente"
]

for tip in tips:
    print(tip)

print("\n" + "=" * 60)
print("FIM DA AULA 03")
print("Próxima aula: Limpeza e tratamento de dados")
print("=" * 60)
