"""
AULA 06: Agrupamentos e Agregações
==================================

Nesta aula, vamos dominar as técnicas de agrupamento e agregação em pandas.
Estas são operações fundamentais para análise de dados e relatórios.

Objetivos:
- Dominar groupby e suas variações
- Aplicar múltiplas funções de agregação
- Usar agg() com funções customizadas
- Trabalhar com MultiIndex resultante
- Filtrar grupos com condições
- Aplicar transform() e apply() em grupos
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

print("=" * 60)
print("AULA 06: AGRUPAMENTOS E AGREGAÇÕES")
print("=" * 60)

# Criando dataset abrangente para demonstrações
np.random.seed(42)
n_records = 500

dados = {
    'data': pd.date_range('2024-01-01', periods=n_records, freq='H'),
    'vendedor': np.random.choice(['Ana Silva', 'João Santos', 'Maria Costa', 'Pedro Lima', 'Carlos Rocha'], n_records),
    'produto': np.random.choice(['Notebook', 'Mouse', 'Teclado', 'Monitor', 'Headset', 'Webcam'], n_records),
    'categoria': np.random.choice(['Eletrônicos', 'Acessórios', 'Periféricos'], n_records),
    'regiao': np.random.choice(['Norte', 'Sul', 'Sudeste', 'Nordeste', 'Centro-Oeste'], n_records),
    'canal': np.random.choice(['Online', 'Loja Física', 'Telefone'], n_records),
    'quantidade': np.random.randint(1, 20, n_records),
    'preco_unitario': np.random.uniform(50, 5000, n_records).round(2),
    'desconto_pct': np.random.uniform(0, 30, n_records).round(1)
}

df = pd.DataFrame(dados)
df['valor_bruto'] = df['quantidade'] * df['preco_unitario']
df['desconto_valor'] = df['valor_bruto'] * (df['desconto_pct'] / 100)
df['valor_liquido'] = df['valor_bruto'] - df['desconto_valor']
df['mes'] = df['data'].dt.month
df['dia_semana'] = df['data'].dt.day_name()

print("Dataset criado para demonstrações:")
print(f"Shape: {df.shape}")
print(f"Período: {df['data'].min()} até {df['data'].max()}")
print("\nPrimeiras linhas:")
print(df.head())

# 1. GROUPBY BÁSICO
print("\n1. GROUPBY BÁSICO")
print("-" * 20)

# Agrupamento simples
print("1.1 Agrupamento por vendedor:")
vendas_por_vendedor = df.groupby('vendedor')['valor_liquido'].sum().sort_values(ascending=False)
print(vendas_por_vendedor)

print("\n1.2 Múltiplas estatísticas:")
stats_vendedor = df.groupby('vendedor')['valor_liquido'].agg(['count', 'sum', 'mean', 'std']).round(2)
print(stats_vendedor)

print("\n1.3 Agrupamento por múltiplas colunas:")
vendas_regiao_canal = df.groupby(['regiao', 'canal'])['valor_liquido'].sum()
print(vendas_regiao_canal.head(10))

# 2. FUNÇÃO AGG() AVANÇADA
print("\n2. FUNÇÃO AGG() AVANÇADA")
print("-" * 30)

# Diferentes funções para diferentes colunas
print("2.1 Agregações específicas por coluna:")
agg_dict = {
    'valor_liquido': ['sum', 'mean', 'count'],
    'quantidade': ['sum', 'mean'],
    'desconto_pct': ['mean', 'max'],
    'produto': 'nunique'  # número de produtos únicos
}

resultado_agg = df.groupby('vendedor').agg(agg_dict).round(2)
print(resultado_agg)

# Renomeando colunas do resultado
print("\n2.2 Renomeando colunas:")
agg_renamed = df.groupby('vendedor').agg({
    'valor_liquido': [('total_vendas', 'sum'), ('ticket_medio', 'mean'), ('num_vendas', 'count')],
    'quantidade': [('total_itens', 'sum')],
    'produto': [('produtos_unicos', 'nunique')]
}).round(2)

# Achatar MultiIndex columns
agg_renamed.columns = ['_'.join(col).strip() for col in agg_renamed.columns.values]
print(agg_renamed)

# 3. FUNÇÕES CUSTOMIZADAS DE AGREGAÇÃO
print("\n3. FUNÇÕES CUSTOMIZADAS")
print("-" * 30)

def calcular_coeficiente_variacao(series):
    """Calcular coeficiente de variação"""
    return series.std() / series.mean() if series.mean() != 0 else 0

def percentil_90(series):
    """Calcular percentil 90"""
    return series.quantile(0.9)

def amplitude(series):
    """Calcular amplitude (max - min)"""
    return series.max() - series.min()

print("3.1 Funções estatísticas customizadas:")
stats_custom = df.groupby('categoria')['valor_liquido'].agg([
    'mean',
    ('coef_variacao', calcular_coeficiente_variacao),
    ('p90', percentil_90),
    ('amplitude', amplitude)
]).round(3)
print(stats_custom)

# 4. TRANSFORM() - MANTENDO O TAMANHO ORIGINAL
print("\n4. TRANSFORM() - ESTATÍSTICAS POR GRUPO")
print("-" * 45)

# Transform mantém o tamanho original do DataFrame
df['media_vendedor'] = df.groupby('vendedor')['valor_liquido'].transform('mean')
df['desvio_da_media'] = df['valor_liquido'] - df['media_vendedor']
df['rank_no_grupo'] = df.groupby('vendedor')['valor_liquido'].transform('rank', ascending=False)

print("Comparação individual vs média do grupo:")
comparacao = df[['vendedor', 'valor_liquido', 'media_vendedor', 'desvio_da_media']].head(10)
print(comparacao.round(2))

# Normalização por grupo (z-score)
df['valor_normalizado'] = df.groupby('vendedor')['valor_liquido'].transform(
    lambda x: (x - x.mean()) / x.std()
)

print("\n4.1 Top 5 vendas normalizadas por vendedor:")
top_normalizadas = df.nlargest(5, 'valor_normalizado')[['vendedor', 'valor_liquido', 'valor_normalizado']]
print(top_normalizadas.round(3))

# 5. APPLY() EM GRUPOS
print("\n5. APPLY() EM GRUPOS")
print("-" * 25)

def analise_vendedor(group):
    """Análise detalhada por vendedor"""
    return pd.Series({
        'total_vendas': group['valor_liquido'].sum(),
        'num_transacoes': len(group),
        'ticket_medio': group['valor_liquido'].mean(),
        'melhor_produto': group.loc[group['valor_liquido'].idxmax(), 'produto'],
        'canal_preferido': group['canal'].mode().iloc[0] if not group['canal'].mode().empty else 'N/A',
        'desconto_medio': group['desconto_pct'].mean(),
        'vendas_fim_semana': group[group['dia_semana'].isin(['Saturday', 'Sunday'])]['valor_liquido'].sum()
    })

print("5.1 Análise detalhada por vendedor:")
analise_detalhada = df.groupby('vendedor').apply(analise_vendedor).round(2)
print(analise_detalhada)

# 6. FILTROS EM GRUPOS
print("\n6. FILTROS EM GRUPOS")
print("-" * 25)

# Filter - manter apenas grupos que atendem critério
print("6.1 Vendedores com mais de 80 transações:")
vendedores_ativos = df.groupby('vendedor').filter(lambda x: len(x) > 80)
print(f"Registros filtrados: {len(vendedores_ativos)} de {len(df)}")
print("Vendedores mantidos:")
print(vendedores_ativos['vendedor'].unique())

# Produtos vendidos em todas as regiões
print("\n6.2 Produtos vendidos em todas as regiões:")
produtos_nacionais = df.groupby('produto').filter(
    lambda x: x['regiao'].nunique() == df['regiao'].nunique()
)
print("Produtos nacionais:")
print(produtos_nacionais['produto'].unique())

# 7. AGRUPAMENTOS TEMPORAIS
print("\n7. AGRUPAMENTOS TEMPORAIS")
print("-" * 30)

# Resample para dados temporais
df_temporal = df.set_index('data')

print("7.1 Vendas por dia:")
vendas_diarias = df_temporal['valor_liquido'].resample('D').sum()
print(vendas_diarias.head())

print("\n7.2 Estatísticas semanais:")
stats_semanais = df_temporal.resample('W').agg({
    'valor_liquido': ['sum', 'mean', 'count'],
    'quantidade': 'sum',
    'vendedor': 'nunique'
}).round(2)
print(stats_semanais.head())

# Grouper para agrupamentos temporais mais flexíveis
print("\n7.3 Vendas por vendedor e semana:")
vendas_vendedor_semana = df.groupby([
    pd.Grouper(key='data', freq='W'),
    'vendedor'
])['valor_liquido'].sum().unstack(fill_value=0)
print(vendas_vendedor_semana.head())

# 8. AGRUPAMENTOS HIERÁRQUICOS (MULTIINDEX)
print("\n8. AGRUPAMENTOS HIERÁRQUICOS")
print("-" * 35)

# Múltiplos níveis de agrupamento
vendas_hierarquicas = df.groupby(['regiao', 'canal', 'categoria'])['valor_liquido'].agg([
    'count', 'sum', 'mean'
]).round(2)

print("8.1 Estrutura hierárquica:")
print(vendas_hierarquicas.head(10))

# Operações em MultiIndex
print("\n8.2 Operações por nível:")
# Soma por região (nível 0)
soma_por_regiao = vendas_hierarquicas.groupby(level=0)['sum'].sum()
print("Total por região:")
print(soma_por_regiao)

# Unstacking para visualização
print("\n8.3 Unstacking para matriz:")
matriz_regiao_canal = df.groupby(['regiao', 'canal'])['valor_liquido'].sum().unstack(fill_value=0)
print(matriz_regiao_canal)

# 9. AGREGAÇÕES CONDICIONAIS
print("\n9. AGREGAÇÕES CONDICIONAIS")
print("-" * 30)

def vendas_condicionais(group):
    """Agregações com condições específicas"""
    return pd.Series({
        'vendas_alto_valor': group[group['valor_liquido'] > 1000]['valor_liquido'].sum(),
        'vendas_com_desconto': group[group['desconto_pct'] > 0]['valor_liquido'].sum(),
        'vendas_sem_desconto': group[group['desconto_pct'] == 0]['valor_liquido'].sum(),
        'pct_com_desconto': (group['desconto_pct'] > 0).mean() * 100
    })

print("9.1 Vendas condicionais por canal:")
vendas_cond = df.groupby('canal').apply(vendas_condicionais).round(2)
print(vendas_cond)

# 10. ROLLING E EXPANDING EM GRUPOS
print("\n10. ROLLING E EXPANDING EM GRUPOS")
print("-" * 35)

# Ordenar por data para operações temporais
df_sorted = df.sort_values(['vendedor', 'data']).copy()

# Rolling statistics por vendedor
df_sorted['media_movel_vendedor'] = df_sorted.groupby('vendedor')['valor_liquido'].rolling(
    window=10, min_periods=1
).mean().values

# Expanding statistics (acumulado)
df_sorted['total_acumulado_vendedor'] = df_sorted.groupby('vendedor')['valor_liquido'].expanding().sum().values

print("10.1 Estatísticas móveis e acumuladas:")
exemplo_rolling = df_sorted[df_sorted['vendedor'] == 'Ana Silva'].head(10)[
    ['data', 'valor_liquido', 'media_movel_vendedor', 'total_acumulado_vendedor']
]
print(exemplo_rolling.round(2))

# 11. PERCENTIS E QUANTIS POR GRUPO
print("\n11. PERCENTIS E QUANTIS")
print("-" * 25)

def calcular_percentis(series):
    """Calcular múltiplos percentis"""
    return pd.Series({
        'p25': series.quantile(0.25),
        'p50': series.quantile(0.50),
        'p75': series.quantile(0.75),
        'p90': series.quantile(0.90),
        'p95': series.quantile(0.95)
    })

print("11.1 Percentis de valor por categoria:")
percentis_categoria = df.groupby('categoria')['valor_liquido'].apply(calcular_percentis).round(2)
print(percentis_categoria)

# 12. CROSSTAB E PIVOT_TABLE AVANÇADOS
print("\n12. CROSSTAB E PIVOT_TABLE")
print("-" * 30)

# Crosstab com percentuais
print("12.1 Crosstab - Canal vs Região (percentual):")
crosstab_pct = pd.crosstab(df['canal'], df['regiao'], normalize='index') * 100
print(crosstab_pct.round(1))

# Pivot table com múltiplas agregações
print("\n12.2 Pivot table complexa:")
pivot_complexa = df.pivot_table(
    values=['valor_liquido', 'quantidade'],
    index=['regiao'],
    columns=['canal'],
    aggfunc={
        'valor_liquido': ['sum', 'mean'],
        'quantidade': 'sum'
    },
    fill_value=0
).round(2)
print(pivot_complexa.head())

# 13. PERFORMANCE E OTIMIZAÇÃO
print("\n13. PERFORMANCE E OTIMIZAÇÃO")
print("-" * 35)

# Comparação de performance
import time

print("13.1 Comparação de métodos:")

# Método 1: apply
start = time.time()
result1 = df.groupby('vendedor').apply(lambda x: x['valor_liquido'].sum())
time1 = time.time() - start

# Método 2: agg
start = time.time()
result2 = df.groupby('vendedor')['valor_liquido'].sum()
time2 = time.time() - start

print(f"Apply: {time1:.4f}s")
print(f"Agg: {time2:.4f}s")
print(f"Agg é {time1/time2:.1f}x mais rápido")

# 14. CASOS DE USO PRÁTICOS PARA ENGENHARIA DE DADOS
print("\n14. CASOS DE USO PRÁTICOS")
print("-" * 30)

class AnalisadorVendas:
    """Classe para análises típicas de engenharia de dados"""
    
    def __init__(self, df):
        self.df = df
    
    def relatorio_executivo(self):
        """Relatório executivo de vendas"""
        return {
            'vendas_totais': self.df['valor_liquido'].sum(),
            'num_transacoes': len(self.df),
            'ticket_medio': self.df['valor_liquido'].mean(),
            'top_vendedor': self.df.groupby('vendedor')['valor_liquido'].sum().idxmax(),
            'melhor_regiao': self.df.groupby('regiao')['valor_liquido'].sum().idxmax(),
            'canal_principal': self.df.groupby('canal')['valor_liquido'].sum().idxmax()
        }
    
    def kpis_por_periodo(self, freq='M'):
        """KPIs por período"""
        df_temp = self.df.set_index('data')
        return df_temp.resample(freq).agg({
            'valor_liquido': ['sum', 'count', 'mean'],
            'vendedor': 'nunique',
            'produto': 'nunique'
        }).round(2)
    
    def analise_cohort_vendedores(self):
        """Análise de performance dos vendedores"""
        return self.df.groupby('vendedor').agg({
            'valor_liquido': ['count', 'sum', 'mean', 'std'],
            'desconto_pct': 'mean',
            'produto': 'nunique',
            'canal': lambda x: x.mode().iloc[0] if not x.mode().empty else 'N/A'
        }).round(2)

# Usando a classe
analisador = AnalisadorVendas(df)

print("14.1 Relatório Executivo:")
relatorio = analisador.relatorio_executivo()
for key, value in relatorio.items():
    if isinstance(value, (int, float)):
        print(f"{key}: {value:,.2f}")
    else:
        print(f"{key}: {value}")

print("\n14.2 KPIs Mensais:")
kpis_mensais = analisador.kpis_por_periodo('M')
print(kpis_mensais.head())

print("\n" + "=" * 60)
print("FIM DA AULA 06")
print("Próxima aula: Joins e merge de dados")
print("=" * 60)
