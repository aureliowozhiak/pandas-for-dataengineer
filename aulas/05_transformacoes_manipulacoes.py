"""
AULA 05: Transformações e Manipulações de Dados
===============================================

Nesta aula, vamos aprender técnicas avançadas de transformação e manipulação
de dados com pandas. Estas são habilidades essenciais para engenharia de dados.

Objetivos:
- Aplicar funções a dados (apply, map, applymap)
- Criar novas colunas derivadas
- Transformar dados categóricos
- Trabalhar com strings
- Reestruturar dados (melt, stack, unstack)
- Filtrar e selecionar dados avançado
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

print("=" * 60)
print("AULA 05: TRANSFORMAÇÕES E MANIPULAÇÕES")
print("=" * 60)

# Criando dataset para demonstrações
np.random.seed(42)
dados_vendas = {
    'data': pd.date_range('2024-01-01', periods=100, freq='D'),
    'vendedor': np.random.choice(['Ana Silva', 'João Santos', 'Maria Costa', 'Pedro Lima'], 100),
    'produto': np.random.choice(['Notebook', 'Mouse', 'Teclado', 'Monitor', 'Headset'], 100),
    'categoria': np.random.choice(['Eletrônicos', 'Acessórios'], 100),
    'quantidade': np.random.randint(1, 10, 100),
    'preco_unitario': np.random.uniform(50, 3000, 100).round(2),
    'regiao': np.random.choice(['Norte', 'Sul', 'Sudeste', 'Nordeste', 'Centro-Oeste'], 100),
    'cliente': np.random.choice(['Empresa A', 'Empresa B', 'Empresa C', 'Pessoa Física'], 100)
}

df = pd.DataFrame(dados_vendas)
print("Dataset de exemplo criado:")
print(df.head())
print(f"Shape: {df.shape}")

# 1. FUNÇÃO APPLY - APLICANDO FUNÇÕES A DADOS
print("\n1. FUNÇÃO APPLY - TRANSFORMAÇÕES CUSTOMIZADAS")
print("-" * 50)

# Apply em Series (por linha)
print("1.1 Apply em Series:")
df['valor_total'] = df['quantidade'] * df['preco_unitario']

# Função customizada para categorizar vendas
def categorizar_venda(valor):
    if valor < 500:
        return 'Pequena'
    elif valor < 2000:
        return 'Média'
    else:
        return 'Grande'

df['categoria_venda'] = df['valor_total'].apply(categorizar_venda)
print("Categorização de vendas:")
print(df['categoria_venda'].value_counts())

# Apply com lambda
df['desconto_5pct'] = df['preco_unitario'].apply(lambda x: x * 0.95)

# Apply em DataFrame (por linha ou coluna)
print("\n1.2 Apply em DataFrame:")
# Por coluna (axis=0) - padrão
stats_numericas = df[['quantidade', 'preco_unitario', 'valor_total']].apply(np.mean)
print("Médias por coluna:")
print(stats_numericas)

# Por linha (axis=1)
def calcular_margem(row):
    # Simulando cálculo de margem baseado no tipo de produto
    if row['categoria'] == 'Eletrônicos':
        return row['valor_total'] * 0.15  # 15% margem
    else:
        return row['valor_total'] * 0.25  # 25% margem

df['margem_estimada'] = df.apply(calcular_margem, axis=1)
print("\nPrimeiras margens calculadas:")
print(df[['produto', 'categoria', 'valor_total', 'margem_estimada']].head())

# 2. FUNÇÃO MAP - MAPEAMENTO DE VALORES
print("\n2. FUNÇÃO MAP - MAPEAMENTO DE VALORES")
print("-" * 40)

# Mapeamento com dicionário
mapa_regioes = {
    'Norte': 'N',
    'Sul': 'S',
    'Sudeste': 'SE',
    'Nordeste': 'NE',
    'Centro-Oeste': 'CO'
}

df['regiao_codigo'] = df['regiao'].map(mapa_regioes)
print("Mapeamento de regiões:")
print(df[['regiao', 'regiao_codigo']].drop_duplicates())

# Map com Series
vendedor_experiencia = pd.Series({
    'Ana Silva': 'Senior',
    'João Santos': 'Pleno',
    'Maria Costa': 'Senior',
    'Pedro Lima': 'Junior'
})

df['nivel_vendedor'] = df['vendedor'].map(vendedor_experiencia)
print("\nNível dos vendedores:")
print(df[['vendedor', 'nivel_vendedor']].drop_duplicates())

# 3. TRANSFORMAÇÕES DE STRING
print("\n3. TRANSFORMAÇÕES DE STRING")
print("-" * 35)

# Operações com strings
print("3.1 Operações básicas com strings:")
df['vendedor_upper'] = df['vendedor'].str.upper()
df['vendedor_lower'] = df['vendedor'].str.lower()
df['primeiro_nome'] = df['vendedor'].str.split().str[0]

print("Transformações de string:")
print(df[['vendedor', 'vendedor_upper', 'primeiro_nome']].drop_duplicates())

# Extrair informações com regex
print("\n3.2 Extração com regex:")
# Simulando códigos de produto
df['codigo_produto'] = df['produto'].str.replace(' ', '').str.upper() + '_' + \
                      df.index.astype(str).str.zfill(3)

# Extrair partes do código
df['prefixo_produto'] = df['codigo_produto'].str.extract(r'([A-Z]+)_')
print("Códigos e prefixos:")
print(df[['produto', 'codigo_produto', 'prefixo_produto']].head())

# 4. TRANSFORMAÇÕES DE DATA E TEMPO
print("\n4. TRANSFORMAÇÕES DE DATA E TEMPO")
print("-" * 40)

# Extrair componentes de data
df['ano'] = df['data'].dt.year
df['mes'] = df['data'].dt.month
df['dia_semana'] = df['data'].dt.day_name()
df['trimestre'] = df['data'].dt.quarter
df['dia_do_ano'] = df['data'].dt.dayofyear

print("Componentes de data:")
print(df[['data', 'ano', 'mes', 'dia_semana', 'trimestre']].head())

# Criar períodos customizados
def classificar_periodo(data):
    if data.month in [12, 1, 2]:
        return 'Verão'
    elif data.month in [3, 4, 5]:
        return 'Outono'
    elif data.month in [6, 7, 8]:
        return 'Inverno'
    else:
        return 'Primavera'

df['estacao'] = df['data'].apply(classificar_periodo)
print("\nVendas por estação:")
print(df['estacao'].value_counts())

# 5. TRANSFORMAÇÕES CONDICIONAIS
print("\n5. TRANSFORMAÇÕES CONDICIONAIS")
print("-" * 35)

# np.where - condicional simples
df['tipo_cliente'] = np.where(df['cliente'] == 'Pessoa Física', 'PF', 'PJ')

# np.select - múltiplas condições
conditions = [
    (df['valor_total'] < 500) & (df['tipo_cliente'] == 'PF'),
    (df['valor_total'] < 500) & (df['tipo_cliente'] == 'PJ'),
    (df['valor_total'] >= 500) & (df['valor_total'] < 2000),
    df['valor_total'] >= 2000
]

choices = ['PF_Pequena', 'PJ_Pequena', 'Média', 'Grande']

df['segmento_detalhado'] = np.select(conditions, choices, default='Outros')

print("Segmentação detalhada:")
print(df['segmento_detalhado'].value_counts())

# 6. BINNING - CRIANDO CATEGORIAS A PARTIR DE VALORES CONTÍNUOS
print("\n6. BINNING - CATEGORIZAÇÃO DE VALORES CONTÍNUOS")
print("-" * 50)

# pd.cut - intervalos de tamanho igual
df['faixa_preco'] = pd.cut(df['preco_unitario'], 
                          bins=5, 
                          labels=['Muito Baixo', 'Baixo', 'Médio', 'Alto', 'Muito Alto'])

print("Distribuição por faixa de preço:")
print(df['faixa_preco'].value_counts().sort_index())

# pd.qcut - intervalos com quantidades iguais
df['quartil_valor'] = pd.qcut(df['valor_total'], 
                             q=4, 
                             labels=['Q1', 'Q2', 'Q3', 'Q4'])

print("\nDistribuição por quartil de valor:")
print(df['quartil_valor'].value_counts().sort_index())

# 7. TRANSFORMAÇÕES AGREGADAS
print("\n7. TRANSFORMAÇÕES AGREGADAS")
print("-" * 35)

# Transform - mantém o tamanho original do DataFrame
df['media_vendedor'] = df.groupby('vendedor')['valor_total'].transform('mean')
df['rank_vendedor'] = df.groupby('vendedor')['valor_total'].transform('rank', ascending=False)

print("Estatísticas por vendedor:")
resultado_vendedor = df.groupby('vendedor').agg({
    'valor_total': ['count', 'sum', 'mean'],
    'media_vendedor': 'first'
}).round(2)
print(resultado_vendedor)

# 8. REESTRUTURAÇÃO DE DADOS - MELT
print("\n8. REESTRUTURAÇÃO - MELT (WIDE TO LONG)")
print("-" * 45)

# Criando dados wide para demonstrar melt
vendas_mensais = df.groupby(['vendedor', 'mes'])['valor_total'].sum().unstack(fill_value=0)
print("Dados wide (vendas por vendedor e mês):")
print(vendas_mensais.head())

# Melt - transformar colunas em linhas
vendas_long = vendas_mensais.reset_index().melt(
    id_vars=['vendedor'],
    var_name='mes',
    value_name='vendas'
)
print("\nDados long após melt:")
print(vendas_long.head(10))

# 9. PIVOT - LONG TO WIDE
print("\n9. PIVOT - LONG TO WIDE")
print("-" * 25)

# Criar tabela pivot
pivot_vendas = df.pivot_table(
    values='valor_total',
    index='vendedor',
    columns='categoria',
    aggfunc='sum',
    fill_value=0
)
print("Pivot table - vendas por vendedor e categoria:")
print(pivot_vendas)

# 10. OPERAÇÕES DE AGRUPAMENTO AVANÇADAS
print("\n10. AGRUPAMENTO AVANÇADO")
print("-" * 30)

# Múltiplas funções de agregação
agg_functions = {
    'valor_total': ['count', 'sum', 'mean', 'std'],
    'quantidade': ['sum', 'mean'],
    'preco_unitario': ['min', 'max']
}

resultado_detalhado = df.groupby(['regiao', 'categoria']).agg(agg_functions).round(2)
print("Agregação detalhada por região e categoria:")
print(resultado_detalhado.head())

# 11. WINDOW FUNCTIONS - FUNÇÕES DE JANELA
print("\n11. WINDOW FUNCTIONS")
print("-" * 25)

# Ordenar por data
df_sorted = df.sort_values('data').copy()

# Rolling statistics
df_sorted['media_movel_7d'] = df_sorted['valor_total'].rolling(window=7).mean()
df_sorted['total_acumulado'] = df_sorted['valor_total'].cumsum()

# Ranking
df_sorted['rank_diario'] = df_sorted.groupby('data')['valor_total'].rank(ascending=False)

print("Window functions:")
print(df_sorted[['data', 'valor_total', 'media_movel_7d', 'total_acumulado']].head(10))

# 12. TRANSFORMAÇÕES CUSTOMIZADAS COMPLEXAS
print("\n12. TRANSFORMAÇÕES CUSTOMIZADAS COMPLEXAS")
print("-" * 45)

def calcular_metricas_vendedor(group):
    """Calcular métricas complexas por vendedor"""
    result = pd.Series({
        'total_vendas': group['valor_total'].sum(),
        'ticket_medio': group['valor_total'].mean(),
        'num_produtos_diferentes': group['produto'].nunique(),
        'melhor_mes': group.groupby('mes')['valor_total'].sum().idxmax(),
        'consistencia': 1 - (group['valor_total'].std() / group['valor_total'].mean()),
        'crescimento': (group['valor_total'].tail(10).mean() - group['valor_total'].head(10).mean())
    })
    return result

metricas_vendedores = df.groupby('vendedor').apply(calcular_metricas_vendedor)
print("Métricas complexas por vendedor:")
print(metricas_vendedores.round(2))

# 13. PIPELINE DE TRANSFORMAÇÃO COMPLETO
print("\n13. PIPELINE DE TRANSFORMAÇÃO COMPLETO")
print("-" * 45)

class TransformadorVendas:
    """Pipeline de transformação para dados de vendas"""
    
    def __init__(self):
        self.transformacoes_aplicadas = []
    
    def log_transformacao(self, nome):
        self.transformacoes_aplicadas.append(nome)
        print(f"✓ {nome}")
    
    def transformar_completo(self, df):
        """Pipeline completo de transformações"""
        df_resultado = df.copy()
        
        # 1. Cálculos básicos
        df_resultado['valor_total'] = df_resultado['quantidade'] * df_resultado['preco_unitario']
        self.log_transformacao("Calculado valor total")
        
        # 2. Categorias derivadas
        df_resultado['categoria_valor'] = pd.cut(
            df_resultado['valor_total'], 
            bins=[0, 500, 2000, np.inf], 
            labels=['Baixo', 'Médio', 'Alto']
        )
        self.log_transformacao("Criadas categorias de valor")
        
        # 3. Features temporais
        df_resultado['mes'] = df_resultado['data'].dt.month
        df_resultado['trimestre'] = df_resultado['data'].dt.quarter
        df_resultado['dia_semana'] = df_resultado['data'].dt.dayofweek
        self.log_transformacao("Extraídas features temporais")
        
        # 4. Estatísticas por grupo
        df_resultado['media_vendedor'] = df_resultado.groupby('vendedor')['valor_total'].transform('mean')
        df_resultado['rank_vendedor'] = df_resultado.groupby('vendedor')['valor_total'].transform('rank')
        self.log_transformacao("Calculadas estatísticas por vendedor")
        
        # 5. Flags e indicadores
        df_resultado['fim_semana'] = df_resultado['dia_semana'].isin([5, 6])
        df_resultado['alta_quantidade'] = df_resultado['quantidade'] > df_resultado['quantidade'].quantile(0.75)
        self.log_transformacao("Criados flags e indicadores")
        
        return df_resultado
    
    def relatorio_transformacoes(self):
        print(f"\nTransformações aplicadas ({len(self.transformacoes_aplicadas)}):")
        for i, transformacao in enumerate(self.transformacoes_aplicadas, 1):
            print(f"{i}. {transformacao}")

# Aplicando o pipeline
transformador = TransformadorVendas()
df_transformado = transformador.transformar_completo(df)
transformador.relatorio_transformacoes()

print(f"\nDataset original: {df.shape}")
print(f"Dataset transformado: {df_transformado.shape}")
print(f"Novas colunas: {set(df_transformado.columns) - set(df.columns)}")

print("\n" + "=" * 60)
print("FIM DA AULA 05")
print("Próxima aula: Agrupamentos e agregações")
print("=" * 60)
