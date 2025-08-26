"""
AULA 09: Pivot Tables e Reshape de Dados
========================================

Nesta aula, vamos dominar as técnicas de reestruturação de dados em pandas.
Estas são habilidades essenciais para transformar dados entre formatos wide e long.

Objetivos:
- Dominar pivot_table() e suas opções
- Usar melt() para transformar wide em long
- Aplicar stack() e unstack()
- Trabalhar com MultiIndex
- Criar crosstabs para análise categórica
- Resolver problemas comuns de reshape
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

print("=" * 60)
print("AULA 09: PIVOT TABLES E RESHAPE")
print("=" * 60)

# 1. CRIANDO DADOS DE EXEMPLO
print("\n1. CRIANDO DADOS DE EXEMPLO")
print("-" * 30)

# Dataset de vendas detalhado
np.random.seed(42)
n_records = 1000

dados_vendas = {
    'data': pd.date_range('2024-01-01', periods=n_records, freq='D'),
    'regiao': np.random.choice(['Norte', 'Sul', 'Sudeste', 'Nordeste'], n_records),
    'vendedor': np.random.choice(['Ana', 'João', 'Maria', 'Pedro', 'Carlos'], n_records),
    'produto': np.random.choice(['Notebook', 'Mouse', 'Teclado', 'Monitor'], n_records),
    'categoria': np.random.choice(['Eletrônicos', 'Acessórios'], n_records),
    'canal': np.random.choice(['Online', 'Loja', 'Telefone'], n_records),
    'quantidade': np.random.randint(1, 20, n_records),
    'preco_unitario': np.random.uniform(50, 3000, n_records).round(2),
    'desconto': np.random.uniform(0, 0.3, n_records).round(3)
}

df = pd.DataFrame(dados_vendas)
df['valor_bruto'] = df['quantidade'] * df['preco_unitario']
df['valor_liquido'] = df['valor_bruto'] * (1 - df['desconto'])
df['mes'] = df['data'].dt.month
df['trimestre'] = df['data'].dt.quarter
df['ano'] = df['data'].dt.year

print("Dataset criado:")
print(f"Shape: {df.shape}")
print("\nPrimeiras linhas:")
print(df[['data', 'regiao', 'vendedor', 'produto', 'valor_liquido']].head())

# 2. PIVOT TABLE BÁSICA
print("\n2. PIVOT TABLE BÁSICA")
print("-" * 25)

# Pivot simples - vendas por região e produto
print("2.1 Pivot simples:")
pivot_basico = df.pivot_table(
    values='valor_liquido',
    index='regiao',
    columns='produto',
    aggfunc='sum',
    fill_value=0
)
print(pivot_basico.round(2))

# Pivot com múltiplas métricas
print("\n2.2 Múltiplas métricas:")
pivot_multiplo = df.pivot_table(
    values='valor_liquido',
    index='regiao',
    columns='produto',
    aggfunc=['sum', 'mean', 'count'],
    fill_value=0
)
print("Shape do pivot múltiplo:", pivot_multiplo.shape)
print("\nPrimeiras linhas:")
print(pivot_multiplo.head())

# 3. PIVOT TABLE AVANÇADA
print("\n3. PIVOT TABLE AVANÇADA")
print("-" * 30)

# Múltiplos índices e colunas
print("3.1 Múltiplos índices e colunas:")
pivot_avancado = df.pivot_table(
    values=['valor_liquido', 'quantidade'],
    index=['regiao', 'vendedor'],
    columns=['produto', 'canal'],
    aggfunc={
        'valor_liquido': 'sum',
        'quantidade': 'mean'
    },
    fill_value=0
)
print("Shape:", pivot_avancado.shape)
print("Níveis do índice:", pivot_avancado.index.names)
print("Níveis das colunas:", pivot_avancado.columns.names)

# Acessando dados específicos
print("\n3.2 Acessando dados específicos:")
try:
    # Vendas de Notebook Online no Sudeste por Ana
    valor_especifico = pivot_avancado.loc[('Sudeste', 'Ana'), ('valor_liquido', 'Notebook', 'Online')]
    print(f"Vendas Ana/Sudeste/Notebook/Online: {valor_especifico:.2f}")
except KeyError:
    print("Combinação específica não encontrada")

# 4. PIVOT COM TOTAIS E SUBTOTAIS
print("\n4. PIVOT COM TOTAIS")
print("-" * 25)

# Pivot com margins (totais)
pivot_com_totais = df.pivot_table(
    values='valor_liquido',
    index='regiao',
    columns='trimestre',
    aggfunc='sum',
    fill_value=0,
    margins=True,  # Adiciona totais
    margins_name='TOTAL'
)
print("Pivot com totais por região e trimestre:")
print(pivot_com_totais.round(2))

# 5. MELT - TRANSFORMANDO WIDE EM LONG
print("\n5. MELT - WIDE PARA LONG")
print("-" * 30)

# Criando dados wide para demonstrar melt
vendas_mensais = df.groupby(['vendedor', 'mes'])['valor_liquido'].sum().unstack(fill_value=0)
print("5.1 Dados wide (vendas por vendedor e mês):")
print(vendas_mensais.head())

# Melt - transformar em formato long
vendas_long = vendas_mensais.reset_index().melt(
    id_vars=['vendedor'],
    var_name='mes',
    value_name='vendas'
)
print(f"\n5.2 Dados long após melt:")
print(f"Shape: {vendas_long.shape}")
print(vendas_long.head(10))

# Melt mais complexo
print("\n5.3 Melt complexo com múltiplas colunas ID:")
# Criando dados mais complexos
vendas_regiao_mes = df.groupby(['vendedor', 'regiao', 'mes'])['valor_liquido'].sum().unstack(fill_value=0)
vendas_regiao_mes_reset = vendas_regiao_mes.reset_index()

vendas_melted = vendas_regiao_mes_reset.melt(
    id_vars=['vendedor', 'regiao'],
    var_name='mes',
    value_name='vendas'
)
print("Melt com múltiplas ID vars:")
print(vendas_melted.head(10))

# 6. STACK E UNSTACK
print("\n6. STACK E UNSTACK")
print("-" * 25)

# Criando MultiIndex DataFrame
multi_df = df.set_index(['regiao', 'vendedor', 'produto'])['valor_liquido']
print("6.1 MultiIndex Series:")
print(multi_df.head(10))

# Stack - mover colunas para índice
print("\n6.2 Unstack - mover índice para colunas:")
unstacked = multi_df.unstack('produto', fill_value=0)
print("Shape após unstack:", unstacked.shape)
print(unstacked.head())

# Stack - mover colunas para índice
print("\n6.3 Stack - mover colunas para índice:")
stacked = unstacked.stack()
print("Shape após stack:", stacked.shape)
print(stacked.head(10))

# Unstack múltiplos níveis
print("\n6.4 Unstack múltiplos níveis:")
unstacked_multi = multi_df.unstack(['vendedor', 'produto'])
print("Shape com unstack múltiplo:", unstacked_multi.shape)

# 7. CROSSTAB - TABELAS DE CONTINGÊNCIA
print("\n7. CROSSTAB - TABELAS DE CONTINGÊNCIA")
print("-" * 40)

# Crosstab básica
print("7.1 Crosstab básica - Região vs Canal:")
crosstab_basica = pd.crosstab(df['regiao'], df['canal'])
print(crosstab_basica)

# Crosstab com valores
print("\n7.2 Crosstab com valores (vendas totais):")
crosstab_valores = pd.crosstab(
    df['regiao'], 
    df['canal'], 
    values=df['valor_liquido'],
    aggfunc='sum'
).fillna(0)
print(crosstab_valores.round(2))

# Crosstab com percentuais
print("\n7.3 Crosstab com percentuais:")
crosstab_pct = pd.crosstab(df['regiao'], df['canal'], normalize='index') * 100
print(crosstab_pct.round(1))

# Crosstab com totais
print("\n7.4 Crosstab com totais:")
crosstab_totais = pd.crosstab(
    df['regiao'], 
    df['canal'], 
    margins=True,
    margins_name='TOTAL'
)
print(crosstab_totais)

# 8. RESHAPE COMPLEXO COM MÚLTIPLAS VARIÁVEIS
print("\n8. RESHAPE COMPLEXO")
print("-" * 25)

# Dados com múltiplas métricas por período
dados_complexos = df.groupby(['regiao', 'mes']).agg({
    'valor_liquido': 'sum',
    'quantidade': 'sum',
    'valor_bruto': 'sum'
}).reset_index()

print("8.1 Dados agregados:")
print(dados_complexos.head())

# Melt com múltiplas variáveis de valor
dados_melted = dados_complexos.melt(
    id_vars=['regiao', 'mes'],
    value_vars=['valor_liquido', 'quantidade', 'valor_bruto'],
    var_name='metrica',
    value_name='valor'
)
print("\n8.2 Dados após melt complexo:")
print(dados_melted.head(10))

# Pivot do resultado melted
dados_pivot_back = dados_melted.pivot_table(
    values='valor',
    index=['regiao', 'mes'],
    columns='metrica'
).reset_index()
print("\n8.3 Dados após pivot de volta:")
print(dados_pivot_back.head())

# 9. TRABALHANDO COM MULTIINDEX
print("\n9. TRABALHANDO COM MULTIINDEX")
print("-" * 35)

# Criando pivot com MultiIndex
pivot_multi = df.pivot_table(
    values=['valor_liquido', 'quantidade'],
    index=['regiao', 'vendedor'],
    columns=['produto'],
    aggfunc='sum',
    fill_value=0
)

print("9.1 Estrutura MultiIndex:")
print("Níveis do índice:", pivot_multi.index.nlevels)
print("Níveis das colunas:", pivot_multi.columns.nlevels)
print("Shape:", pivot_multi.shape)

# Operações com MultiIndex
print("\n9.2 Operações por nível:")
# Soma por região (nível 0)
soma_regiao = pivot_multi.groupby(level=0).sum()
print("Soma por região:")
print(soma_regiao.head())

# Achatando MultiIndex
print("\n9.3 Achatando MultiIndex:")
pivot_flat = pivot_multi.copy()
pivot_flat.columns = ['_'.join(col).strip() for col in pivot_flat.columns.values]
pivot_flat = pivot_flat.reset_index()
print("Colunas achatadas:", list(pivot_flat.columns))

# 10. CASOS DE USO PRÁTICOS
print("\n10. CASOS DE USO PRÁTICOS")
print("-" * 30)

# Análise de performance por vendedor e período
print("10.1 Dashboard de vendas por vendedor:")
dashboard_vendedor = df.pivot_table(
    values=['valor_liquido', 'quantidade'],
    index='vendedor',
    columns='trimestre',
    aggfunc={
        'valor_liquido': 'sum',
        'quantidade': 'sum'
    },
    fill_value=0
)

# Calculando totais e médias
dashboard_vendedor[('valor_liquido', 'TOTAL')] = dashboard_vendedor['valor_liquido'].sum(axis=1)
dashboard_vendedor[('valor_liquido', 'MÉDIA')] = dashboard_vendedor['valor_liquido'].mean(axis=1)

print(dashboard_vendedor['valor_liquido'].round(2))

# Análise de mix de produtos por região
print("\n10.2 Mix de produtos por região (%):")
mix_produtos = pd.crosstab(df['regiao'], df['produto'], normalize='index') * 100
print(mix_produtos.round(1))

# 11. PIVOT DINÂMICO COM FILTROS
print("\n11. PIVOT DINÂMICO COM FILTROS")
print("-" * 35)

def criar_pivot_dinamico(df, filtros=None, valores='valor_liquido', 
                        indice='regiao', colunas='produto', agregacao='sum'):
    """Criar pivot table dinâmico com filtros"""
    
    # Aplicar filtros se especificados
    df_filtrado = df.copy()
    if filtros:
        for coluna, valor in filtros.items():
            if isinstance(valor, list):
                df_filtrado = df_filtrado[df_filtrado[coluna].isin(valor)]
            else:
                df_filtrado = df_filtrado[df_filtrado[coluna] == valor]
    
    # Criar pivot
    pivot = df_filtrado.pivot_table(
        values=valores,
        index=indice,
        columns=colunas,
        aggfunc=agregacao,
        fill_value=0
    )
    
    return pivot, len(df_filtrado)

# Exemplos de uso
print("11.1 Pivot para vendas online apenas:")
pivot_online, registros = criar_pivot_dinamico(
    df, 
    filtros={'canal': 'Online'},
    valores='valor_liquido',
    indice='regiao',
    colunas='produto'
)
print(f"Registros filtrados: {registros}")
print(pivot_online.round(2))

print("\n11.2 Pivot para múltiplos vendedores:")
pivot_vendedores, registros = criar_pivot_dinamico(
    df,
    filtros={'vendedor': ['Ana', 'João']},
    valores='quantidade',
    indice='vendedor',
    colunas='mes'
)
print(f"Registros filtrados: {registros}")
print(pivot_vendedores.head())

# 12. PERFORMANCE E OTIMIZAÇÃO
print("\n12. PERFORMANCE E OTIMIZAÇÃO")
print("-" * 35)

import time

# Comparando métodos
print("12.1 Comparação de performance:")

# Método 1: Pivot table
start = time.time()
resultado1 = df.pivot_table(
    values='valor_liquido',
    index='regiao',
    columns='produto',
    aggfunc='sum'
)
tempo1 = time.time() - start

# Método 2: GroupBy + Unstack
start = time.time()
resultado2 = df.groupby(['regiao', 'produto'])['valor_liquido'].sum().unstack(fill_value=0)
tempo2 = time.time() - start

print(f"Pivot table: {tempo1:.4f}s")
print(f"GroupBy + Unstack: {tempo2:.4f}s")
print(f"Diferença: {abs(tempo1-tempo2)/min(tempo1,tempo2)*100:.1f}%")

# 13. VALIDAÇÃO E TROUBLESHOOTING
print("\n13. VALIDAÇÃO E TROUBLESHOOTING")
print("-" * 40)

def validar_pivot(df_original, df_pivot, valor_col):
    """Validar integridade do pivot"""
    print("VALIDAÇÃO DO PIVOT:")
    print("-" * 20)
    
    # Verificar totais
    total_original = df_original[valor_col].sum()
    total_pivot = df_pivot.sum().sum()
    
    print(f"Total original: {total_original:.2f}")
    print(f"Total pivot: {total_pivot:.2f}")
    
    if abs(total_original - total_pivot) < 0.01:
        print("✓ Totais conferem")
    else:
        print("⚠️  Diferença nos totais")
    
    # Verificar dimensões
    registros_original = len(df_original)
    combinacoes_pivot = df_pivot.count().sum()
    
    print(f"Registros originais: {registros_original}")
    print(f"Combinações no pivot: {combinacoes_pivot}")

# Validando um pivot
validar_pivot(df, pivot_basico, 'valor_liquido')

# 14. CLASSE PARA ANÁLISE DE RESHAPE
print("\n14. CLASSE PARA ANÁLISE DE RESHAPE")
print("-" * 40)

class AnalisadorReshape:
    """Classe para análises de reshape e pivot"""
    
    def __init__(self, df):
        self.df = df
        self.pivots_criados = {}
    
    def criar_pivot_vendas(self, indice, colunas, valores='valor_liquido', 
                          agregacao='sum', nome=None):
        """Criar e armazenar pivot de vendas"""
        pivot = self.df.pivot_table(
            values=valores,
            index=indice,
            columns=colunas,
            aggfunc=agregacao,
            fill_value=0
        )
        
        if nome:
            self.pivots_criados[nome] = pivot
        
        return pivot
    
    def comparar_periodos(self, periodo_col='mes'):
        """Comparar vendas entre períodos"""
        pivot_periodo = self.criar_pivot_vendas(
            indice='regiao',
            colunas=periodo_col,
            nome='periodo'
        )
        
        # Calcular crescimento
        if pivot_periodo.shape[1] >= 2:
            cols = pivot_periodo.columns
            crescimento = (pivot_periodo[cols[-1]] - pivot_periodo[cols[0]]) / pivot_periodo[cols[0]] * 100
            crescimento = crescimento.fillna(0)
            
            return pivot_periodo, crescimento
        
        return pivot_periodo, None
    
    def analise_mix_produtos(self):
        """Análise do mix de produtos"""
        # Mix por região
        mix_regiao = pd.crosstab(self.df['regiao'], self.df['produto'], normalize='index') * 100
        
        # Produto dominante por região
        produto_dominante = mix_regiao.idxmax(axis=1)
        
        return mix_regiao, produto_dominante
    
    def relatorio_reshape(self):
        """Relatório completo de análises"""
        print("RELATÓRIO DE ANÁLISE RESHAPE")
        print("=" * 35)
        
        # Pivot básico
        pivot_basico = self.criar_pivot_vendas('regiao', 'produto')
        print(f"Vendas por região e produto:")
        print(pivot_basico.round(2))
        
        # Comparação temporal
        pivot_tempo, crescimento = self.comparar_periodos()
        if crescimento is not None:
            print(f"\nCrescimento por região (primeiro vs último mês):")
            for regiao, cresc in crescimento.items():
                print(f"{regiao}: {cresc:.1f}%")
        
        # Mix de produtos
        mix, dominante = self.analise_mix_produtos()
        print(f"\nProduto dominante por região:")
        for regiao, produto in dominante.items():
            print(f"{regiao}: {produto}")

# Usando a classe
analisador = AnalisadorReshape(df)
analisador.relatorio_reshape()

print("\n" + "=" * 60)
print("FIM DA AULA 09")
print("Próxima aula: Performance e otimização")
print("=" * 60)
