"""
AULA 07: Joins e Merge de Dados
===============================

Nesta aula, vamos aprender a combinar dados de diferentes fontes usando pandas.
Esta é uma habilidade essencial para engenharia de dados e integração de datasets.

Objetivos:
- Entender diferentes tipos de joins (inner, left, right, outer)
- Usar merge() com diferentes estratégias
- Trabalhar com concat() para empilhar dados
- Resolver conflitos em joins
- Otimizar performance de joins
- Validar integridade após joins
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

print("=" * 60)
print("AULA 07: JOINS E MERGE DE DADOS")
print("=" * 60)

# 1. CRIANDO DATASETS RELACIONADOS
print("\n1. CRIANDO DATASETS RELACIONADOS")
print("-" * 35)

# Dataset principal - Vendas
np.random.seed(42)
vendas = pd.DataFrame({
    'venda_id': range(1, 101),
    'cliente_id': np.random.randint(1, 21, 100),
    'produto_id': np.random.randint(1, 11, 100),
    'vendedor_id': np.random.randint(1, 6, 100),
    'data_venda': pd.date_range('2024-01-01', periods=100, freq='D'),
    'quantidade': np.random.randint(1, 10, 100),
    'valor_unitario': np.random.uniform(50, 1000, 100).round(2)
})

# Dataset de Clientes
clientes = pd.DataFrame({
    'cliente_id': range(1, 21),
    'nome_cliente': [f'Cliente {i}' for i in range(1, 21)],
    'cidade': np.random.choice(['São Paulo', 'Rio de Janeiro', 'Belo Horizonte', 'Salvador', 'Brasília'], 20),
    'segmento': np.random.choice(['Corporativo', 'Varejo', 'Governo'], 20),
    'data_cadastro': pd.date_range('2023-01-01', periods=20, freq='15D')
})

# Dataset de Produtos
produtos = pd.DataFrame({
    'produto_id': range(1, 11),
    'nome_produto': ['Notebook', 'Mouse', 'Teclado', 'Monitor', 'Headset', 
                    'Webcam', 'Impressora', 'Scanner', 'Tablet', 'Smartphone'],
    'categoria': ['Eletrônicos', 'Acessórios', 'Acessórios', 'Eletrônicos', 'Acessórios',
                 'Acessórios', 'Eletrônicos', 'Eletrônicos', 'Eletrônicos', 'Eletrônicos'],
    'preco_sugerido': [2500, 85, 320, 899, 220, 180, 450, 380, 1200, 800]
})

# Dataset de Vendedores
vendedores = pd.DataFrame({
    'vendedor_id': range(1, 6),
    'nome_vendedor': ['Ana Silva', 'João Santos', 'Maria Costa', 'Pedro Lima', 'Carlos Rocha'],
    'regiao': ['Sudeste', 'Sul', 'Nordeste', 'Norte', 'Centro-Oeste'],
    'meta_mensal': [50000, 45000, 40000, 35000, 42000]
})

# Dataset adicional - alguns produtos sem vendas
produtos_extras = pd.DataFrame({
    'produto_id': range(11, 16),
    'nome_produto': ['Roteador', 'Switch', 'Cabo HDMI', 'Adaptador', 'Carregador'],
    'categoria': ['Rede', 'Rede', 'Acessórios', 'Acessórios', 'Acessórios'],
    'preco_sugerido': [280, 450, 25, 65, 95]
})

print("Datasets criados:")
print(f"Vendas: {vendas.shape}")
print(f"Clientes: {clientes.shape}")
print(f"Produtos: {produtos.shape}")
print(f"Vendedores: {vendedores.shape}")

print("\nPrimeiras linhas - Vendas:")
print(vendas.head())

# 2. INNER JOIN - INTERSEÇÃO
print("\n2. INNER JOIN - INTERSEÇÃO")
print("-" * 30)

# Join básico - vendas com clientes
vendas_clientes = pd.merge(vendas, clientes, on='cliente_id', how='inner')
print(f"Vendas: {len(vendas)} → Vendas + Clientes (inner): {len(vendas_clientes)}")

print("\nPrimeiras linhas do join:")
print(vendas_clientes[['venda_id', 'cliente_id', 'nome_cliente', 'cidade', 'valor_unitario']].head())

# Join múltiplo - adicionando produtos
vendas_completas = pd.merge(vendas_clientes, produtos, on='produto_id', how='inner')
print(f"Adicionando produtos: {len(vendas_completas)}")

# Join final - adicionando vendedores
vendas_full = pd.merge(vendas_completas, vendedores, on='vendedor_id', how='inner')
print(f"Dataset completo: {len(vendas_full)}")

print("\nDataset final com todas as informações:")
colunas_importantes = ['venda_id', 'nome_cliente', 'nome_produto', 'nome_vendedor', 'valor_unitario']
print(vendas_full[colunas_importantes].head())

# 3. LEFT JOIN - MANTER TODOS DA ESQUERDA
print("\n3. LEFT JOIN - MANTER TODOS DA ESQUERDA")
print("-" * 45)

# Criando dataset com alguns clientes sem vendas
clientes_expandidos = pd.concat([clientes, pd.DataFrame({
    'cliente_id': range(21, 26),
    'nome_cliente': [f'Cliente Novo {i}' for i in range(1, 6)],
    'cidade': ['Curitiba'] * 5,
    'segmento': ['Prospects'] * 5,
    'data_cadastro': [datetime.now()] * 5
})], ignore_index=True)

# Left join - manter todos os clientes
vendas_left = pd.merge(clientes_expandidos, vendas, on='cliente_id', how='left')
print(f"Clientes: {len(clientes_expandidos)} → Left join: {len(vendas_left)}")

# Verificar clientes sem vendas
clientes_sem_vendas = vendas_left[vendas_left['venda_id'].isna()]
print(f"\nClientes sem vendas: {len(clientes_sem_vendas)}")
print(clientes_sem_vendas[['cliente_id', 'nome_cliente', 'segmento']].drop_duplicates())

# 4. RIGHT JOIN - MANTER TODOS DA DIREITA
print("\n4. RIGHT JOIN - MANTER TODOS DA DIREITA")
print("-" * 45)

# Adicionando produtos extras ao dataset principal
todos_produtos = pd.concat([produtos, produtos_extras], ignore_index=True)

# Right join - manter todos os produtos
produtos_vendas = pd.merge(vendas, todos_produtos, on='produto_id', how='right')
print(f"Produtos: {len(todos_produtos)} → Right join: {len(produtos_vendas)}")

# Produtos sem vendas
produtos_sem_vendas = produtos_vendas[produtos_vendas['venda_id'].isna()]
print(f"\nProdutos sem vendas: {len(produtos_sem_vendas)}")
print(produtos_sem_vendas[['produto_id', 'nome_produto', 'categoria']].drop_duplicates())

# 5. OUTER JOIN - UNIÃO COMPLETA
print("\n5. OUTER JOIN - UNIÃO COMPLETA")
print("-" * 30)

# Outer join - todos os registros de ambos os lados
vendas_outer = pd.merge(vendas, todos_produtos, on='produto_id', how='outer')
print(f"Vendas: {len(vendas)} + Produtos: {len(todos_produtos)} → Outer: {len(vendas_outer)}")

print("\nResumo do outer join:")
print(f"Vendas com produtos: {(~vendas_outer['venda_id'].isna() & ~vendas_outer['nome_produto'].isna()).sum()}")
print(f"Vendas sem produto: {(~vendas_outer['venda_id'].isna() & vendas_outer['nome_produto'].isna()).sum()}")
print(f"Produtos sem vendas: {(vendas_outer['venda_id'].isna() & ~vendas_outer['nome_produto'].isna()).sum()}")

# 6. JOINS COM MÚLTIPLAS CHAVES
print("\n6. JOINS COM MÚLTIPLAS CHAVES")
print("-" * 35)

# Criando dataset com chave composta
vendas_detalhes = pd.DataFrame({
    'cliente_id': np.random.randint(1, 11, 50),
    'produto_id': np.random.randint(1, 6, 50),
    'avaliacao': np.random.randint(1, 6, 50),
    'comentario': [f'Comentário {i}' for i in range(1, 51)]
})

# Join com múltiplas chaves
vendas_com_avaliacoes = pd.merge(
    vendas, 
    vendas_detalhes, 
    on=['cliente_id', 'produto_id'], 
    how='left'
)
print(f"Join com chaves múltiplas: {len(vendas_com_avaliacoes)}")
print(f"Vendas com avaliação: {vendas_com_avaliacoes['avaliacao'].notna().sum()}")

# 7. JOINS COM COLUNAS DE NOMES DIFERENTES
print("\n7. JOINS COM COLUNAS DIFERENTES")
print("-" * 40)

# Dataset com nomes de colunas diferentes
regioes_vendedores = pd.DataFrame({
    'id_vendedor': range(1, 6),  # Nome diferente da chave
    'territorio': ['SP/RJ', 'RS/SC', 'BA/PE', 'AM/PA', 'MT/GO'],
    'bonus_pct': [0.05, 0.04, 0.045, 0.035, 0.042]
})

# Join especificando mapeamento de colunas
vendedores_bonus = pd.merge(
    vendedores, 
    regioes_vendedores, 
    left_on='vendedor_id', 
    right_on='id_vendedor',
    how='inner'
)
print("Join com colunas de nomes diferentes:")
print(vendedores_bonus[['vendedor_id', 'nome_vendedor', 'territorio', 'bonus_pct']])

# 8. CONCAT - EMPILHANDO DATAFRAMES
print("\n8. CONCAT - EMPILHANDO DATAFRAMES")
print("-" * 35)

# Criando vendas de diferentes períodos
vendas_q1 = vendas.head(25).copy()
vendas_q1['trimestre'] = 'Q1'

vendas_q2 = vendas.iloc[25:50].copy()
vendas_q2['trimestre'] = 'Q2'
vendas_q2['venda_id'] = vendas_q2['venda_id'] + 1000  # IDs diferentes

# Concatenação vertical (empilhar)
vendas_concatenadas = pd.concat([vendas_q1, vendas_q2], ignore_index=True)
print(f"Concat vertical: {len(vendas_q1)} + {len(vendas_q2)} = {len(vendas_concatenadas)}")

# Concatenação horizontal (lado a lado)
info_adicional = pd.DataFrame({
    'canal': np.random.choice(['Online', 'Loja'], 25),
    'promocao': np.random.choice([True, False], 25)
})

vendas_horizontal = pd.concat([vendas_q1.reset_index(drop=True), info_adicional], axis=1)
print(f"Concat horizontal: {vendas_horizontal.shape}")

# 9. MERGE COM SUFIXOS
print("\n9. MERGE COM SUFIXOS")
print("-" * 25)

# Datasets com colunas de mesmo nome
vendas_2023 = pd.DataFrame({
    'produto_id': range(1, 6),
    'vendas': np.random.randint(100, 1000, 5),
    'margem': np.random.uniform(0.1, 0.3, 5)
})

vendas_2024 = pd.DataFrame({
    'produto_id': range(1, 6),
    'vendas': np.random.randint(150, 1200, 5),
    'margem': np.random.uniform(0.12, 0.35, 5)
})

# Merge com sufixos para distinguir colunas
comparacao_anual = pd.merge(
    vendas_2023, 
    vendas_2024, 
    on='produto_id',
    suffixes=('_2023', '_2024')
)
print("Merge com sufixos:")
print(comparacao_anual)

# Calculando crescimento
comparacao_anual['crescimento_vendas'] = (
    (comparacao_anual['vendas_2024'] - comparacao_anual['vendas_2023']) / 
    comparacao_anual['vendas_2023'] * 100
).round(2)
print("\nCrescimento calculado:")
print(comparacao_anual[['produto_id', 'vendas_2023', 'vendas_2024', 'crescimento_vendas']])

# 10. VALIDAÇÃO DE JOINS
print("\n10. VALIDAÇÃO DE JOINS")
print("-" * 25)

def validar_join(df_original, df_resultado, chave_join):
    """Validar integridade do join"""
    print("VALIDAÇÃO DO JOIN:")
    print("-" * 18)
    
    # Verificar duplicação de chaves
    duplicatas_orig = df_original[chave_join].duplicated().sum()
    print(f"Chaves duplicadas no original: {duplicatas_orig}")
    
    # Verificar perda de registros
    registros_perdidos = len(df_original) - len(df_resultado)
    if registros_perdidos > 0:
        print(f"⚠️  Registros perdidos: {registros_perdidos}")
    else:
        print("✓ Nenhum registro perdido")
    
    # Verificar valores nulos introduzidos
    nulos_introduzidos = df_resultado.isnull().sum().sum() - df_original.isnull().sum().sum()
    if nulos_introduzidos > 0:
        print(f"⚠️  Valores nulos introduzidos: {nulos_introduzidos}")
    else:
        print("✓ Nenhum valor nulo introduzido")

# Validando um join
validar_join(vendas, vendas_clientes, 'cliente_id')

# 11. PERFORMANCE DE JOINS
print("\n11. PERFORMANCE DE JOINS")
print("-" * 25)

import time

# Criando datasets maiores para teste de performance
df_grande1 = pd.DataFrame({
    'id': range(10000),
    'valor': np.random.randn(10000)
})

df_grande2 = pd.DataFrame({
    'id': np.random.choice(range(10000), 5000, replace=False),
    'categoria': np.random.choice(['A', 'B', 'C'], 5000)
})

# Testando performance com e sem índice
print("11.1 Join sem índice:")
start = time.time()
resultado_sem_indice = pd.merge(df_grande1, df_grande2, on='id')
tempo_sem_indice = time.time() - start
print(f"Tempo: {tempo_sem_indice:.4f}s - Resultado: {len(resultado_sem_indice)} registros")

print("\n11.2 Join com índice:")
df_grande1_idx = df_grande1.set_index('id')
df_grande2_idx = df_grande2.set_index('id')

start = time.time()
resultado_com_indice = df_grande1_idx.join(df_grande2_idx, how='inner')
tempo_com_indice = time.time() - start
print(f"Tempo: {tempo_com_indice:.4f}s - Resultado: {len(resultado_com_indice)} registros")
print(f"Melhoria: {tempo_sem_indice/tempo_com_indice:.1f}x mais rápido")

# 12. CASOS ESPECIAIS E TROUBLESHOOTING
print("\n12. CASOS ESPECIAIS")
print("-" * 20)

# Problema: Chaves com tipos diferentes
vendas_str = vendas.copy()
vendas_str['cliente_id'] = vendas_str['cliente_id'].astype(str)

print("12.1 Problema - tipos diferentes:")
print(f"Vendas cliente_id: {vendas_str['cliente_id'].dtype}")
print(f"Clientes cliente_id: {clientes['cliente_id'].dtype}")

# Solução: converter tipos antes do join
vendas_str['cliente_id'] = vendas_str['cliente_id'].astype(int)
join_corrigido = pd.merge(vendas_str, clientes, on='cliente_id')
print(f"Join após correção: {len(join_corrigido)} registros")

# Problema: Chaves com valores nulos
vendas_com_nulos = vendas.copy()
vendas_com_nulos.loc[0:4, 'cliente_id'] = np.nan

print("\n12.2 Problema - valores nulos na chave:")
join_com_nulos = pd.merge(vendas_com_nulos, clientes, on='cliente_id', how='left')
nulos_resultado = join_com_nulos['nome_cliente'].isnull().sum()
print(f"Registros com nome_cliente nulo: {nulos_resultado}")

# 13. PIPELINE DE JOINS COMPLEXO
print("\n13. PIPELINE DE JOINS COMPLEXO")
print("-" * 35)

class PipelineJoins:
    """Pipeline para joins complexos com validação"""
    
    def __init__(self):
        self.log_operacoes = []
    
    def log(self, operacao, registros_antes, registros_depois):
        self.log_operacoes.append({
            'operacao': operacao,
            'registros_antes': registros_antes,
            'registros_depois': registros_depois,
            'diferenca': registros_depois - registros_antes
        })
        print(f"✓ {operacao}: {registros_antes} → {registros_depois}")
    
    def pipeline_vendas_completo(self, vendas, clientes, produtos, vendedores):
        """Pipeline completo de joins para análise de vendas"""
        resultado = vendas.copy()
        registros_inicial = len(resultado)
        
        # Join 1: Adicionar clientes
        resultado = pd.merge(resultado, clientes, on='cliente_id', how='left')
        self.log("Join Clientes", registros_inicial, len(resultado))
        
        # Join 2: Adicionar produtos
        resultado = pd.merge(resultado, produtos, on='produto_id', how='left')
        self.log("Join Produtos", registros_inicial, len(resultado))
        
        # Join 3: Adicionar vendedores
        resultado = pd.merge(resultado, vendedores, on='vendedor_id', how='left')
        self.log("Join Vendedores", registros_inicial, len(resultado))
        
        # Cálculos derivados
        resultado['valor_total'] = resultado['quantidade'] * resultado['valor_unitario']
        resultado['margem_produto'] = resultado['valor_unitario'] - resultado['preco_sugerido']
        
        return resultado
    
    def relatorio_pipeline(self):
        """Relatório das operações realizadas"""
        print("\nRELATÓRIO DO PIPELINE:")
        print("-" * 25)
        for op in self.log_operacoes:
            print(f"• {op['operacao']}: {op['diferenca']:+d} registros")

# Executando pipeline
pipeline = PipelineJoins()
vendas_pipeline = pipeline.pipeline_vendas_completo(vendas, clientes, produtos, vendedores)
pipeline.relatorio_pipeline()

print(f"\nDataset final: {vendas_pipeline.shape}")
print("Colunas disponíveis:", list(vendas_pipeline.columns))

print("\n" + "=" * 60)
print("FIM DA AULA 07")
print("Próxima aula: Séries temporais")
print("=" * 60)
