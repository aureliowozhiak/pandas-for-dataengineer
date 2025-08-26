"""
AULA 10: Performance e Otimização
=================================

Nesta aula, vamos aprender técnicas para otimizar performance em pandas.
Essencial para trabalhar com grandes volumes de dados na engenharia de dados.

Objetivos:
- Entender bottlenecks de performance
- Otimizar tipos de dados
- Usar vectorização vs loops
- Aplicar chunking para grandes datasets
- Configurar pandas para melhor performance
- Monitorar uso de memória
"""

import pandas as pd
import numpy as np
import time
import psutil
import os
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

print("=" * 60)
print("AULA 10: PERFORMANCE E OTIMIZAÇÃO")
print("=" * 60)

# 1. CRIANDO DATASET GRANDE PARA TESTES
print("\n1. CRIANDO DATASET PARA TESTES DE PERFORMANCE")
print("-" * 50)

def criar_dataset_grande(n_rows=100000):
    """Criar dataset grande para testes de performance"""
    np.random.seed(42)
    
    print(f"Criando dataset com {n_rows:,} registros...")
    
    data = {
        'id': range(n_rows),
        'categoria': np.random.choice(['A', 'B', 'C', 'D', 'E'], n_rows),
        'subcategoria': np.random.choice([f'Sub{i}' for i in range(20)], n_rows),
        'valor': np.random.uniform(10, 10000, n_rows),
        'quantidade': np.random.randint(1, 100, n_rows),
        'data': pd.date_range('2020-01-01', periods=n_rows, freq='H'),
        'descricao': [f'Produto {i}' for i in np.random.randint(1, 1000, n_rows)],
        'ativo': np.random.choice([True, False], n_rows),
        'score': np.random.normal(50, 15, n_rows)
    }
    
    df = pd.DataFrame(data)
    print(f"Dataset criado: {df.shape}")
    
    return df

# Criando dataset de teste
df_grande = criar_dataset_grande(100000)

# 2. MONITORAMENTO DE MEMÓRIA
print("\n2. MONITORAMENTO DE MEMÓRIA")
print("-" * 30)

def monitorar_memoria():
    """Monitorar uso de memória atual"""
    process = psutil.Process(os.getpid())
    memoria_mb = process.memory_info().rss / 1024 / 1024
    return memoria_mb

def info_memoria_df(df):
    """Informações detalhadas de memória do DataFrame"""
    print("INFORMAÇÕES DE MEMÓRIA:")
    print("-" * 25)
    
    memoria_total = df.memory_usage(deep=True).sum() / 1024 / 1024
    print(f"Memória total do DataFrame: {memoria_total:.2f} MB")
    
    print("\nMemória por coluna:")
    for col in df.columns:
        memoria_col = df[col].memory_usage(deep=True) / 1024 / 1024
        print(f"{col}: {memoria_col:.2f} MB ({df[col].dtype})")
    
    print(f"\nMemória do processo Python: {monitorar_memoria():.2f} MB")

info_memoria_df(df_grande)

# 3. OTIMIZAÇÃO DE TIPOS DE DADOS
print("\n3. OTIMIZAÇÃO DE TIPOS DE DADOS")
print("-" * 35)

def otimizar_tipos(df):
    """Otimizar tipos de dados para reduzir uso de memória"""
    df_otimizado = df.copy()
    memoria_inicial = df_otimizado.memory_usage(deep=True).sum()
    
    print("Otimizando tipos de dados...")
    
    # Otimizar inteiros
    for col in df_otimizado.select_dtypes(include=['int64']).columns:
        col_min = df_otimizado[col].min()
        col_max = df_otimizado[col].max()
        
        if col_min >= 0:  # Unsigned
            if col_max < 255:
                df_otimizado[col] = df_otimizado[col].astype('uint8')
            elif col_max < 65535:
                df_otimizado[col] = df_otimizado[col].astype('uint16')
            elif col_max < 4294967295:
                df_otimizado[col] = df_otimizado[col].astype('uint32')
        else:  # Signed
            if col_min > -128 and col_max < 127:
                df_otimizado[col] = df_otimizado[col].astype('int8')
            elif col_min > -32768 and col_max < 32767:
                df_otimizado[col] = df_otimizado[col].astype('int16')
            elif col_min > -2147483648 and col_max < 2147483647:
                df_otimizado[col] = df_otimizado[col].astype('int32')
    
    # Otimizar floats
    for col in df_otimizado.select_dtypes(include=['float64']).columns:
        df_otimizado[col] = df_otimizado[col].astype('float32')
    
    # Converter strings repetitivas para category
    for col in df_otimizado.select_dtypes(include=['object']).columns:
        num_unique = df_otimizado[col].nunique()
        num_total = len(df_otimizado)
        
        if num_unique / num_total < 0.5:  # Se menos de 50% são únicos
            df_otimizado[col] = df_otimizado[col].astype('category')
    
    memoria_final = df_otimizado.memory_usage(deep=True).sum()
    reducao = (memoria_inicial - memoria_final) / memoria_inicial * 100
    
    print(f"Redução de memória: {reducao:.1f}%")
    print(f"Antes: {memoria_inicial/1024/1024:.2f} MB")
    print(f"Depois: {memoria_final/1024/1024:.2f} MB")
    
    return df_otimizado

# Aplicando otimização
print("Dataset original:")
info_memoria_df(df_grande.head(1000))  # Amostra pequena para demonstração

df_otimizado = otimizar_tipos(df_grande.head(1000))
print("\nDataset otimizado:")
info_memoria_df(df_otimizado)

# 4. VECTORIZAÇÃO VS LOOPS
print("\n4. VECTORIZAÇÃO VS LOOPS")
print("-" * 30)

# Criando dados para teste
df_teste = pd.DataFrame({
    'a': np.random.randint(1, 100, 10000),
    'b': np.random.randint(1, 100, 10000)
})

print("Comparando métodos de cálculo:")

# Método 1: Loop Python (LENTO)
def metodo_loop(df):
    resultado = []
    for i in range(len(df)):
        resultado.append(df.iloc[i]['a'] * df.iloc[i]['b'])
    return resultado

# Método 2: Apply (MÉDIO)
def metodo_apply(df):
    return df.apply(lambda row: row['a'] * row['b'], axis=1)

# Método 3: Vectorização (RÁPIDO)
def metodo_vectorizado(df):
    return df['a'] * df['b']

# Método 4: NumPy (MAIS RÁPIDO)
def metodo_numpy(df):
    return np.multiply(df['a'].values, df['b'].values)

# Testando performance
metodos = [
    ('Vectorização Pandas', metodo_vectorizado),
    ('NumPy', metodo_numpy),
    ('Apply', metodo_apply),
    # ('Loop Python', metodo_loop)  # Muito lento, descomentando se necessário
]

tempos = {}
for nome, metodo in metodos:
    start = time.time()
    resultado = metodo(df_teste)
    tempo = time.time() - start
    tempos[nome] = tempo
    print(f"{nome}: {tempo:.4f}s")

# Calculando speedup
tempo_base = tempos['Apply']
print(f"\nSpeedup em relação ao Apply:")
for nome, tempo in tempos.items():
    if nome != 'Apply':
        speedup = tempo_base / tempo
        print(f"{nome}: {speedup:.1f}x mais rápido")

# 5. CHUNKING PARA GRANDES DATASETS
print("\n5. CHUNKING PARA GRANDES DATASETS")
print("-" * 40)

def processar_em_chunks(arquivo_csv, chunk_size=10000):
    """Processar arquivo grande em pedaços"""
    print(f"Processando em chunks de {chunk_size:,} registros...")
    
    resultados = []
    chunk_count = 0
    
    # Simulando leitura de arquivo grande
    for chunk in pd.read_csv(arquivo_csv, chunksize=chunk_size):
        chunk_count += 1
        
        # Processamento do chunk
        chunk_processado = chunk.groupby('categoria')['valor'].sum()
        resultados.append(chunk_processado)
        
        print(f"Chunk {chunk_count} processado: {len(chunk)} registros")
    
    # Combinando resultados
    resultado_final = pd.concat(resultados).groupby(level=0).sum()
    print(f"Processamento concluído: {chunk_count} chunks")
    
    return resultado_final

# Simulando arquivo grande (salvando dataset para demonstrar)
arquivo_temp = 'temp_dataset.csv'
df_grande.head(50000).to_csv(arquivo_temp, index=False)

# Processando em chunks
try:
    resultado_chunks = processar_em_chunks(arquivo_temp, chunk_size=10000)
    print("Resultado por categoria:")
    print(resultado_chunks)
finally:
    # Limpando arquivo temporário
    if os.path.exists(arquivo_temp):
        os.remove(arquivo_temp)

# 6. CONFIGURAÇÕES DE PERFORMANCE
print("\n6. CONFIGURAÇÕES DE PERFORMANCE")
print("-" * 40)

print("Configurações atuais do pandas:")
print(f"Modo copy-on-write: {pd.get_option('mode.copy_on_write')}")
print(f"Precisão float: {pd.get_option('display.precision')}")
print(f"Max linhas display: {pd.get_option('display.max_rows')}")

# Configurações recomendadas para performance
print("\nConfigurações recomendadas:")
pd.set_option('mode.copy_on_write', True)  # Reduz cópias desnecessárias
pd.set_option('compute.use_bottleneck', True)  # Usa bottleneck para operações numéricas
pd.set_option('compute.use_numexpr', True)  # Usa numexpr para expressões complexas

print("✓ Configurações otimizadas aplicadas")

# 7. INDEXAÇÃO EFICIENTE
print("\n7. INDEXAÇÃO EFICIENTE")
print("-" * 25)

# Criando dados para teste de indexação
df_index_test = pd.DataFrame({
    'id': range(50000),
    'categoria': np.random.choice(['A', 'B', 'C'], 50000),
    'valor': np.random.randn(50000)
})

print("Testando performance de indexação:")

# Sem índice
start = time.time()
resultado1 = df_index_test[df_index_test['id'] == 25000]
tempo_sem_index = time.time() - start

# Com índice
df_com_index = df_index_test.set_index('id')
start = time.time()
resultado2 = df_com_index.loc[25000]
tempo_com_index = time.time() - start

print(f"Sem índice: {tempo_sem_index:.6f}s")
print(f"Com índice: {tempo_com_index:.6f}s")
print(f"Melhoria: {tempo_sem_index/tempo_com_index:.1f}x mais rápido")

# 8. OPERAÇÕES EFICIENTES COM STRINGS
print("\n8. OPERAÇÕES EFICIENTES COM STRINGS")
print("-" * 40)

# Criando dados de string para teste
df_strings = pd.DataFrame({
    'texto': [f'Produto {i}' for i in range(10000)]
})

print("Comparando operações de string:")

# Método 1: Operações pandas string (eficiente)
start = time.time()
resultado1 = df_strings['texto'].str.upper()
tempo_pandas_str = time.time() - start

# Método 2: Apply com função Python (menos eficiente)
start = time.time()
resultado2 = df_strings['texto'].apply(lambda x: x.upper())
tempo_apply = time.time() - start

print(f"Pandas .str: {tempo_pandas_str:.4f}s")
print(f"Apply: {tempo_apply:.4f}s")
print(f"Pandas .str é {tempo_apply/tempo_pandas_str:.1f}x mais rápido")

# 9. QUERY VS BOOLEAN INDEXING
print("\n9. QUERY VS BOOLEAN INDEXING")
print("-" * 35)

df_query_test = pd.DataFrame({
    'a': np.random.randint(1, 1000, 50000),
    'b': np.random.randint(1, 1000, 50000),
    'categoria': np.random.choice(['X', 'Y', 'Z'], 50000)
})

print("Comparando métodos de filtro:")

# Boolean indexing
start = time.time()
resultado1 = df_query_test[(df_query_test['a'] > 500) & (df_query_test['categoria'] == 'X')]
tempo_boolean = time.time() - start

# Query method
start = time.time()
resultado2 = df_query_test.query('a > 500 and categoria == "X"')
tempo_query = time.time() - start

print(f"Boolean indexing: {tempo_boolean:.4f}s")
print(f"Query method: {tempo_query:.4f}s")

if tempo_boolean < tempo_query:
    print(f"Boolean indexing é {tempo_query/tempo_boolean:.1f}x mais rápido")
else:
    print(f"Query method é {tempo_boolean/tempo_query:.1f}x mais rápido")

# 10. PARALLEL PROCESSING
print("\n10. PROCESSAMENTO PARALELO")
print("-" * 35)

# Exemplo com multiprocessing (conceitual)
def processar_chunk_paralelo(chunk):
    """Função para processar chunk em paralelo"""
    return chunk.groupby('categoria')['valor'].sum()

print("Exemplo de processamento paralelo:")
print("- Dividir dataset em chunks")
print("- Processar cada chunk em processo separado")
print("- Combinar resultados")
print("- Usar multiprocessing.Pool ou joblib")

# Simulação de divisão em chunks
num_chunks = 4
chunk_size = len(df_grande) // num_chunks

print(f"\nDividindo {len(df_grande):,} registros em {num_chunks} chunks de ~{chunk_size:,}")

chunks = []
for i in range(num_chunks):
    start_idx = i * chunk_size
    end_idx = start_idx + chunk_size if i < num_chunks - 1 else len(df_grande)
    chunk = df_grande.iloc[start_idx:end_idx]
    chunks.append(chunk)

print(f"Chunks criados: {[len(c) for c in chunks]}")

# 11. PROFILING E DEBUGGING
print("\n11. PROFILING E DEBUGGING")
print("-" * 30)

def profile_funcao(func, *args, **kwargs):
    """Fazer profiling de uma função"""
    import cProfile
    import io
    import pstats
    
    pr = cProfile.Profile()
    pr.enable()
    
    resultado = func(*args, **kwargs)
    
    pr.disable()
    
    # Capturar estatísticas
    s = io.StringIO()
    ps = pstats.Stats(pr, stream=s)
    ps.sort_stats('cumulative')
    ps.print_stats(10)  # Top 10 funções
    
    print("PROFILING RESULTS:")
    print("-" * 20)
    print(s.getvalue())
    
    return resultado

# Exemplo de profiling
def operacao_complexa(df):
    """Operação complexa para profiling"""
    resultado = df.groupby('categoria').agg({
        'valor': ['sum', 'mean', 'std'],
        'quantidade': 'sum'
    })
    return resultado

print("Fazendo profiling de operação complexa:")
# resultado_profile = profile_funcao(operacao_complexa, df_grande.head(10000))

# 12. MELHORES PRÁTICAS
print("\n12. MELHORES PRÁTICAS DE PERFORMANCE")
print("-" * 45)

melhores_praticas = [
    "✓ Use tipos de dados apropriados (int8 vs int64)",
    "✓ Converta strings repetitivas para category",
    "✓ Use vectorização em vez de loops",
    "✓ Configure índices para operações frequentes",
    "✓ Use chunking para datasets muito grandes",
    "✓ Evite operações desnecessárias de cópia",
    "✓ Use .loc e .iloc em vez de chaining",
    "✓ Configure pandas para usar bottleneck/numexpr",
    "✓ Monitore uso de memória regularmente",
    "✓ Use query() para filtros complexos",
    "✓ Considere Dask para datasets > RAM",
    "✓ Profile código para identificar gargalos"
]

for pratica in melhores_praticas:
    print(pratica)

# 13. CLASSE PARA MONITORAMENTO DE PERFORMANCE
print("\n13. CLASSE PARA MONITORAMENTO")
print("-" * 35)

class MonitorPerformance:
    """Classe para monitorar performance de operações pandas"""
    
    def __init__(self):
        self.historico = []
        self.memoria_inicial = self.get_memoria()
    
    def get_memoria(self):
        """Obter uso atual de memória"""
        process = psutil.Process(os.getpid())
        return process.memory_info().rss / 1024 / 1024
    
    def cronometrar(self, nome_operacao):
        """Decorator para cronometrar operações"""
        def decorator(func):
            def wrapper(*args, **kwargs):
                memoria_antes = self.get_memoria()
                start_time = time.time()
                
                resultado = func(*args, **kwargs)
                
                end_time = time.time()
                memoria_depois = self.get_memoria()
                
                self.historico.append({
                    'operacao': nome_operacao,
                    'tempo': end_time - start_time,
                    'memoria_antes': memoria_antes,
                    'memoria_depois': memoria_depois,
                    'memoria_delta': memoria_depois - memoria_antes,
                    'timestamp': datetime.now()
                })
                
                print(f"✓ {nome_operacao}: {end_time - start_time:.4f}s")
                
                return resultado
            return wrapper
        return decorator
    
    def relatorio(self):
        """Gerar relatório de performance"""
        if not self.historico:
            print("Nenhuma operação monitorada ainda")
            return
        
        print("\nRELATÓRIO DE PERFORMANCE")
        print("=" * 30)
        
        df_historico = pd.DataFrame(self.historico)
        
        print("Resumo das operações:")
        print(f"Total de operações: {len(df_historico)}")
        print(f"Tempo total: {df_historico['tempo'].sum():.4f}s")
        print(f"Tempo médio: {df_historico['tempo'].mean():.4f}s")
        print(f"Operação mais lenta: {df_historico.loc[df_historico['tempo'].idxmax(), 'operacao']}")
        
        print("\nDetalhes por operação:")
        for _, row in df_historico.iterrows():
            print(f"{row['operacao']}: {row['tempo']:.4f}s (Δ mem: {row['memoria_delta']:+.2f}MB)")

# Exemplo de uso
monitor = MonitorPerformance()

@monitor.cronometrar("Agrupamento por categoria")
def agrupar_dados(df):
    return df.groupby('categoria')['valor'].sum()

@monitor.cronometrar("Cálculo de estatísticas")
def calcular_stats(df):
    return df.describe()

# Executando operações monitoradas
resultado1 = agrupar_dados(df_grande.head(10000))
resultado2 = calcular_stats(df_grande.head(10000))

# Relatório final
monitor.relatorio()

print("\n" + "=" * 60)
print("FIM DA AULA 10")
print("Próxima aula: Técnicas avançadas")
print("=" * 60)
