"""
AULA 12: Integração com Outras Ferramentas
==========================================

Nesta aula final, vamos aprender como integrar pandas com outras ferramentas
essenciais no ecossistema de engenharia de dados.

Objetivos:
- Integrar pandas com SQL databases
- Trabalhar com Apache Parquet
- Conectar com APIs REST
- Usar pandas com Dask para big data
- Integrar com ferramentas de visualização
- Exportar para diferentes formatos
"""

import pandas as pd
import numpy as np
import json
import sqlite3
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

print("=" * 60)
print("AULA 12: INTEGRAÇÃO COM OUTRAS FERRAMENTAS")
print("=" * 60)

# 1. INTEGRAÇÃO COM SQL DATABASES
print("\n1. INTEGRAÇÃO COM SQL DATABASES")
print("-" * 35)

# Criando database SQLite para demonstração
def criar_database_exemplo():
    """Criar database SQLite com dados de exemplo"""
    conn = sqlite3.connect('exemplo.db')
    
    # Criando dados de exemplo
    np.random.seed(42)
    
    # Tabela de clientes
    clientes = pd.DataFrame({
        'cliente_id': range(1, 101),
        'nome': [f'Cliente {i}' for i in range(1, 101)],
        'email': [f'cliente{i}@email.com' for i in range(1, 101)],
        'cidade': np.random.choice(['São Paulo', 'Rio de Janeiro', 'Belo Horizonte'], 100),
        'data_cadastro': pd.date_range('2023-01-01', periods=100, freq='D')
    })
    
    # Tabela de pedidos
    pedidos = pd.DataFrame({
        'pedido_id': range(1, 501),
        'cliente_id': np.random.randint(1, 101, 500),
        'valor': np.random.uniform(50, 1000, 500).round(2),
        'data_pedido': pd.date_range('2024-01-01', periods=500, freq='H'),
        'status': np.random.choice(['Pendente', 'Processando', 'Enviado', 'Entregue'], 500)
    })
    
    # Salvando no SQLite
    clientes.to_sql('clientes', conn, if_exists='replace', index=False)
    pedidos.to_sql('pedidos', conn, if_exists='replace', index=False)
    
    conn.close()
    print("✓ Database SQLite criado com sucesso")

# Criando database
criar_database_exemplo()

# Lendo dados do SQL
print("\n1.1 Lendo dados do SQL:")
conn = sqlite3.connect('exemplo.db')

# Leitura simples
df_clientes = pd.read_sql_query("SELECT * FROM clientes", conn)
print(f"Clientes carregados: {len(df_clientes)}")

# Query complexa com JOIN
query_complexa = """
SELECT 
    c.nome,
    c.cidade,
    COUNT(p.pedido_id) as total_pedidos,
    SUM(p.valor) as valor_total,
    AVG(p.valor) as ticket_medio
FROM clientes c
LEFT JOIN pedidos p ON c.cliente_id = p.cliente_id
GROUP BY c.cliente_id, c.nome, c.cidade
HAVING total_pedidos > 3
ORDER BY valor_total DESC
"""

df_analise = pd.read_sql_query(query_complexa, conn)
print(f"\nAnálise de clientes (com mais de 3 pedidos):")
print(df_analise.head())

# Escrevendo dados de volta para SQL
print("\n1.2 Escrevendo dados para SQL:")
# Criando nova análise
df_resumo = df_analise.copy()
df_resumo['categoria_cliente'] = pd.cut(
    df_resumo['valor_total'], 
    bins=3, 
    labels=['Bronze', 'Prata', 'Ouro']
)

# Salvando nova tabela
df_resumo.to_sql('analise_clientes', conn, if_exists='replace', index=False)
print("✓ Análise salva na tabela 'analise_clientes'")

conn.close()

# 2. TRABALHANDO COM PARQUET
print("\n2. TRABALHANDO COM PARQUET")
print("-" * 30)

# Criando dataset para demonstrar Parquet
df_grande = pd.DataFrame({
    'id': range(100000),
    'timestamp': pd.date_range('2024-01-01', periods=100000, freq='T'),
    'categoria': np.random.choice(['A', 'B', 'C', 'D'], 100000),
    'valor': np.random.uniform(0, 1000, 100000),
    'quantidade': np.random.randint(1, 100, 100000),
    'ativo': np.random.choice([True, False], 100000)
})

print(f"Dataset criado: {df_grande.shape}")

# Salvando em diferentes formatos para comparar
print("\n2.1 Comparando formatos de arquivo:")

import time
import os

# CSV
start = time.time()
df_grande.to_csv('dados_teste.csv', index=False)
tempo_csv_write = time.time() - start
tamanho_csv = os.path.getsize('dados_teste.csv') / 1024 / 1024

# Parquet
start = time.time()
df_grande.to_parquet('dados_teste.parquet', index=False)
tempo_parquet_write = time.time() - start
tamanho_parquet = os.path.getsize('dados_teste.parquet') / 1024 / 1024

print(f"CSV: {tempo_csv_write:.2f}s, {tamanho_csv:.2f} MB")
print(f"Parquet: {tempo_parquet_write:.2f}s, {tamanho_parquet:.2f} MB")
print(f"Parquet é {tamanho_csv/tamanho_parquet:.1f}x menor")

# Lendo de volta
print("\n2.2 Comparando velocidade de leitura:")

start = time.time()
df_csv = pd.read_csv('dados_teste.csv')
tempo_csv_read = time.time() - start

start = time.time()
df_parquet = pd.read_parquet('dados_teste.parquet')
tempo_parquet_read = time.time() - start

print(f"Leitura CSV: {tempo_csv_read:.2f}s")
print(f"Leitura Parquet: {tempo_parquet_read:.2f}s")
print(f"Parquet é {tempo_csv_read/tempo_parquet_read:.1f}x mais rápido")

# Limpando arquivos
os.remove('dados_teste.csv')
os.remove('dados_teste.parquet')

# 3. INTEGRAÇÃO COM APIs REST
print("\n3. INTEGRAÇÃO COM APIs REST")
print("-" * 30)

# Simulando uma API REST simples
class SimuladorAPI:
    """Simulador de API REST para demonstração"""
    
    def __init__(self):
        self.dados = {
            'usuarios': [
                {'id': i, 'nome': f'Usuario {i}', 'ativo': i % 2 == 0}
                for i in range(1, 11)
            ],
            'vendas': [
                {'id': i, 'usuario_id': (i % 10) + 1, 'valor': np.random.uniform(100, 1000)}
                for i in range(1, 51)
            ]
        }
    
    def get(self, endpoint):
        """Simular GET request"""
        if endpoint in self.dados:
            return {'status': 'success', 'data': self.dados[endpoint]}
        return {'status': 'error', 'message': 'Endpoint não encontrado'}

# Usando o simulador
api = SimuladorAPI()

print("3.1 Consumindo dados de API:")
response_usuarios = api.get('usuarios')
if response_usuarios['status'] == 'success':
    df_usuarios_api = pd.DataFrame(response_usuarios['data'])
    print(f"Usuários da API: {len(df_usuarios_api)}")
    print(df_usuarios_api.head())

response_vendas = api.get('vendas')
if response_vendas['status'] == 'success':
    df_vendas_api = pd.DataFrame(response_vendas['data'])
    print(f"\nVendas da API: {len(df_vendas_api)}")
    
    # Juntando dados da API
    df_api_completo = pd.merge(df_vendas_api, df_usuarios_api, 
                              left_on='usuario_id', right_on='id', 
                              suffixes=('_venda', '_usuario'))
    print("Dados combinados da API:")
    print(df_api_completo[['nome', 'valor', 'ativo']].head())

# 4. PANDAS COM DASK (Big Data)
print("\n4. PANDAS COM DASK (Big Data)")
print("-" * 35)

print("4.1 Conceitos do Dask:")
print("- Dask DataFrame é como pandas, mas para dados que não cabem na memória")
print("- Operações lazy (só executam quando necessário)")
print("- Paralelização automática")
print("- API similar ao pandas")

# Exemplo conceitual (sem instalar Dask)
exemplo_dask = """
import dask.dataframe as dd

# Lendo arquivo grande
df_dask = dd.read_csv('arquivo_muito_grande.csv')

# Operações lazy (não executam ainda)
resultado = df_dask.groupby('categoria').valor.sum()

# Executar computação
resultado_final = resultado.compute()
"""

print("\n4.2 Exemplo de código Dask:")
print(exemplo_dask)

# 5. INTEGRAÇÃO COM FERRAMENTAS DE VISUALIZAÇÃO
print("\n5. INTEGRAÇÃO COM VISUALIZAÇÃO")
print("-" * 40)

# Preparando dados para visualização
df_viz = pd.DataFrame({
    'mes': pd.date_range('2024-01-01', periods=12, freq='M'),
    'vendas': np.random.uniform(1000, 5000, 12),
    'regiao': np.repeat(['Norte', 'Sul', 'Sudeste', 'Nordeste'], 3)
})

print("5.1 Dados para visualização:")
print(df_viz.head())

# Preparando dados para diferentes ferramentas
print("\n5.2 Formatos para diferentes ferramentas:")

# Para Matplotlib/Seaborn
dados_matplotlib = df_viz.pivot(index='mes', columns='regiao', values='vendas')
print("Formato para Matplotlib (pivot):")
print(dados_matplotlib.head())

# Para Plotly
dados_plotly = df_viz.to_dict('records')
print(f"\nFormato para Plotly (dict): {len(dados_plotly)} registros")

# Para D3.js (JSON)
dados_d3 = df_viz.to_json(orient='records', date_format='iso')
print(f"Formato para D3.js (JSON): {len(dados_d3)} caracteres")

# 6. EXPORTAÇÃO PARA MÚLTIPLOS FORMATOS
print("\n6. EXPORTAÇÃO PARA MÚLTIPLOS FORMATOS")
print("-" * 45)

# Dataset de exemplo
df_export = pd.DataFrame({
    'produto': ['Produto A', 'Produto B', 'Produto C'],
    'vendas_2023': [1000, 1500, 800],
    'vendas_2024': [1200, 1300, 900],
    'categoria': ['Eletrônicos', 'Casa', 'Esporte']
})

class ExportadorMultiplo:
    """Classe para exportar dados em múltiplos formatos"""
    
    def __init__(self, df, nome_base='dados'):
        self.df = df
        self.nome_base = nome_base
        self.formatos_exportados = []
    
    def para_csv(self, separador=','):
        """Exportar para CSV"""
        arquivo = f"{self.nome_base}.csv"
        self.df.to_csv(arquivo, sep=separador, index=False)
        self.formatos_exportados.append(('CSV', arquivo))
        return self
    
    def para_excel(self, multiplas_abas=False):
        """Exportar para Excel"""
        arquivo = f"{self.nome_base}.xlsx"
        
        if multiplas_abas and len(self.df.select_dtypes(include=['object']).columns) > 0:
            # Criar abas por categoria se possível
            col_categoria = self.df.select_dtypes(include=['object']).columns[0]
            
            with pd.ExcelWriter(arquivo) as writer:
                for categoria in self.df[col_categoria].unique():
                    df_categoria = self.df[self.df[col_categoria] == categoria]
                    df_categoria.to_excel(writer, sheet_name=str(categoria)[:31], index=False)
        else:
            self.df.to_excel(arquivo, index=False)
        
        self.formatos_exportados.append(('Excel', arquivo))
        return self
    
    def para_json(self, formato='records'):
        """Exportar para JSON"""
        arquivo = f"{self.nome_base}.json"
        self.df.to_json(arquivo, orient=formato, indent=2)
        self.formatos_exportados.append(('JSON', arquivo))
        return self
    
    def para_html(self, incluir_css=True):
        """Exportar para HTML"""
        arquivo = f"{self.nome_base}.html"
        
        if incluir_css:
            css = """
            <style>
            table { border-collapse: collapse; width: 100%; }
            th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
            th { background-color: #f2f2f2; }
            tr:nth-child(even) { background-color: #f9f9f9; }
            </style>
            """
            html_content = css + self.df.to_html(index=False)
        else:
            html_content = self.df.to_html(index=False)
        
        with open(arquivo, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        self.formatos_exportados.append(('HTML', arquivo))
        return self
    
    def relatorio_exportacao(self):
        """Relatório dos formatos exportados"""
        print(f"\nArquivos exportados para '{self.nome_base}':")
        for formato, arquivo in self.formatos_exportados:
            tamanho = os.path.getsize(arquivo) / 1024
            print(f"  {formato}: {arquivo} ({tamanho:.2f} KB)")

# Usando o exportador
print("6.1 Exportação múltipla:")
exportador = ExportadorMultiplo(df_export, 'relatorio_vendas')
exportador.para_csv().para_excel().para_json().para_html()
exportador.relatorio_exportacao()

# 7. PIPELINE DE ETL COMPLETO
print("\n7. PIPELINE DE ETL COMPLETO")
print("-" * 30)

class PipelineETL:
    """Pipeline completo de ETL integrando múltiplas fontes"""
    
    def __init__(self, nome="ETL Pipeline"):
        self.nome = nome
        self.dados = {}
        self.logs = []
    
    def log(self, mensagem):
        """Adicionar log"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_msg = f"[{timestamp}] {mensagem}"
        self.logs.append(log_msg)
        print(log_msg)
    
    def extract_sql(self, db_path, query, nome_dataset):
        """Extrair dados do SQL"""
        self.log(f"Extraindo dados SQL: {nome_dataset}")
        conn = sqlite3.connect(db_path)
        self.dados[nome_dataset] = pd.read_sql_query(query, conn)
        conn.close()
        self.log(f"✓ {len(self.dados[nome_dataset])} registros extraídos")
        return self
    
    def extract_csv(self, arquivo, nome_dataset, **kwargs):
        """Extrair dados do CSV"""
        self.log(f"Extraindo dados CSV: {nome_dataset}")
        self.dados[nome_dataset] = pd.read_csv(arquivo, **kwargs)
        self.log(f"✓ {len(self.dados[nome_dataset])} registros extraídos")
        return self
    
    def transform_join(self, dataset1, dataset2, chave, tipo='inner', nome_resultado='joined'):
        """Transformar: fazer join entre datasets"""
        self.log(f"Fazendo join: {dataset1} + {dataset2}")
        df1 = self.dados[dataset1]
        df2 = self.dados[dataset2]
        
        resultado = pd.merge(df1, df2, on=chave, how=tipo)
        self.dados[nome_resultado] = resultado
        self.log(f"✓ Join concluído: {len(resultado)} registros")
        return self
    
    def transform_aggregate(self, dataset, group_by, agg_dict, nome_resultado='aggregated'):
        """Transformar: agregar dados"""
        self.log(f"Agregando dados: {dataset}")
        df = self.dados[dataset]
        
        resultado = df.groupby(group_by).agg(agg_dict).reset_index()
        self.dados[nome_resultado] = resultado
        self.log(f"✓ Agregação concluída: {len(resultado)} registros")
        return self
    
    def load_multiple(self, dataset, formatos=['csv', 'parquet'], prefixo='output'):
        """Carregar dados em múltiplos formatos"""
        self.log(f"Carregando dados: {dataset}")
        df = self.dados[dataset]
        
        for formato in formatos:
            arquivo = f"{prefixo}_{dataset}.{formato}"
            
            if formato == 'csv':
                df.to_csv(arquivo, index=False)
            elif formato == 'parquet':
                df.to_parquet(arquivo, index=False)
            elif formato == 'json':
                df.to_json(arquivo, orient='records', indent=2)
            
            self.log(f"✓ Salvo: {arquivo}")
        
        return self
    
    def relatorio_pipeline(self):
        """Relatório completo do pipeline"""
        print(f"\nRELATÓRIO: {self.nome}")
        print("=" * (len(self.nome) + 11))
        
        print("Datasets disponíveis:")
        for nome, df in self.dados.items():
            print(f"  {nome}: {df.shape}")
        
        print(f"\nLogs de execução ({len(self.logs)} entradas):")
        for log in self.logs[-10:]:  # Últimos 10 logs
            print(f"  {log}")

# Executando pipeline completo
print("7.1 Pipeline ETL completo:")
pipeline = PipelineETL("Pipeline Vendas")

# Extract
pipeline.extract_sql(
    'exemplo.db',
    'SELECT * FROM clientes',
    'clientes'
).extract_sql(
    'exemplo.db',
    'SELECT * FROM pedidos',
    'pedidos'
)

# Transform
pipeline.transform_join(
    'clientes', 'pedidos', 'cliente_id', 'inner', 'vendas_completas'
).transform_aggregate(
    'vendas_completas',
    ['cidade'],
    {'valor': ['sum', 'mean', 'count']},
    'vendas_por_cidade'
)

# Load
pipeline.load_multiple('vendas_por_cidade', ['csv', 'json'])

# Relatório
pipeline.relatorio_pipeline()

# 8. BOAS PRÁTICAS DE INTEGRAÇÃO
print("\n8. BOAS PRÁTICAS DE INTEGRAÇÃO")
print("-" * 40)

boas_praticas = [
    "✓ Use Parquet para arquivos grandes e frequentes",
    "✓ Implemente retry logic para APIs instáveis",
    "✓ Use chunking para datasets muito grandes",
    "✓ Configure timeouts adequados para conexões",
    "✓ Valide dados após cada etapa de integração",
    "✓ Use connection pooling para databases",
    "✓ Implemente logging detalhado",
    "✓ Considere Dask para processamento paralelo",
    "✓ Use formatos específicos para cada ferramenta",
    "✓ Monitore performance e uso de memória",
    "✓ Implemente tratamento de erros robusto",
    "✓ Documente formatos e schemas de dados"
]

for pratica in boas_praticas:
    print(pratica)

# 9. LIMPEZA DE ARQUIVOS TEMPORÁRIOS
print("\n9. LIMPEZA DE ARQUIVOS")
print("-" * 25)

arquivos_temp = [
    'exemplo.db',
    'relatorio_vendas.csv',
    'relatorio_vendas.xlsx', 
    'relatorio_vendas.json',
    'relatorio_vendas.html',
    'output_vendas_por_cidade.csv',
    'output_vendas_por_cidade.json'
]

arquivos_removidos = 0
for arquivo in arquivos_temp:
    try:
        if os.path.exists(arquivo):
            os.remove(arquivo)
            arquivos_removidos += 1
    except:
        pass

print(f"✓ {arquivos_removidos} arquivos temporários removidos")

print("\n" + "=" * 60)
print("FIM DA AULA 12 - CURSO COMPLETO!")
print("Parabéns! Você completou o curso de Pandas para Engenharia de Dados")
print("=" * 60)
