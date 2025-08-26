"""
AULA 02: Leitura e Escrita de Dados
===================================

Nesta aula, vamos aprender como ler e escrever dados em diferentes formatos.
Esta é uma habilidade fundamental para engenharia de dados.

Objetivos:
- Ler dados de CSV, JSON, Excel
- Entender parâmetros importantes na leitura
- Salvar dados em diferentes formatos
- Lidar com problemas comuns na leitura de dados
"""

import pandas as pd
import os

print("=" * 60)
print("AULA 02: LEITURA E ESCRITA DE DADOS")
print("=" * 60)

# 1. LENDO ARQUIVOS CSV
print("\n1. LENDO ARQUIVOS CSV")
print("-" * 30)

# Lendo o arquivo de vendas que criamos
try:
    df_vendas = pd.read_csv('data/vendas.csv')
    print("Arquivo vendas.csv carregado com sucesso!")
    print(f"Shape: {df_vendas.shape}")
    print("\nPrimeiras linhas:")
    print(df_vendas.head())
    
    print("\nTipos de dados:")
    print(df_vendas.dtypes)
    
except FileNotFoundError:
    print("Arquivo vendas.csv não encontrado. Verifique se está na pasta 'data/'")

# 2. PARÂMETROS IMPORTANTES DO READ_CSV
print("\n2. PARÂMETROS IMPORTANTES DO READ_CSV")
print("-" * 40)

# Demonstrando diferentes parâmetros
print("Parâmetros úteis:")
print("- sep: separador (padrão é vírgula)")
print("- encoding: codificação do arquivo")
print("- parse_dates: converter colunas para datetime")
print("- index_col: definir coluna como índice")
print("- usecols: ler apenas colunas específicas")
print("- skiprows: pular linhas")
print("- nrows: ler apenas N linhas")

# Exemplo com parâmetros
try:
    # Lendo com conversão de data
    df_vendas_dates = pd.read_csv('data/vendas.csv', 
                                 parse_dates=['data'])
    print("\nLendo com parse_dates:")
    print(df_vendas_dates.dtypes)
    
    # Lendo apenas algumas colunas
    df_vendas_subset = pd.read_csv('data/vendas.csv', 
                                  usecols=['data', 'vendedor', 'quantidade'])
    print(f"\nLendo apenas algumas colunas: {df_vendas_subset.shape}")
    print(df_vendas_subset.head(3))
    
except FileNotFoundError:
    print("Arquivo não encontrado para demonstração de parâmetros")

# 3. LENDO ARQUIVOS JSON
print("\n3. LENDO ARQUIVOS JSON")
print("-" * 30)

try:
    df_funcionarios = pd.read_json('data/funcionarios.json')
    print("Arquivo funcionarios.json carregado!")
    print(df_funcionarios.head())
    
    print(f"\nShape: {df_funcionarios.shape}")
    print("Tipos de dados:")
    print(df_funcionarios.dtypes)
    
except FileNotFoundError:
    print("Arquivo funcionarios.json não encontrado")

# 4. LENDO DADOS COM PROBLEMAS COMUNS
print("\n4. LIDANDO COM PROBLEMAS COMUNS")
print("-" * 40)

# Criando um CSV com problemas para demonstrar
dados_problematicos = """nome;idade;salario;observacoes
João Silva;28;5000,50;Funcionário exemplar
Maria Santos;35;;Sem informação de salário
;42;7500,00;Nome em branco
Pedro Costa;29;6000;Normal
Ana Oliveira;31;5500,75;Promoção recente"""

# Salvando temporariamente
with open('temp_problemas.csv', 'w', encoding='utf-8') as f:
    f.write(dados_problematicos)

print("Lendo arquivo com problemas:")
df_problemas = pd.read_csv('temp_problemas.csv', 
                          sep=';',  # separador diferente
                          decimal=',',  # vírgula como decimal
                          na_values=['', 'Sem informação de salário'])  # valores nulos

print(df_problemas)
print("\nInformações sobre valores nulos:")
print(df_problemas.isnull().sum())

# Limpando arquivo temporário
os.remove('temp_problemas.csv')

# 5. ESCREVENDO DADOS
print("\n5. ESCREVENDO DADOS")
print("-" * 30)

# Criando um DataFrame de exemplo
dados_exemplo = {
    'produto': ['Produto A', 'Produto B', 'Produto C'],
    'vendas_q1': [1000, 1500, 800],
    'vendas_q2': [1200, 1300, 900],
    'categoria': ['Eletrônicos', 'Casa', 'Esporte']
}

df_exemplo = pd.DataFrame(dados_exemplo)
print("DataFrame de exemplo:")
print(df_exemplo)

# Salvando em CSV
df_exemplo.to_csv('output/vendas_trimestrais.csv', index=False)
print("\n✓ Salvo como CSV em 'output/vendas_trimestrais.csv'")

# Salvando em JSON
df_exemplo.to_json('output/vendas_trimestrais.json', 
                  orient='records', indent=2)
print("✓ Salvo como JSON em 'output/vendas_trimestrais.json'")

# Salvando em Excel (se xlsxwriter estiver instalado)
try:
    df_exemplo.to_excel('output/vendas_trimestrais.xlsx', 
                       index=False, sheet_name='Vendas')
    print("✓ Salvo como Excel em 'output/vendas_trimestrais.xlsx'")
except ImportError:
    print("! Para salvar Excel, instale: pip install xlsxwriter")

# 6. LENDO DE URLS E APIS
print("\n6. LENDO DE FONTES EXTERNAS")
print("-" * 35)

print("Pandas pode ler diretamente de URLs:")
print("df = pd.read_csv('https://exemplo.com/dados.csv')")
print("\nPara APIs JSON:")
print("df = pd.read_json('https://api.exemplo.com/dados')")

# 7. BOAS PRÁTICAS PARA ENGENHARIA DE DADOS
print("\n7. BOAS PRÁTICAS PARA ENGENHARIA DE DADOS")
print("-" * 50)

practices = [
    "✓ Sempre verificar o shape e tipos de dados após carregar",
    "✓ Usar parse_dates para colunas de data/hora",
    "✓ Especificar encoding para evitar problemas de caracteres",
    "✓ Usar chunksize para arquivos muito grandes",
    "✓ Validar dados após carregamento",
    "✓ Documentar formato e origem dos dados",
    "✓ Usar try/except para tratar erros de leitura",
    "✓ Configurar valores nulos apropriadamente"
]

for practice in practices:
    print(practice)

# 8. EXEMPLO PRÁTICO: PIPELINE SIMPLES
print("\n8. EXEMPLO PRÁTICO: PIPELINE SIMPLES")
print("-" * 45)

def processar_vendas(arquivo_entrada, arquivo_saida):
    """
    Pipeline simples para processar dados de vendas
    """
    try:
        # 1. Carregar dados
        df = pd.read_csv(arquivo_entrada, parse_dates=['data'])
        print(f"✓ Carregados {len(df)} registros")
        
        # 2. Calcular valor total
        df['valor_total'] = df['quantidade'] * df['preco_unitario']
        
        # 3. Adicionar informações derivadas
        df['mes'] = df['data'].dt.month
        df['ano'] = df['data'].dt.year
        
        # 4. Salvar resultado
        df.to_csv(arquivo_saida, index=False)
        print(f"✓ Dados processados salvos em {arquivo_saida}")
        
        return df
        
    except Exception as e:
        print(f"Erro no processamento: {e}")
        return None

# Executando o pipeline
try:
    df_processado = processar_vendas('data/vendas.csv', 
                                   'output/vendas_processadas.csv')
    if df_processado is not None:
        print("\nPrimeiras linhas do resultado:")
        print(df_processado.head(3))
except:
    print("Pipeline não executado - verifique os arquivos de dados")

print("\n" + "=" * 60)
print("FIM DA AULA 02")
print("Próxima aula: Exploração e análise descritiva")
print("=" * 60)
