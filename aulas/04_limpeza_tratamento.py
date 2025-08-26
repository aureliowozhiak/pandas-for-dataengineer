"""
AULA 04: Limpeza e Tratamento de Dados
======================================

Nesta aula, vamos aprender técnicas essenciais para limpar e tratar dados.
Esta é uma das tarefas mais importantes e demoradas na engenharia de dados.

Objetivos:
- Identificar e tratar valores nulos
- Remover e tratar duplicatas
- Padronizar formatos de dados
- Tratar outliers
- Validar e corrigir tipos de dados
- Normalizar texto e dados categóricos
"""

import pandas as pd
import numpy as np
import re
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

print("=" * 60)
print("AULA 04: LIMPEZA E TRATAMENTO DE DADOS")
print("=" * 60)

# 1. CRIANDO DADOS SUJOS PARA DEMONSTRAÇÃO
print("\n1. CRIANDO DATASET COM PROBLEMAS COMUNS")
print("-" * 45)

# Simulando dados reais com problemas típicos
dados_sujos = {
    'id': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 10],  # ID duplicado
    'nome': ['João Silva', 'MARIA SANTOS', 'pedro costa', ' Ana Oliveira ', 
             'Carlos Rocha', '', 'Lucia Mendes', 'José Santos', 'Ana Oliveira', 
             'Roberto Lima', 'Roberto Lima'],
    'email': ['joao@email.com', 'MARIA@EMAIL.COM', 'pedro@', 'ana.oliveira@empresa.com.br',
              'carlos.rocha@empresa.com', np.nan, 'lucia@empresa.com', 'jose@email.com',
              'ana.oliveira@empresa.com.br', 'roberto@email.com', 'roberto@email.com'],
    'idade': [25, 35, -5, 28, 150, 30, np.nan, 45, 28, 32, 32],  # Idades inválidas
    'salario': ['5000', '6.500,00', '4500.0', '7000', 'N/A', '5500', '6800.50',
                '8000', '7000', '5200', '5200'],  # Formatos inconsistentes
    'data_contratacao': ['2022-01-15', '15/02/2021', '2023-03-10', '10-04-2020',
                        '2022/05/20', '', '2021-12-01', '2020-08-15', '2020-04-10',
                        '2023-01-20', '2023-01-20'],  # Formatos de data inconsistentes
    'departamento': ['vendas', 'MARKETING', 'ti', 'RH', 'Vendas', 'TI', 'rh',
                    'Marketing', 'RH', 'TI', 'TI']  # Inconsistência de case
}

df_sujo = pd.DataFrame(dados_sujos)
print("Dataset original com problemas:")
print(df_sujo)
print(f"\nShape: {df_sujo.shape}")
print(f"Tipos de dados:\n{df_sujo.dtypes}")

# 2. IDENTIFICANDO PROBLEMAS
print("\n2. IDENTIFICANDO PROBLEMAS NOS DADOS")
print("-" * 40)

def diagnosticar_problemas(df):
    """Função para diagnosticar problemas comuns em datasets"""
    print("DIAGNÓSTICO DE PROBLEMAS:")
    print("-" * 25)
    
    # Valores nulos
    nulos = df.isnull().sum()
    print(f"Valores nulos:\n{nulos[nulos > 0]}")
    
    # Duplicatas
    duplicatas_completas = df.duplicated().sum()
    print(f"\nLinhas completamente duplicadas: {duplicatas_completas}")
    
    # Duplicatas por coluna chave
    if 'id' in df.columns:
        duplicatas_id = df['id'].duplicated().sum()
        print(f"IDs duplicados: {duplicatas_id}")
    
    # Strings vazias
    for col in df.select_dtypes(include=['object']):
        vazias = (df[col] == '').sum()
        if vazias > 0:
            print(f"Strings vazias em '{col}': {vazias}")
    
    # Outliers numéricos (exemplo com idade)
    if 'idade' in df.columns:
        idades_invalidas = ((df['idade'] < 0) | (df['idade'] > 120)).sum()
        print(f"Idades inválidas: {idades_invalidas}")

diagnosticar_problemas(df_sujo)

# 3. TRATAMENTO DE VALORES NULOS
print("\n3. TRATAMENTO DE VALORES NULOS")
print("-" * 35)

df_limpo = df_sujo.copy()

print("Estratégias para valores nulos:")
print("- Remover registros com muitos nulos")
print("- Preencher com valores padrão")
print("- Preencher com médias/modas")
print("- Interpolação")

# Identificar strings vazias como nulos
df_limpo = df_limpo.replace('', np.nan)

# Tratar coluna idade (preencher com mediana)
idade_mediana = df_limpo['idade'].median()
print(f"\nIdade mediana para preenchimento: {idade_mediana}")
df_limpo['idade'] = df_limpo['idade'].fillna(idade_mediana)

# Tratar email (remover registros com email nulo para este exemplo)
print(f"Registros antes da remoção: {len(df_limpo)}")
df_limpo = df_limpo.dropna(subset=['email'])
print(f"Registros após remoção (email nulo): {len(df_limpo)}")

# Tratar nome (preencher com 'Nome não informado')
df_limpo['nome'] = df_limpo['nome'].fillna('Nome não informado')

print("Valores nulos após tratamento:")
print(df_limpo.isnull().sum())

# 4. TRATAMENTO DE DUPLICATAS
print("\n4. TRATAMENTO DE DUPLICATAS")
print("-" * 30)

print(f"Registros antes: {len(df_limpo)}")

# Remover duplicatas baseado no ID
df_limpo = df_limpo.drop_duplicates(subset=['id'], keep='first')
print(f"Registros após remoção de IDs duplicados: {len(df_limpo)}")

# Verificar duplicatas por nome e email
duplicatas_nome_email = df_limpo.duplicated(subset=['nome', 'email']).sum()
print(f"Duplicatas por nome+email: {duplicatas_nome_email}")

if duplicatas_nome_email > 0:
    df_limpo = df_limpo.drop_duplicates(subset=['nome', 'email'], keep='first')
    print(f"Registros após remoção: {len(df_limpo)}")

# 5. PADRONIZAÇÃO DE TEXTO
print("\n5. PADRONIZAÇÃO DE TEXTO")
print("-" * 30)

def limpar_texto(texto):
    """Função para limpar e padronizar texto"""
    if pd.isna(texto):
        return texto
    # Remover espaços extras, converter para title case
    return str(texto).strip().title()

def padronizar_departamento(dept):
    """Padronizar nomes de departamentos"""
    if pd.isna(dept):
        return dept
    
    dept = str(dept).strip().lower()
    
    mapeamento = {
        'vendas': 'Vendas',
        'marketing': 'Marketing',
        'ti': 'TI',
        'rh': 'RH',
        'financeiro': 'Financeiro'
    }
    
    return mapeamento.get(dept, dept.title())

print("Antes da padronização:")
print("Nomes únicos:", df_limpo['nome'].unique())
print("Departamentos únicos:", df_limpo['departamento'].unique())

# Aplicar limpeza
df_limpo['nome'] = df_limpo['nome'].apply(limpar_texto)
df_limpo['departamento'] = df_limpo['departamento'].apply(padronizar_departamento)
df_limpo['email'] = df_limpo['email'].str.lower().str.strip()

print("\nApós padronização:")
print("Nomes únicos:", df_limpo['nome'].unique())
print("Departamentos únicos:", df_limpo['departamento'].unique())

# 6. VALIDAÇÃO E CORREÇÃO DE EMAILS
print("\n6. VALIDAÇÃO DE EMAILS")
print("-" * 25)

def validar_email(email):
    """Validar formato básico de email"""
    if pd.isna(email):
        return False
    
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, email))

df_limpo['email_valido'] = df_limpo['email'].apply(validar_email)
emails_invalidos = df_limpo[~df_limpo['email_valido']]

print(f"Emails inválidos encontrados: {len(emails_invalidos)}")
if len(emails_invalidos) > 0:
    print("Emails inválidos:")
    print(emails_invalidos[['nome', 'email']])

# Remover emails inválidos
df_limpo = df_limpo[df_limpo['email_valido']].drop('email_valido', axis=1)
print(f"Registros após remoção de emails inválidos: {len(df_limpo)}")

# 7. TRATAMENTO DE OUTLIERS
print("\n7. TRATAMENTO DE OUTLIERS")
print("-" * 30)

def tratar_outliers_idade(idade):
    """Tratar idades inválidas"""
    if pd.isna(idade):
        return idade
    if idade < 18:
        return 18  # Idade mínima
    if idade > 70:
        return 70  # Idade máxima para aposentadoria
    return idade

print("Idades antes do tratamento:")
print(df_limpo['idade'].describe())

df_limpo['idade'] = df_limpo['idade'].apply(tratar_outliers_idade)

print("\nIdades após tratamento:")
print(df_limpo['idade'].describe())

# 8. PADRONIZAÇÃO DE FORMATOS NUMÉRICOS
print("\n8. PADRONIZAÇÃO DE SALÁRIOS")
print("-" * 35)

def limpar_salario(salario):
    """Converter salário para float"""
    if pd.isna(salario) or salario == 'N/A':
        return np.nan
    
    # Converter para string e limpar
    salario_str = str(salario)
    # Remover pontos (milhares) e converter vírgulas para pontos (decimais)
    salario_str = salario_str.replace('.', '').replace(',', '.')
    
    try:
        return float(salario_str)
    except:
        return np.nan

print("Salários antes da conversão:")
print(df_limpo['salario'].unique())

df_limpo['salario'] = df_limpo['salario'].apply(limpar_salario)

print("\nSalários após conversão:")
print(df_limpo['salario'].describe())

# 9. PADRONIZAÇÃO DE DATAS
print("\n9. PADRONIZAÇÃO DE DATAS")
print("-" * 30)

def padronizar_data(data_str):
    """Converter diferentes formatos de data para datetime"""
    if pd.isna(data_str) or data_str == '':
        return np.nan
    
    data_str = str(data_str).strip()
    
    # Diferentes formatos possíveis
    formatos = [
        '%Y-%m-%d',     # 2022-01-15
        '%d/%m/%Y',     # 15/02/2021
        '%d-%m-%Y',     # 10-04-2020
        '%Y/%m/%d',     # 2022/05/20
    ]
    
    for formato in formatos:
        try:
            return pd.to_datetime(data_str, format=formato)
        except:
            continue
    
    # Se nenhum formato funcionar, tentar parsing automático
    try:
        return pd.to_datetime(data_str)
    except:
        return np.nan

print("Datas antes da padronização:")
print(df_limpo['data_contratacao'].unique())

df_limpo['data_contratacao'] = df_limpo['data_contratacao'].apply(padronizar_data)

print("\nDatas após padronização:")
print(df_limpo['data_contratacao'].dt.strftime('%Y-%m-%d').unique())

# 10. VALIDAÇÕES FINAIS
print("\n10. VALIDAÇÕES FINAIS")
print("-" * 25)

def validar_dataset_limpo(df):
    """Validar qualidade do dataset limpo"""
    print("VALIDAÇÕES FINAIS:")
    print("-" * 18)
    
    # Completude
    completude = (1 - df.isnull().sum() / len(df)) * 100
    print(f"Completude média: {completude.mean():.1f}%")
    
    # Duplicatas
    duplicatas = df.duplicated().sum()
    print(f"Duplicatas restantes: {duplicatas}")
    
    # Consistência de tipos
    print("Tipos de dados finais:")
    for col, dtype in df.dtypes.items():
        print(f"  {col}: {dtype}")
    
    # Ranges válidos
    if 'idade' in df.columns:
        idades_validas = ((df['idade'] >= 18) & (df['idade'] <= 70)).all()
        print(f"Todas as idades válidas: {idades_validas}")
    
    if 'salario' in df.columns:
        salarios_positivos = (df['salario'] > 0).all()
        print(f"Todos os salários positivos: {salarios_positivos}")

validar_dataset_limpo(df_limpo)

print("\nDataset final limpo:")
print(df_limpo)

# 11. PIPELINE DE LIMPEZA REUTILIZÁVEL
print("\n11. PIPELINE DE LIMPEZA REUTILIZÁVEL")
print("-" * 40)

class LimpadorDados:
    """Classe para pipeline de limpeza de dados reutilizável"""
    
    def __init__(self):
        self.log_limpeza = []
    
    def log(self, mensagem):
        self.log_limpeza.append(mensagem)
        print(f"✓ {mensagem}")
    
    def limpar_dataset_funcionarios(self, df):
        """Pipeline específico para dados de funcionários"""
        df_resultado = df.copy()
        registros_iniciais = len(df_resultado)
        
        # 1. Tratar valores nulos
        df_resultado = df_resultado.replace('', np.nan)
        
        # 2. Remover duplicatas
        duplicatas_removidas = df_resultado.duplicated().sum()
        df_resultado = df_resultado.drop_duplicates()
        if duplicatas_removidas > 0:
            self.log(f"Removidas {duplicatas_removidas} duplicatas")
        
        # 3. Padronizar texto
        for col in ['nome', 'departamento']:
            if col in df_resultado.columns:
                df_resultado[col] = df_resultado[col].apply(
                    lambda x: str(x).strip().title() if pd.notna(x) else x
                )
        
        # 4. Validar emails
        if 'email' in df_resultado.columns:
            emails_validos = df_resultado['email'].apply(validar_email)
            emails_invalidos = (~emails_validos).sum()
            if emails_invalidos > 0:
                self.log(f"Encontrados {emails_invalidos} emails inválidos")
        
        # 5. Tratar outliers
        if 'idade' in df_resultado.columns:
            df_resultado['idade'] = df_resultado['idade'].apply(tratar_outliers_idade)
        
        registros_finais = len(df_resultado)
        self.log(f"Limpeza concluída: {registros_iniciais} → {registros_finais} registros")
        
        return df_resultado
    
    def relatorio_limpeza(self):
        """Gerar relatório das operações de limpeza"""
        print("\nRELATÓRIO DE LIMPEZA:")
        print("-" * 25)
        for item in self.log_limpeza:
            print(f"• {item}")

# Demonstração do pipeline
limpador = LimpadorDados()
df_pipeline = limpador.limpar_dataset_funcionarios(df_sujo)
limpador.relatorio_limpeza()

# 12. BOAS PRÁTICAS PARA ENGENHARIA DE DADOS
print("\n12. BOAS PRÁTICAS PARA LIMPEZA DE DADOS")
print("-" * 45)

boas_praticas = [
    "✓ Sempre manter backup dos dados originais",
    "✓ Documentar todas as transformações aplicadas",
    "✓ Criar funções reutilizáveis para limpeza",
    "✓ Validar dados após cada etapa de limpeza",
    "✓ Estabelecer regras de negócio claras",
    "✓ Automatizar validações com testes",
    "✓ Monitorar qualidade dos dados continuamente",
    "✓ Tratar casos especiais e exceções",
    "✓ Usar logging para rastrear mudanças",
    "✓ Criar relatórios de qualidade de dados"
]

for pratica in boas_praticas:
    print(pratica)

print("\n" + "=" * 60)
print("FIM DA AULA 04")
print("Próxima aula: Transformações e manipulações")
print("=" * 60)
