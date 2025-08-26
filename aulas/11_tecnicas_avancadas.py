"""
AULA 11: Técnicas Avançadas
===========================

Nesta aula, vamos explorar técnicas avançadas e recursos menos conhecidos do pandas.
Essencial para engenheiros de dados que querem dominar completamente a biblioteca.

Objetivos:
- Usar pipe() para pipelines funcionais
- Trabalhar com dados categóricos avançados
- Aplicar window functions complexas
- Usar accessor customizados
- Implementar validações de dados
- Técnicas de debugging avançado
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import re
import warnings
warnings.filterwarnings('ignore')

print("=" * 60)
print("AULA 11: TÉCNICAS AVANÇADAS")
print("=" * 60)

# 1. PIPE() - PIPELINES FUNCIONAIS
print("\n1. PIPE() - PIPELINES FUNCIONAIS")
print("-" * 35)

# Criando dataset para demonstrações
np.random.seed(42)
df = pd.DataFrame({
    'cliente_id': range(1, 1001),
    'nome': [f'Cliente {i}' for i in range(1, 1001)],
    'idade': np.random.randint(18, 80, 1000),
    'salario': np.random.uniform(2000, 20000, 1000),
    'categoria': np.random.choice(['Bronze', 'Prata', 'Ouro', 'Platina'], 1000),
    'data_cadastro': pd.date_range('2020-01-01', periods=1000, freq='D'),
    'score_credito': np.random.randint(300, 850, 1000),
    'ativo': np.random.choice([True, False], 1000, p=[0.8, 0.2])
})

print("Dataset criado:")
print(df.head())

# Funções para pipeline
def filtrar_ativos(df):
    """Filtrar apenas clientes ativos"""
    return df[df['ativo'] == True]

def calcular_faixa_etaria(df):
    """Adicionar faixa etária"""
    def classificar_idade(idade):
        if idade < 30:
            return 'Jovem'
        elif idade < 50:
            return 'Adulto'
        else:
            return 'Senior'
    
    df = df.copy()
    df['faixa_etaria'] = df['idade'].apply(classificar_idade)
    return df

def normalizar_score(df):
    """Normalizar score de crédito"""
    df = df.copy()
    df['score_normalizado'] = (df['score_credito'] - df['score_credito'].min()) / \
                             (df['score_credito'].max() - df['score_credito'].min())
    return df

def adicionar_segmentacao(df):
    """Adicionar segmentação baseada em múltiplas variáveis"""
    df = df.copy()
    
    conditions = [
        (df['salario'] >= 15000) & (df['score_credito'] >= 750),
        (df['salario'] >= 10000) & (df['score_credito'] >= 650),
        (df['salario'] >= 5000) & (df['score_credito'] >= 550),
    ]
    
    choices = ['Premium', 'Gold', 'Standard']
    df['segmento'] = np.select(conditions, choices, default='Basic')
    
    return df

# Pipeline tradicional (sem pipe)
print("\n1.1 Pipeline tradicional:")
df_resultado1 = filtrar_ativos(df)
df_resultado1 = calcular_faixa_etaria(df_resultado1)
df_resultado1 = normalizar_score(df_resultado1)
df_resultado1 = adicionar_segmentacao(df_resultado1)

print(f"Resultado pipeline tradicional: {df_resultado1.shape}")

# Pipeline com pipe() - mais elegante
print("\n1.2 Pipeline com pipe():")
df_resultado2 = (df
    .pipe(filtrar_ativos)
    .pipe(calcular_faixa_etaria)
    .pipe(normalizar_score)
    .pipe(adicionar_segmentacao)
)

print(f"Resultado pipeline com pipe: {df_resultado2.shape}")
print("\nSegmentação final:")
print(df_resultado2['segmento'].value_counts())

# 2. DADOS CATEGÓRICOS AVANÇADOS
print("\n2. DADOS CATEGÓRICOS AVANÇADOS")
print("-" * 35)

# Criando dados categóricos ordenados
categorias_ordenadas = ['Bronze', 'Prata', 'Ouro', 'Platina']
df['categoria_ordered'] = pd.Categorical(
    df['categoria'], 
    categories=categorias_ordenadas, 
    ordered=True
)

print("2.1 Categoria ordenada:")
print(df['categoria_ordered'].dtype)
print(df['categoria_ordered'].cat.categories)

# Operações com categorias ordenadas
print("\n2.2 Comparações com categorias ordenadas:")
clientes_premium = df[df['categoria_ordered'] >= 'Ouro']
print(f"Clientes Ouro/Platina: {len(clientes_premium)}")

# Adicionando nova categoria
print("\n2.3 Adicionando nova categoria:")
df['categoria_ordered'] = df['categoria_ordered'].cat.add_categories(['Diamante'])
print("Categorias após adição:", df['categoria_ordered'].cat.categories)

# Renomeando categorias
df['categoria_renamed'] = df['categoria_ordered'].cat.rename_categories({
    'Bronze': 'Básico',
    'Prata': 'Intermediário',
    'Ouro': 'Avançado',
    'Platina': 'Premium'
})

print("\n2.4 Categorias renomeadas:")
print(df['categoria_renamed'].value_counts())

# 3. WINDOW FUNCTIONS AVANÇADAS
print("\n3. WINDOW FUNCTIONS AVANÇADAS")
print("-" * 35)

# Ordenar por data para window functions
df_sorted = df.sort_values('data_cadastro').copy()

# Rolling com diferentes windows
print("3.1 Rolling windows customizados:")

# Rolling por tempo (não por número de registros)
df_sorted = df_sorted.set_index('data_cadastro')
df_sorted['media_salario_30d'] = df_sorted['salario'].rolling('30D').mean()
df_sorted['count_cadastros_7d'] = df_sorted['cliente_id'].rolling('7D').count()

# Expanding windows
df_sorted['salario_medio_acumulado'] = df_sorted['salario'].expanding().mean()
df_sorted['percentil_salario'] = df_sorted['salario'].expanding().quantile(0.5)

print("Window functions aplicadas:")
print(df_sorted[['salario', 'media_salario_30d', 'salario_medio_acumulado']].tail(10).round(2))

# Window functions por grupo
df_sorted = df_sorted.reset_index()
df_sorted['rank_salario_categoria'] = df_sorted.groupby('categoria')['salario'].rank(ascending=False)
df_sorted['pct_rank_score'] = df_sorted.groupby('categoria')['score_credito'].rank(pct=True)

print("\n3.2 Ranking por categoria:")
top_por_categoria = df_sorted[df_sorted['rank_salario_categoria'] <= 3][
    ['nome', 'categoria', 'salario', 'rank_salario_categoria']
].sort_values(['categoria', 'rank_salario_categoria'])
print(top_por_categoria.head(10))

# 4. ACCESSOR CUSTOMIZADOS
print("\n4. ACCESSOR CUSTOMIZADOS")
print("-" * 30)

# Criando accessor customizado para análise de clientes
@pd.api.extensions.register_dataframe_accessor("cliente_analysis")
class ClienteAnalysisAccessor:
    def __init__(self, pandas_obj):
        self._obj = pandas_obj
    
    def perfil_completo(self):
        """Gerar perfil completo dos clientes"""
        df = self._obj
        
        perfil = {
            'total_clientes': len(df),
            'idade_media': df['idade'].mean(),
            'salario_mediano': df['salario'].median(),
            'score_medio': df['score_credito'].mean(),
            'taxa_ativacao': df['ativo'].mean(),
            'categoria_dominante': df['categoria'].mode().iloc[0] if not df['categoria'].mode().empty else None
        }
        
        return pd.Series(perfil)
    
    def clientes_alto_valor(self, percentil=0.8):
        """Identificar clientes de alto valor"""
        threshold_salario = df['salario'].quantile(percentil)
        threshold_score = df['score_credito'].quantile(percentil)
        
        return self._obj[
            (self._obj['salario'] >= threshold_salario) & 
            (self._obj['score_credito'] >= threshold_score)
        ]
    
    def analise_por_segmento(self):
        """Análise detalhada por segmento"""
        return self._obj.groupby('categoria').agg({
            'idade': ['mean', 'std'],
            'salario': ['mean', 'median', 'std'],
            'score_credito': ['mean', 'min', 'max'],
            'ativo': 'mean'
        }).round(2)

# Usando o accessor customizado
print("4.1 Perfil completo usando accessor:")
perfil = df.cliente_analysis.perfil_completo()
print(perfil)

print("\n4.2 Clientes de alto valor:")
alto_valor = df.cliente_analysis.clientes_alto_valor(percentil=0.9)
print(f"Clientes top 10%: {len(alto_valor)}")

print("\n4.3 Análise por segmento:")
analise_segmento = df.cliente_analysis.analise_por_segmento()
print(analise_segmento)

# 5. VALIDAÇÃO DE DADOS AVANÇADA
print("\n5. VALIDAÇÃO DE DADOS AVANÇADA")
print("-" * 35)

class ValidadorDados:
    """Classe para validação avançada de dados"""
    
    def __init__(self, df):
        self.df = df
        self.erros = []
        self.warnings = []
    
    def validar_tipos(self, schema):
        """Validar tipos de dados conforme schema"""
        for coluna, tipo_esperado in schema.items():
            if coluna in self.df.columns:
                if not self.df[coluna].dtype == tipo_esperado:
                    self.erros.append(f"Coluna {coluna}: esperado {tipo_esperado}, encontrado {self.df[coluna].dtype}")
    
    def validar_ranges(self, ranges):
        """Validar ranges de valores"""
        for coluna, (min_val, max_val) in ranges.items():
            if coluna in self.df.columns:
                valores_fora = ((self.df[coluna] < min_val) | (self.df[coluna] > max_val)).sum()
                if valores_fora > 0:
                    self.warnings.append(f"Coluna {coluna}: {valores_fora} valores fora do range [{min_val}, {max_val}]")
    
    def validar_unicidade(self, colunas_unicas):
        """Validar unicidade de colunas"""
        for coluna in colunas_unicas:
            if coluna in self.df.columns:
                duplicatas = self.df[coluna].duplicated().sum()
                if duplicatas > 0:
                    self.erros.append(f"Coluna {coluna}: {duplicatas} valores duplicados encontrados")
    
    def validar_completude(self, colunas_obrigatorias):
        """Validar completude de dados"""
        for coluna in colunas_obrigatorias:
            if coluna in self.df.columns:
                nulos = self.df[coluna].isnull().sum()
                if nulos > 0:
                    self.erros.append(f"Coluna {coluna}: {nulos} valores nulos encontrados")
    
    def validar_consistencia(self):
        """Validações de consistência específicas"""
        # Exemplo: score de crédito vs categoria
        if 'score_credito' in self.df.columns and 'categoria' in self.df.columns:
            scores_por_categoria = self.df.groupby('categoria')['score_credito'].mean()
            
            # Verificar se ordem faz sentido
            if 'Bronze' in scores_por_categoria.index and 'Platina' in scores_por_categoria.index:
                if scores_por_categoria['Bronze'] >= scores_por_categoria['Platina']:
                    self.warnings.append("Inconsistência: Bronze tem score maior que Platina")
    
    def relatorio_validacao(self):
        """Gerar relatório completo de validação"""
        print("RELATÓRIO DE VALIDAÇÃO")
        print("=" * 25)
        
        if not self.erros and not self.warnings:
            print("✓ Todos os dados estão válidos!")
            return
        
        if self.erros:
            print("ERROS ENCONTRADOS:")
            for erro in self.erros:
                print(f"❌ {erro}")
        
        if self.warnings:
            print("\nWARNINGS:")
            for warning in self.warnings:
                print(f"⚠️  {warning}")
        
        print(f"\nResumo: {len(self.erros)} erros, {len(self.warnings)} warnings")

# Usando o validador
print("5.1 Validação completa dos dados:")
validador = ValidadorDados(df)

# Definindo regras de validação
schema = {
    'cliente_id': 'int64',
    'idade': 'int64',
    'salario': 'float64'
}

ranges = {
    'idade': (18, 100),
    'score_credito': (300, 850),
    'salario': (1000, 50000)
}

validador.validar_tipos(schema)
validador.validar_ranges(ranges)
validador.validar_unicidade(['cliente_id'])
validador.validar_completude(['cliente_id', 'nome'])
validador.validar_consistencia()

validador.relatorio_validacao()

# 6. DEBUGGING AVANÇADO
print("\n6. DEBUGGING AVANÇADO")
print("-" * 25)

def debug_dataframe(df, nome="DataFrame"):
    """Função para debug completo de DataFrame"""
    print(f"\nDEBUG: {nome}")
    print("=" * (len(nome) + 7))
    
    print(f"Shape: {df.shape}")
    print(f"Memória: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
    
    print("\nTipos de dados:")
    for col, dtype in df.dtypes.items():
        memoria_col = df[col].memory_usage(deep=True) / 1024 / 1024
        nulos = df[col].isnull().sum()
        print(f"  {col}: {dtype} ({memoria_col:.2f} MB, {nulos} nulos)")
    
    print("\nEstatísticas rápidas:")
    for col in df.select_dtypes(include=[np.number]).columns:
        print(f"  {col}: min={df[col].min():.2f}, max={df[col].max():.2f}, mean={df[col].mean():.2f}")
    
    print("\nValores únicos (categóricas):")
    for col in df.select_dtypes(include=['object', 'category']).columns:
        unique_count = df[col].nunique()
        print(f"  {col}: {unique_count} valores únicos")
        if unique_count <= 10:
            print(f"    Valores: {list(df[col].unique())}")

# Aplicando debug
debug_dataframe(df.head(100), "Dataset de Clientes (amostra)")

# 7. OPERAÇÕES CONDICIONAIS COMPLEXAS
print("\n7. OPERAÇÕES CONDICIONAIS COMPLEXAS")
print("-" * 40)

# np.select para múltiplas condições
def classificar_cliente_complexo(df):
    """Classificação complexa de clientes"""
    conditions = [
        # VIP: Alto salário, alto score, ativo, categoria premium
        (df['salario'] > df['salario'].quantile(0.8)) & 
        (df['score_credito'] > df['score_credito'].quantile(0.8)) & 
        (df['ativo'] == True) & 
        (df['categoria'].isin(['Ouro', 'Platina'])),
        
        # Potencial: Bom salário ou bom score, ativo
        ((df['salario'] > df['salario'].quantile(0.6)) | 
         (df['score_credito'] > df['score_credito'].quantile(0.6))) & 
        (df['ativo'] == True),
        
        # Risco: Baixo score, inativo ou baixo salário
        (df['score_credito'] < df['score_credito'].quantile(0.3)) | 
        (df['ativo'] == False) | 
        (df['salario'] < df['salario'].quantile(0.2)),
    ]
    
    choices = ['VIP', 'Potencial', 'Risco']
    
    return np.select(conditions, choices, default='Regular')

df['classificacao_complexa'] = classificar_cliente_complexo(df)

print("Classificação complexa de clientes:")
print(df['classificacao_complexa'].value_counts())

# Análise cruzada
print("\nAnálise cruzada: Classificação vs Categoria:")
crosstab_complexa = pd.crosstab(df['classificacao_complexa'], df['categoria'], normalize='columns') * 100
print(crosstab_complexa.round(1))

# 8. TÉCNICAS DE SAMPLING AVANÇADAS
print("\n8. SAMPLING AVANÇADAS")
print("-" * 25)

# Sampling estratificado
def sample_estratificado(df, coluna_estrato, n_samples=100):
    """Amostragem estratificada"""
    return df.groupby(coluna_estrato, group_keys=False).apply(
        lambda x: x.sample(min(len(x), n_samples // df[coluna_estrato].nunique()))
    )

# Sampling balanceado
def sample_balanceado(df, coluna_target, n_per_class=50):
    """Amostragem balanceada por classe"""
    return df.groupby(coluna_target, group_keys=False).apply(
        lambda x: x.sample(min(len(x), n_per_class))
    )

print("8.1 Amostragem estratificada por categoria:")
amostra_estratificada = sample_estratificado(df, 'categoria', 200)
print(f"Amostra: {len(amostra_estratificada)} registros")
print("Distribuição por categoria:")
print(amostra_estratificada['categoria'].value_counts())

print("\n8.2 Amostragem balanceada por classificação:")
amostra_balanceada = sample_balanceado(df, 'classificacao_complexa', 25)
print(f"Amostra: {len(amostra_balanceada)} registros")
print("Distribuição por classificação:")
print(amostra_balanceada['classificacao_complexa'].value_counts())

# 9. TRANSFORMAÇÕES FUNCIONAIS AVANÇADAS
print("\n9. TRANSFORMAÇÕES FUNCIONAIS")
print("-" * 35)

# Compose functions
def compose(*functions):
    """Compor múltiplas funções"""
    return lambda x: functools.reduce(lambda acc, f: f(acc), functions, x)

import functools

# Funções de transformação
def normalizar_salario(df):
    df = df.copy()
    df['salario_normalizado'] = (df['salario'] - df['salario'].mean()) / df['salario'].std()
    return df

def calcular_score_composto(df):
    df = df.copy()
    df['score_composto'] = (
        df['salario_normalizado'] * 0.4 + 
        (df['score_credito'] / 850) * 0.6
    )
    return df

def adicionar_decis(df):
    df = df.copy()
    df['decil_score'] = pd.qcut(df['score_composto'], 10, labels=False) + 1
    return df

# Aplicando composição
print("9.1 Transformação funcional composta:")
transformacao_composta = compose(
    normalizar_salario,
    calcular_score_composto,
    adicionar_decis
)

df_transformado = transformacao_composta(df.head(500))
print("Score composto por decil:")
print(df_transformado.groupby('decil_score')['score_composto'].agg(['mean', 'std']).round(3))

# 10. PIPELINE COMPLETO AVANÇADO
print("\n10. PIPELINE COMPLETO AVANÇADO")
print("-" * 35)

class PipelineAvancado:
    """Pipeline avançado com logging e validação"""
    
    def __init__(self, nome="Pipeline"):
        self.nome = nome
        self.steps = []
        self.logs = []
        self.data_history = []
    
    def add_step(self, func, nome=None, validacao=None):
        """Adicionar step com validação opcional"""
        step_nome = nome or func.__name__
        self.steps.append({
            'func': func,
            'nome': step_nome,
            'validacao': validacao
        })
        return self
    
    def execute(self, df):
        """Executar pipeline completo"""
        resultado = df.copy()
        
        print(f"Executando {self.nome}:")
        print("-" * (len(self.nome) + 11))
        
        for i, step in enumerate(self.steps, 1):
            try:
                print(f"Step {i}: {step['nome']}...")
                
                # Executar função
                resultado = step['func'](resultado)
                
                # Validação opcional
                if step['validacao']:
                    step['validacao'](resultado)
                
                # Log
                self.logs.append({
                    'step': step['nome'],
                    'shape': resultado.shape,
                    'memoria_mb': resultado.memory_usage(deep=True).sum() / 1024 / 1024,
                    'timestamp': datetime.now()
                })
                
                print(f"  ✓ Concluído: {resultado.shape}")
                
            except Exception as e:
                print(f"  ❌ Erro: {e}")
                raise
        
        return resultado
    
    def relatorio_execucao(self):
        """Relatório da execução"""
        if not self.logs:
            print("Pipeline não executado ainda")
            return
        
        print(f"\nRELATÓRIO: {self.nome}")
        print("=" * (len(self.nome) + 11))
        
        for log in self.logs:
            print(f"{log['step']}: {log['shape']} ({log['memoria_mb']:.2f} MB)")

# Criando pipeline avançado
pipeline = PipelineAvancado("Análise de Clientes")

pipeline.add_step(
    lambda df: df[df['ativo'] == True],
    "Filtrar clientes ativos"
).add_step(
    normalizar_salario,
    "Normalizar salário"
).add_step(
    calcular_score_composto,
    "Calcular score composto"
).add_step(
    adicionar_decis,
    "Adicionar decis"
)

# Executando pipeline
resultado_pipeline = pipeline.execute(df.head(500))
pipeline.relatorio_execucao()

print(f"\nResultado final: {resultado_pipeline.shape}")
print("Primeiras linhas:")
print(resultado_pipeline[['nome', 'salario_normalizado', 'score_composto', 'decil_score']].head())

print("\n" + "=" * 60)
print("FIM DA AULA 11")
print("Próxima aula: Integração com outras ferramentas")
print("=" * 60)
