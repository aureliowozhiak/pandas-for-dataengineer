"""
AULA 08: Séries Temporais com Pandas
====================================

Nesta aula, vamos dominar o trabalho com dados temporais em pandas.
Séries temporais são fundamentais em engenharia de dados e análises.

Objetivos:
- Trabalhar com objetos datetime
- Criar e manipular índices temporais
- Usar resample() para agregações temporais
- Aplicar rolling windows e estatísticas móveis
- Detectar tendências e sazonalidade
- Lidar com dados faltantes em séries temporais
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

print("=" * 60)
print("AULA 08: SÉRIES TEMPORAIS")
print("=" * 60)

# 1. CRIANDO E MANIPULANDO DATAS
print("\n1. CRIANDO E MANIPULANDO DATAS")
print("-" * 35)

# Diferentes formas de criar datas
print("1.1 Criando objetos datetime:")
data_string = '2024-01-15'
data_datetime = pd.to_datetime(data_string)
print(f"String: {data_string} → Datetime: {data_datetime}")

# Lista de datas
datas_lista = ['2024-01-01', '2024-01-02', '2024-01-03']
datas_series = pd.to_datetime(datas_lista)
print(f"Lista convertida: {datas_series}")

# date_range - sequências de datas
print("\n1.2 Usando date_range:")
datas_diarias = pd.date_range(start='2024-01-01', end='2024-01-31', freq='D')
print(f"Janeiro 2024 (diário): {len(datas_diarias)} datas")

datas_horarias = pd.date_range(start='2024-01-01', periods=24, freq='H')
print(f"24 horas: {datas_horarias[:3]} ... {datas_horarias[-1]}")

# Diferentes frequências
frequencias = {
    'D': 'Diário',
    'W': 'Semanal',
    'M': 'Mensal',
    'Q': 'Trimestral',
    'Y': 'Anual',
    'H': 'Horário',
    '15min': '15 minutos',
    'B': 'Dias úteis'
}

print("\n1.3 Diferentes frequências:")
for freq, desc in frequencias.items():
    datas = pd.date_range('2024-01-01', periods=5, freq=freq)
    print(f"{desc} ({freq}): {datas[0]} até {datas[-1]}")

# 2. CRIANDO SÉRIE TEMPORAL DE EXEMPLO
print("\n2. CRIANDO SÉRIE TEMPORAL DE DADOS")
print("-" * 40)

# Dados de vendas horárias por 30 dias
np.random.seed(42)
datas = pd.date_range(start='2024-01-01', periods=30*24, freq='H')

# Simulando padrões realistas de vendas
vendas_base = 100
vendas = []

for i, data in enumerate(datas):
    # Padrão semanal (mais vendas em dias úteis)
    fator_semana = 1.3 if data.weekday() < 5 else 0.7
    
    # Padrão diário (picos às 12h e 18h)
    hora = data.hour
    if 9 <= hora <= 11 or 14 <= hora <= 16:
        fator_hora = 1.5
    elif 12 <= hora <= 13 or 17 <= hora <= 19:
        fator_hora = 2.0
    elif 0 <= hora <= 6:
        fator_hora = 0.3
    else:
        fator_hora = 1.0
    
    # Tendência de crescimento
    fator_tendencia = 1 + (i / len(datas)) * 0.2
    
    # Ruído aleatório
    ruido = np.random.normal(1, 0.2)
    
    venda = vendas_base * fator_semana * fator_hora * fator_tendencia * ruido
    vendas.append(max(0, venda))  # Não permitir vendas negativas

# Criando DataFrame
df_vendas = pd.DataFrame({
    'data_hora': datas,
    'vendas': vendas
})

# Definindo índice temporal
df_vendas.set_index('data_hora', inplace=True)

print(f"Série temporal criada: {len(df_vendas)} registros")
print(f"Período: {df_vendas.index.min()} até {df_vendas.index.max()}")
print("\nPrimeiros registros:")
print(df_vendas.head())

# 3. EXPLORANDO COMPONENTES TEMPORAIS
print("\n3. COMPONENTES TEMPORAIS")
print("-" * 30)

# Extraindo componentes
df_vendas['ano'] = df_vendas.index.year
df_vendas['mes'] = df_vendas.index.month
df_vendas['dia'] = df_vendas.index.day
df_vendas['hora'] = df_vendas.index.hour
df_vendas['dia_semana'] = df_vendas.index.day_name()
df_vendas['dia_semana_num'] = df_vendas.index.dayofweek
df_vendas['fim_semana'] = df_vendas['dia_semana_num'].isin([5, 6])

print("Componentes temporais extraídos:")
print(df_vendas[['vendas', 'dia_semana', 'hora', 'fim_semana']].head(10))

# Análise por componentes
print("\n3.1 Vendas médias por dia da semana:")
vendas_por_dia = df_vendas.groupby('dia_semana')['vendas'].mean().round(2)
print(vendas_por_dia)

print("\n3.2 Vendas médias por hora:")
vendas_por_hora = df_vendas.groupby('hora')['vendas'].mean().round(2)
print("Horas com maiores vendas:")
print(vendas_por_hora.nlargest(5))

# 4. RESAMPLE - AGREGAÇÕES TEMPORAIS
print("\n4. RESAMPLE - AGREGAÇÕES TEMPORAIS")
print("-" * 40)

# Agregações por diferentes períodos
print("4.1 Vendas diárias:")
vendas_diarias = df_vendas['vendas'].resample('D').sum()
print(vendas_diarias.head())

print("\n4.2 Vendas semanais:")
vendas_semanais = df_vendas['vendas'].resample('W').agg(['sum', 'mean', 'count']).round(2)
print(vendas_semanais.head())

print("\n4.3 Vendas mensais com múltiplas métricas:")
vendas_mensais = df_vendas.resample('M').agg({
    'vendas': ['sum', 'mean', 'std', 'min', 'max']
}).round(2)
vendas_mensais.columns = ['total', 'media', 'desvio', 'minimo', 'maximo']
print(vendas_mensais)

# 5. ROLLING WINDOWS - ESTATÍSTICAS MÓVEIS
print("\n5. ROLLING WINDOWS - ESTATÍSTICAS MÓVEIS")
print("-" * 45)

# Médias móveis
df_vendas['ma_24h'] = df_vendas['vendas'].rolling(window=24).mean()  # Média móvel 24h
df_vendas['ma_7d'] = df_vendas['vendas'].rolling(window=24*7).mean()  # Média móvel 7 dias

# Rolling statistics
df_vendas['rolling_std'] = df_vendas['vendas'].rolling(window=24).std()
df_vendas['rolling_min'] = df_vendas['vendas'].rolling(window=24).min()
df_vendas['rolling_max'] = df_vendas['vendas'].rolling(window=24).max()

print("Estatísticas móveis (últimos registros):")
colunas_rolling = ['vendas', 'ma_24h', 'ma_7d', 'rolling_std']
print(df_vendas[colunas_rolling].tail(10).round(2))

# Bandas de Bollinger (exemplo de indicador técnico)
df_vendas['bb_upper'] = df_vendas['ma_24h'] + (2 * df_vendas['rolling_std'])
df_vendas['bb_lower'] = df_vendas['ma_24h'] - (2 * df_vendas['rolling_std'])

print("\n5.1 Valores fora das bandas de Bollinger:")
outliers_bb = df_vendas[(df_vendas['vendas'] > df_vendas['bb_upper']) | 
                       (df_vendas['vendas'] < df_vendas['bb_lower'])]
print(f"Outliers detectados: {len(outliers_bb)} de {len(df_vendas)} ({len(outliers_bb)/len(df_vendas)*100:.1f}%)")

# 6. EXPANDING WINDOWS - ESTATÍSTICAS ACUMULADAS
print("\n6. EXPANDING WINDOWS - ESTATÍSTICAS ACUMULADAS")
print("-" * 50)

# Estatísticas acumuladas desde o início
df_vendas['vendas_acumuladas'] = df_vendas['vendas'].expanding().sum()
df_vendas['media_acumulada'] = df_vendas['vendas'].expanding().mean()
df_vendas['std_acumulada'] = df_vendas['vendas'].expanding().std()

print("Estatísticas acumuladas (amostra):")
print(df_vendas[['vendas', 'vendas_acumuladas', 'media_acumulada']].iloc[::100].round(2))

# 7. SHIFT E LAG - VALORES ANTERIORES/POSTERIORES
print("\n7. SHIFT E LAG")
print("-" * 20)

# Valores anteriores (lag)
df_vendas['vendas_lag1'] = df_vendas['vendas'].shift(1)  # 1 período anterior
df_vendas['vendas_lag24'] = df_vendas['vendas'].shift(24)  # 24h anterior (mesmo horário dia anterior)

# Valores posteriores (lead)
df_vendas['vendas_lead1'] = df_vendas['vendas'].shift(-1)  # 1 período posterior

# Diferenças (variações)
df_vendas['diff_1h'] = df_vendas['vendas'] - df_vendas['vendas_lag1']
df_vendas['diff_24h'] = df_vendas['vendas'] - df_vendas['vendas_lag24']
df_vendas['pct_change_1h'] = df_vendas['vendas'].pct_change()

print("Análise de mudanças temporais:")
colunas_shift = ['vendas', 'vendas_lag1', 'diff_1h', 'pct_change_1h']
print(df_vendas[colunas_shift].dropna().head(10).round(2))

# 8. DETECÇÃO DE TENDÊNCIAS
print("\n8. DETECÇÃO DE TENDÊNCIAS")
print("-" * 30)

# Tendência usando regressão linear simples
from scipy import stats

def calcular_tendencia(series, janela=24*7):  # Janela de 1 semana
    """Calcular tendência usando regressão linear"""
    if len(series) < janela:
        return np.nan
    
    x = np.arange(len(series))
    slope, intercept, r_value, p_value, std_err = stats.linregress(x, series)
    return slope

# Tendência móvel
df_vendas['tendencia'] = df_vendas['vendas'].rolling(window=24*7).apply(
    calcular_tendencia, raw=False
)

print("Análise de tendência:")
print(f"Tendência média: {df_vendas['tendencia'].mean():.4f}")
print(f"Tendência atual: {df_vendas['tendencia'].iloc[-1]:.4f}")

# Períodos com tendência positiva/negativa
tendencia_positiva = (df_vendas['tendencia'] > 0).sum()
tendencia_negativa = (df_vendas['tendencia'] < 0).sum()
print(f"Períodos com tendência positiva: {tendencia_positiva}")
print(f"Períodos com tendência negativa: {tendencia_negativa}")

# 9. SAZONALIDADE
print("\n9. ANÁLISE DE SAZONALIDADE")
print("-" * 30)

# Decomposição básica por componentes
vendas_diarias_completas = df_vendas['vendas'].resample('D').sum()

# Sazonalidade semanal
vendas_por_dia_semana = df_vendas.groupby('dia_semana_num')['vendas'].mean()
print("Padrão semanal (0=Segunda, 6=Domingo):")
for dia, venda in vendas_por_dia_semana.items():
    nome_dia = ['Seg', 'Ter', 'Qua', 'Qui', 'Sex', 'Sab', 'Dom'][dia]
    print(f"{nome_dia}: {venda:.2f}")

# Sazonalidade horária
vendas_por_hora_detalhada = df_vendas.groupby('hora')['vendas'].mean()
print("\nPicos horários:")
picos_horarios = vendas_por_hora_detalhada.nlargest(3)
for hora, venda in picos_horarios.items():
    print(f"{hora:02d}h: {venda:.2f}")

# 10. PREENCHIMENTO DE DADOS FALTANTES
print("\n10. TRATAMENTO DE DADOS FALTANTES")
print("-" * 40)

# Introduzindo alguns valores faltantes para demonstração
df_com_falhas = df_vendas.copy()
indices_falhas = np.random.choice(df_com_falhas.index, size=50, replace=False)
df_com_falhas.loc[indices_falhas, 'vendas'] = np.nan

print(f"Dados faltantes introduzidos: {df_com_falhas['vendas'].isna().sum()}")

# Diferentes estratégias de preenchimento
print("\n10.1 Estratégias de preenchimento:")

# Forward fill
df_com_falhas['vendas_ffill'] = df_com_falhas['vendas'].fillna(method='ffill')

# Backward fill
df_com_falhas['vendas_bfill'] = df_com_falhas['vendas'].fillna(method='bfill')

# Interpolação linear
df_com_falhas['vendas_interp'] = df_com_falhas['vendas'].interpolate()

# Preenchimento com média móvel
df_com_falhas['vendas_ma_fill'] = df_com_falhas['vendas'].fillna(
    df_com_falhas['vendas'].rolling(window=24, center=True).mean()
)

# Comparando métodos
print("Comparação de métodos (primeiros valores faltantes):")
cols_comparacao = ['vendas', 'vendas_ffill', 'vendas_interp', 'vendas_ma_fill']
falhas_exemplo = df_com_falhas[df_com_falhas['vendas'].isna()].head(5)
print(falhas_exemplo[cols_comparacao].round(2))

# 11. DETECÇÃO DE ANOMALIAS TEMPORAIS
print("\n11. DETECÇÃO DE ANOMALIAS")
print("-" * 30)

def detectar_anomalias_zscore(series, threshold=3):
    """Detectar anomalias usando Z-score"""
    z_scores = np.abs(stats.zscore(series.dropna()))
    return z_scores > threshold

def detectar_anomalias_iqr(series):
    """Detectar anomalias usando IQR"""
    Q1 = series.quantile(0.25)
    Q3 = series.quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    return (series < lower_bound) | (series > upper_bound)

# Aplicando detecção
vendas_limpas = df_vendas['vendas'].dropna()
anomalias_zscore = detectar_anomalias_zscore(vendas_limpas)
anomalias_iqr = detectar_anomalias_iqr(vendas_limpas)

print(f"Anomalias por Z-score: {anomalias_zscore.sum()}")
print(f"Anomalias por IQR: {anomalias_iqr.sum()}")

# Anomalias temporais (mudanças bruscas)
df_vendas['diff_abs'] = df_vendas['vendas'].diff().abs()
mudancas_bruscas = df_vendas['diff_abs'] > df_vendas['diff_abs'].quantile(0.95)
print(f"Mudanças bruscas detectadas: {mudancas_bruscas.sum()}")

# 12. ANÁLISE DE CORRELAÇÃO TEMPORAL
print("\n12. CORRELAÇÃO TEMPORAL")
print("-" * 30)

# Autocorrelação (correlação com valores anteriores)
def calcular_autocorrelacao(series, lags=24):
    """Calcular autocorrelação para diferentes lags"""
    autocorr = []
    for lag in range(1, lags+1):
        corr = series.corr(series.shift(lag))
        autocorr.append(corr)
    return autocorr

autocorr_vendas = calcular_autocorrelacao(df_vendas['vendas'])
print("Autocorrelação mais forte (top 5):")
for i, corr in enumerate(sorted(autocorr_vendas, reverse=True)[:5]):
    lag = autocorr_vendas.index(corr) + 1
    print(f"Lag {lag}h: {corr:.3f}")

# 13. PREVISÃO SIMPLES
print("\n13. PREVISÃO SIMPLES")
print("-" * 25)

def previsao_media_movel(series, janela=24, periodos=24):
    """Previsão simples usando média móvel"""
    ultima_media = series.tail(janela).mean()
    previsoes = [ultima_media] * periodos
    return previsoes

def previsao_tendencia_linear(series, janela=24*7, periodos=24):
    """Previsão usando tendência linear"""
    dados_recentes = series.tail(janela).dropna()
    if len(dados_recentes) < 2:
        return [series.iloc[-1]] * periodos
    
    x = np.arange(len(dados_recentes))
    slope, intercept, _, _, _ = stats.linregress(x, dados_recentes)
    
    previsoes = []
    for i in range(1, periodos+1):
        prev = intercept + slope * (len(dados_recentes) + i)
        previsoes.append(prev)
    
    return previsoes

# Fazendo previsões
previsoes_ma = previsao_media_movel(df_vendas['vendas'])
previsoes_trend = previsao_tendencia_linear(df_vendas['vendas'])

print("Previsões para próximas 24 horas:")
print(f"Média móvel: {np.mean(previsoes_ma):.2f}")
print(f"Tendência linear: {np.mean(previsoes_trend):.2f}")
print(f"Valor atual: {df_vendas['vendas'].iloc[-1]:.2f}")

# 14. PIPELINE DE ANÁLISE TEMPORAL
print("\n14. PIPELINE DE ANÁLISE TEMPORAL")
print("-" * 40)

class AnalisadorSeriesTemporal:
    """Pipeline completo para análise de séries temporais"""
    
    def __init__(self, data, coluna_valor, coluna_data=None):
        if coluna_data:
            self.df = data.set_index(coluna_data)
        else:
            self.df = data.copy()
        self.coluna_valor = coluna_valor
        self.resultados = {}
    
    def estatisticas_basicas(self):
        """Calcular estatísticas básicas da série"""
        serie = self.df[self.coluna_valor]
        self.resultados['estatisticas'] = {
            'count': len(serie),
            'mean': serie.mean(),
            'std': serie.std(),
            'min': serie.min(),
            'max': serie.max(),
            'missing': serie.isna().sum()
        }
        return self.resultados['estatisticas']
    
    def detectar_sazonalidade(self):
        """Detectar padrões sazonais básicos"""
        self.df['hora'] = self.df.index.hour
        self.df['dia_semana'] = self.df.index.dayofweek
        
        sazonalidade_horaria = self.df.groupby('hora')[self.coluna_valor].mean()
        sazonalidade_semanal = self.df.groupby('dia_semana')[self.coluna_valor].mean()
        
        self.resultados['sazonalidade'] = {
            'horaria_cv': sazonalidade_horaria.std() / sazonalidade_horaria.mean(),
            'semanal_cv': sazonalidade_semanal.std() / sazonalidade_semanal.mean(),
            'pico_horario': sazonalidade_horaria.idxmax(),
            'pico_semanal': sazonalidade_semanal.idxmax()
        }
        return self.resultados['sazonalidade']
    
    def calcular_tendencia(self, janela=24*7):
        """Calcular tendência geral"""
        serie = self.df[self.coluna_valor].dropna()
        if len(serie) < janela:
            return None
        
        x = np.arange(len(serie))
        slope, _, r_value, p_value, _ = stats.linregress(x, serie)
        
        self.resultados['tendencia'] = {
            'slope': slope,
            'r_squared': r_value**2,
            'p_value': p_value,
            'significativa': p_value < 0.05
        }
        return self.resultados['tendencia']
    
    def relatorio_completo(self):
        """Gerar relatório completo"""
        print("RELATÓRIO DE ANÁLISE TEMPORAL")
        print("=" * 35)
        
        # Estatísticas básicas
        stats = self.estatisticas_basicas()
        print(f"Período: {self.df.index.min()} até {self.df.index.max()}")
        print(f"Registros: {stats['count']:,}")
        print(f"Média: {stats['mean']:.2f}")
        print(f"Desvio padrão: {stats['std']:.2f}")
        print(f"Dados faltantes: {stats['missing']}")
        
        # Sazonalidade
        sazon = self.detectar_sazonalidade()
        print(f"\nSazonalidade horária (CV): {sazon['horaria_cv']:.3f}")
        print(f"Pico horário: {sazon['pico_horario']}h")
        print(f"Sazonalidade semanal (CV): {sazon['semanal_cv']:.3f}")
        
        # Tendência
        tend = self.calcular_tendencia()
        if tend:
            print(f"\nTendência: {tend['slope']:.6f}")
            print(f"R²: {tend['r_squared']:.3f}")
            print(f"Significativa: {tend['significativa']}")

# Usando o pipeline
analisador = AnalisadorSeriesTemporal(df_vendas.reset_index(), 'vendas', 'data_hora')
analisador.relatorio_completo()

print("\n" + "=" * 60)
print("FIM DA AULA 08")
print("Próxima aula: Pivot tables e reshape")
print("=" * 60)
