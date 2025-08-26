# Pandas para Engenharia de Dados 🐼

Um curso completo e progressivo de pandas voltado especificamente para profissionais de engenharia de dados. Este curso foi desenvolvido para cobrir desde os conceitos fundamentais até técnicas avançadas, com foco prático em cenários reais de engenharia de dados.

## 📚 Estrutura do Curso

### **Aulas Básicas (1-4)**
- **Aula 01**: Introdução e DataFrames Básicos
- **Aula 02**: Leitura e Escrita de Dados  
- **Aula 03**: Exploração e Análise Descritiva
- **Aula 04**: Limpeza e Tratamento de Dados

### **Aulas Intermediárias (5-8)**
- **Aula 05**: Transformações e Manipulações
- **Aula 06**: Agrupamentos e Agregações
- **Aula 07**: Joins e Merge de Dados
- **Aula 08**: Séries Temporais

### **Aulas Avançadas (9-12)**
- **Aula 09**: Pivot Tables e Reshape
- **Aula 10**: Performance e Otimização
- **Aula 11**: Técnicas Avançadas
- **Aula 12**: Integração com Outras Ferramentas

## 🗂️ Estrutura de Arquivos

```
pandas-for-dataengineer/
│
├── README.md                    # Este arquivo
├── requirements.txt             # Dependências do projeto
│
├── aulas/                       # Aulas do curso
│   ├── 01_introducao_dataframes.py
│   ├── 02_leitura_escrita_dados.py
│   ├── 03_exploracao_analise.py
│   ├── 04_limpeza_tratamento.py
│   ├── 05_transformacoes_manipulacoes.py
│   ├── 06_agrupamentos_agregacoes.py
│   ├── 07_joins_merge.py
│   ├── 08_series_temporais.py
│   ├── 09_pivot_reshape.py
│   ├── 10_performance_otimizacao.py
│   ├── 11_tecnicas_avancadas.py
│   └── 12_integracao_ferramentas.py
│
├── data/                        # Dados auxiliares
│   ├── vendas.csv
│   ├── funcionarios.json
│   ├── logs_sistema.csv
│   └── metricas_performance.csv
│
└── output/                      # Arquivos gerados durante as aulas
    └── .gitkeep
```

## 🚀 Como Usar

### 1. **Instalação**
```bash
# Clone o repositório
git clone https://github.com/seu-usuario/pandas-for-dataengineer.git
cd pandas-for-dataengineer

# Instale as dependências
pip install -r requirements.txt
```

### 2. **Executando as Aulas**
```bash
# Execute qualquer aula diretamente
python aulas/01_introducao_dataframes.py

# Ou execute em sequência
python aulas/02_leitura_escrita_dados.py
python aulas/03_exploracao_analise.py
# ... e assim por diante
```

### 3. **Usando Jupyter Notebook (Opcional)**
```bash
# Inicie o Jupyter
jupyter notebook

# Abra e execute os arquivos .py como notebooks
```

## 📋 Conteúdo Detalhado

### **Aula 01: Introdução e DataFrames Básicos**
- O que é pandas e sua importância
- Estruturas básicas: Series e DataFrame
- Criação de DataFrames
- Operações básicas de visualização
- Acessando e selecionando dados

### **Aula 02: Leitura e Escrita de Dados**
- Lendo arquivos CSV, JSON, Excel
- Parâmetros importantes na leitura
- Salvando dados em diferentes formatos
- Lidando com problemas comuns
- Pipeline simples de processamento

### **Aula 03: Exploração e Análise Descritiva**
- Estatísticas descritivas
- Análise de frequências e distribuições
- Detecção de outliers
- Análise temporal básica
- Correlações entre variáveis
- Relatórios de qualidade de dados

### **Aula 04: Limpeza e Tratamento de Dados**
- Identificação e tratamento de valores nulos
- Remoção de duplicatas
- Padronização de formatos
- Validação de dados
- Pipeline de limpeza reutilizável

### **Aula 05: Transformações e Manipulações**
- Função apply() e suas variações
- Transformações de string e data
- Criação de colunas derivadas
- Binning e categorização
- Window functions básicas

### **Aula 06: Agrupamentos e Agregações**
- GroupBy avançado
- Funções de agregação customizadas
- Transform vs Apply
- Filtros em grupos
- Agrupamentos temporais

### **Aula 07: Joins e Merge de Dados**
- Tipos de joins (inner, left, right, outer)
- Merge com múltiplas chaves
- Concat para empilhamento
- Resolução de conflitos
- Validação de joins

### **Aula 08: Séries Temporais**
- Objetos datetime e índices temporais
- Resample para agregações temporais
- Rolling windows e estatísticas móveis
- Detecção de tendências e sazonalidade
- Preenchimento de dados faltantes

### **Aula 09: Pivot Tables e Reshape**
- Pivot tables avançadas
- Melt para transformar wide em long
- Stack e unstack
- Crosstabs para análise categórica
- Trabalhando com MultiIndex

### **Aula 10: Performance e Otimização**
- Otimização de tipos de dados
- Vectorização vs loops
- Chunking para grandes datasets
- Monitoramento de memória
- Configurações de performance

### **Aula 11: Técnicas Avançadas**
- Pipeline funcional com pipe()
- Dados categóricos avançados
- Accessor customizados
- Validação avançada de dados
- Debugging e profiling

### **Aula 12: Integração com Outras Ferramentas**
- Integração com SQL databases
- Trabalho com Apache Parquet
- Consumo de APIs REST
- Introdução ao Dask
- Exportação para múltiplos formatos

## 🎯 Público-Alvo

Este curso é ideal para:
- **Engenheiros de Dados** que querem dominar pandas
- **Analistas de Dados** buscando técnicas avançadas
- **Cientistas de Dados** que precisam de skills de engenharia
- **Desenvolvedores** trabalhando com processamento de dados
- **Profissionais de TI** migrando para área de dados

## 📊 Dados de Exemplo

O curso inclui datasets realistas que simulam cenários comuns:
- **vendas.csv**: Dados de vendas com múltiplas dimensões
- **funcionarios.json**: Informações de RH em formato JSON
- **logs_sistema.csv**: Logs de sistema para análise temporal
- **metricas_performance.csv**: Métricas de infraestrutura

## 🔧 Pré-requisitos

- **Python básico**: Conhecimento de sintaxe e estruturas básicas
- **Lógica de programação**: Loops, condicionais, funções
- **SQL básico** (opcional): Ajuda nas aulas de joins e integração
- **Conceitos de dados**: Entendimento básico de tabelas e relacionamentos

## 💡 Diferenciais do Curso

- ✅ **Foco em Engenharia de Dados**: Técnicas específicas para ETL e pipelines
- ✅ **Progressivo**: Do básico ao avançado de forma estruturada
- ✅ **Prático**: Exemplos reais e cenários do dia a dia
- ✅ **Performance**: Ênfase em otimização e boas práticas
- ✅ **Integração**: Como usar pandas com outras ferramentas
- ✅ **Código Executável**: Todos os exemplos funcionam imediatamente

## 🤝 Contribuições

Contribuições são bem-vindas! Por favor:
1. Faça um fork do projeto
2. Crie uma branch para sua feature
3. Commit suas mudanças
4. Abra um Pull Request

## 📝 Licença

Este projeto está sob a licença MIT. Veja o arquivo LICENSE para detalhes.

## 📞 Contato

- **GitHub**: [Seu perfil GitHub]
- **LinkedIn**: [Seu LinkedIn]
- **Email**: [Seu email]

---

⭐ **Se este curso foi útil para você, considere dar uma estrela no repositório!**
