# cybersecurity-breach-data-project

Pipeline de engenharia de dados sobre um dataset de cibersegurança, organizado em camadas **Bronze** e **Silver**.

---

## Estrutura do Projeto

```
cybersecurity-breach-data-project/
├── data/
│   └── bronze/          # Parquets gerados pela ingestão + metadata.json
├── notebooks/
│   └── exploracao_silver.ipynb   # Transformações da camada Silver (PySpark)
├── reports/
│   ├── quality_report.md         # Relatório de qualidade gerado automaticamente
│   └── quality_report.json       # Idem, em JSON
├── src/
│   ├── ingestion.py     # Etapa 1 — Ingestão e camada Bronze
│   └── quality.py       # Etapa 2 — Validação de qualidade
└── requirements.txt
```

---

## Pré-requisitos

- Python 3.10+

---

## Instalação

```bash
# Clone o repositório
git clone <url-do-repositorio>
cd cybersecurity-breach-data-project

# Crie e ative o ambiente virtual
python -m venv venv

# Windows
venv\Scripts\activate

# Linux/macOS
source venv/bin/activate

# Instale as dependências
pip install -r requirements.txt
```

---

## Execução

### Etapa 1 — Ingestão (camada Bronze)

Baixa o dataset do Kaggle, padroniza as colunas para snake_case, adiciona colunas de lineagem por linha e salva em Parquet. Gera também `data/bronze/metadata.json` com hash MD5, contagem de linhas/colunas e tipos de cada arquivo.

```bash
python src/ingestion.py
```

**Saída esperada em `data/bronze/`:**
- `financial_impact (1).parquet`
- `incidents_master.parquet`
- `market_impact.parquet`
- `metadata.json`

### Etapa 2 — Validação de Qualidade

Aplica regras de qualidade sobre os Parquets Bronze, gera um relatório detalhado e salva versões validadas com coluna `quality_flag` por linha.

```bash
python src/quality.py
```

**Saída esperada:**
- `reports/quality_report.md` — relatório legível com achados, perfil completo das colunas e recomendações para a Silver
- `reports/quality_report.json` — idem em JSON
- `data/bronze/*_validated.parquet` — Parquets com coluna `quality_flag`

### Etapa 3 — Camada Silver (notebook PySpark)

```bash
jupyter notebook notebooks/exploracao_silver.ipynb
```

> Requer Java instalado para rodar o PySpark localmente.

---

## Datasets

O dataset `algozee/cyber-security` é baixado automaticamente do Kaggle via `kagglehub` na primeira execução da Etapa 1. O cache fica em `~/.cache/kagglehub/`.

| Arquivo | Linhas | Colunas | Descrição |
|---------|--------|---------|-----------|
| `financial_impact (1).csv` | 778 | 21 | Impacto financeiro dos incidentes |
| `incidents_master.csv` | 850 | 34 | Registro mestre de incidentes |
| `market_impact.csv` | 358 | 33 | Impacto no mercado de ações |
