# Relatório de Qualidade — Camadas Prata e Ouro

> **Etapa final (Pessoa 4) — governança.** Consolida o estado de qualidade da Silver e da Gold,
> com números **verificados diretamente nos artefatos** (`data/silver/*.parquet`,
> `data/gold/dataset_ml_ready.parquet`) em 2026-06-08, não copiados de etapas anteriores.

---

## 1. Camada Prata (recapitulação)

Cada dataset Silver é tratado de forma independente (joins pertencem à Gold). Detalhes de decisão
por coluna em [`docs/silver_decisions.md`](silver_decisions.md).

| Dataset | Shape | Nulos remanescentes | Colunas com nulos | Observação |
|---------|-------|---------------------|-------------------|------------|
| `incidents_master_silver.parquet` | 849 × 28 | 438 | 1 | `stock_ticker` mantido como `None` (empresas privadas sem ticker) |
| `financial_impact_silver.parquet` | 778 × 15 | 1910 | 3 | `ransom_demanded_usd`, `ransom_paid_usd`, `regulatory_fine_usd` — nulos **estruturais** (não-aplicável), mantidos como `None` |
| `market_impact_silver.parquet` | 358 × 28 | 0 | 0 | Sem nulos após limpeza |

**Interpretação:** os nulos remanescentes na Silver são **intencionais e estruturais** — representam
"não aplicável" (ex.: ransom só existe em ataques de ransomware), e não falhas de qualidade. A decisão
foi preservá-los como `None` na Silver e resolvê-los na Gold (imputação + flag), onde o contexto de ML
exige ausência de NaN. Ver checklist em [`docs/anti_leakage_checklist.md`](anti_leakage_checklist.md).

---

## 2. Camada Ouro

### 2.1 Dataset ML-ready

| Propriedade | Valor (verificado) |
|-------------|--------------------|
| Arquivo | `data/gold/dataset_ml_ready.parquet` |
| Shape | **849 × 103** (101 features + `label` + `split`) |
| Nulos | **0** (todos resolvidos no pipeline Gold) |
| Tipos | 101 `float64`, 1 `int64` (`label`), 1 `str` (`split`) |

**Distribuição do label** (alvo de *dwell time*: 1 = descoberta acima da mediana de `days_to_discovery`):

| Conjunto | Linhas | Classe 0 | Classe 1 |
|----------|--------|----------|----------|
| treino (`split == "train"`) | 679 | 51,1% | 48,9% |
| teste (`split == "test"`) | 170 | 51,2% | 48,8% |

O split estratificado (80/20, `random_state=42`) preserva a proporção do label entre treino e teste.

> ⚠️ **Inconsistência detectada na governança.** O documento auto-gerado
> [`docs/gold_transformations.md`](gold_transformations.md) ainda descreve, na seção "Split", o
> **label antigo** `label_severe_incident` (≈14% / 86%) e reporta **102 colunas**. O dataset
> efetivamente entregue usa o **novo alvo de *dwell time* balanceado (≈51% / 49%)** e tem **103
> colunas**. A mudança de target (commit `abc0889`) não atualizou aquele trecho do gerador. **Esta
> tabela reflete o estado real do parquet**; o `gold_transformations.md` deve ser corrigido na seção
> Split numa próxima iteração.

### 2.2 Transformações aplicadas (síntese)

Pipeline `sklearn` (`ColumnTransformer` + `Pipeline`), com `fit` **exclusivamente no treino** e
`transform` em treino e teste. Tabela completa em [`docs/gold_transformations.md`](gold_transformations.md).

| Grupo de colunas | Missing | Outliers | Scaling / Encoding |
|------------------|---------|----------|--------------------|
| Numéricas "normais" (`incident_year`, `days_to_*`, `employee_count`, métricas de mercado…) | `SimpleImputer(median, add_indicator)` | `IQRClipper(k=1.5)` | `StandardScaler` |
| Numéricas monetárias (`total_loss_usd`, `company_revenue_usd`, `*_usd`…) | `SimpleImputer(median, add_indicator)` | `log1p` (compressão de cauda) | `RobustScaler` |
| Categóricas baixa cardinalidade (`attack_vector_primary`, `sector_index`…) | `SimpleImputer(constant="unknown")` | — | `OneHotEncoder(min_frequency=10, handle_unknown="ignore")` |
| Categóricas alta cardinalidade (`industry_primary`, `country_hq`, `attributed_group`) | `SimpleImputer(constant="unknown")` | — | `TargetEncoder(target_type="binary")` |
| Binárias/flags (`has_data_loss`, `is_ransomware`…) | `SimpleImputer(constant=0)` | — | passthrough |

**Técnicas atendidas:** 2 encodings distintos (OneHot + Target), 2 scalings (Standard + Robust),
2 estratégias de missing (mediana+flag, constante), 2 tratamentos de outliers (clipping IQR + log1p).

Artefato do pré-processador serializado: `models/gold_preprocessor.joblib` (carregável via `joblib.load`).

### 2.3 Anti-leakage

Checklist completo em [`docs/anti_leakage_checklist.md`](anti_leakage_checklist.md). Resumo do que foi
**removido na Gold** para o modelo principal:

- **Identificadores:** `incident_id`, `stock_ticker` (e a duplicata `stock_ticker_mkt` do join).
- **Datas cruas:** `incident_date`, `discovery_date`, `incident_month`, `incident_day` (já derivadas em `days_to_*`).
- **Variáveis pós-evento (mercado):** `price_*_after`, `abnormal_return_*`, `car_*`,
  `post_incident_volatility_30d`, `days_to_price_recovery` — não observáveis antes do evento; seriam
  *leakage* em predição em tempo real. Preservadas separadamente em `data/gold/market_retroactive.parquet`
  para análise retroativa.
- Já removidas desde a Silver: `quality_score`, `quality_grade`, `confidence_tier`, `review_flag`,
  `disclosure_date` (raw), metadados de pipeline (`created_at`, `updated_at`, `ingestion_timestamp`, etc.).

---

## 3. Camada Ouro — Refatoração PySpark (escalabilidade)

Para demonstrar escalabilidade, o *join* estrutural e agregações foram reimplementados em PySpark
(`notebooks/pyspark_refactor.ipynb`), produzindo:

| Artefato | Conteúdo |
|----------|----------|
| `data/gold/spark_join.parquet` | Join LEFT dos 3 Silver por `incident_id` (849 × 68) |
| `data/gold/spark_agg_by_vector.parquet` | `groupBy(attack_vector_primary)` → contagem, média e mediana de `total_loss_usd` |
| `data/gold/spark_top5_per_year.parquet` | Window `row_number()` — top-5 incidentes por perda em cada ano |

**Nota de schema:** `stock_ticker` existe em `incidents` e `market`; a versão de `market` é descartada
antes do join para evitar coluna ambígua (mesma decisão da Gold pandas).

**Benchmark (volume atual, ~849 linhas):** Pandas ≈ 0,13s vs PySpark ≈ 0,94s — Pandas vence por ~7×
devido ao overhead de inicialização da JVM. O ganho do Spark aparece em escala (>10M linhas, clusters);
o objetivo aqui é demonstrar a portabilidade do código para uma API distribuída.

---

## 4. Conclusão

- **Silver:** nulos remanescentes são estruturais e documentados; nenhuma pendência de qualidade.
- **Gold:** dataset ML-ready com **0 nulos**, label balanceado (~51/49) e split estratificado íntegro.
- **Ressalva aberta:** corrigir a seção "Split" de `gold_transformations.md` (descreve o target antigo).
- **Escalabilidade:** join + agregações + window function validados em PySpark, com benchmark honesto.
