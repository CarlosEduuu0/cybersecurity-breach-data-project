# Plano de Implementação — Etapa 4: Análise Exploratória (EDA)

## Objetivo

Explorar visualmente e estatisticamente os datasets Silver para:
1. Entender distribuições, padrões e anomalias antes de construir modelos
2. Validar que as decisões da Silver fazem sentido (sanity check visual)
3. Identificar relações entre features e o label `label_severe_incident`
4. Gerar insights acionáveis com interpretações — não apenas gráficos

**Princípio:** cada gráfico deve ter uma interpretação textual explícita. Plot sem interpretação é ruído.

---

## Entradas

| Dataset | Arquivo | Shape |
|---------|---------|-------|
| Incidentes Silver | `data/silver/incidents_master_silver.parquet` | (846, 28) |
| Impacto Financeiro Silver | `data/silver/financial_impact_silver.parquet` | (778, 15) |
| Impacto de Mercado Silver | `data/silver/market_impact_silver.parquet` | (358, 28) |

### Schema resumido

**incidents_master** — 28 colunas:
- Categóricas (8): `country_hq`, `industry_primary`, `attack_vector_primary`, `attributed_group`, `attribution_confidence`, `data_type`, `data_source_type`, `stock_ticker`
- Numéricas (15): `company_revenue_usd`, `employee_count`, `data_compromised_records`, `downtime_hours`, `has_secondary_vector`, `data_loss_unknown`, `downtime_unknown`, `incident_year`, `incident_month`, `incident_day`, `days_to_discovery`, `days_to_disclosure`, `has_data_loss`, `has_downtime`, `label_severe_incident`
- Datetime (2): `incident_date`, `discovery_date`
- Bool (2): `is_public_company`, `incident_date_estimated`
- ID: `incident_id`

**financial_impact** — 15 colunas:
- Numéricas (14): `direct_loss_usd`, `ransom_demanded_usd`, `ransom_paid_usd`, `recovery_cost_usd`, `legal_fees_usd`, `regulatory_fine_usd`, `insurance_payout_usd`, `total_loss_usd`, `total_loss_lower_bound`, `total_loss_upper_bound`, `inflation_adjusted_usd`, `is_ransomware`, `has_regulatory_fine`, `insurance_unknown`
- ID: `incident_id`

**market_impact** — 28 colunas:
- Categórica (1): `sector_index`
- Numéricas (24): preços, volumes, retornos anormais, CARs, estatísticas t/p, volatilidade, `days_to_price_recovery`
- Bool (1): `earnings_announcement_within_7d`
- IDs: `incident_id`, `stock_ticker`

---

## Saídas

| Artefato | Arquivo |
|----------|---------|
| Notebook EDA | `notebooks/eda.ipynb` |

---

## Decisões de design

1. **Cada dataset é explorado individualmente primeiro** — coerente com a Silver (sem joins).
2. **Join exploratório no final** — uma seção opcional junta `incidents_master` + `financial_impact` via `incident_id` para análises cross-dataset (ex: severidade vs custo financeiro). Isso antecipa a Gold sem contaminar a Silver.
3. **Biblioteca de visualização:** `matplotlib` + `seaborn` para gráficos estáticos (mais compatíveis com relatórios e notebooks exportados).
4. **Paleta de cores consistente:** usar uma paleta fixa para todas as visualizações. Sem cores aleatórias.
5. **Formato de valores monetários:** abreviar com K/M/B nos eixos para legibilidade.

---

## Células do Notebook (`notebooks/eda.ipynb`)

### Célula 1 — Setup

- Importações: `pandas`, `numpy`, `matplotlib`, `seaborn`, `pathlib`
- Configurar estilo global: `sns.set_theme(style="whitegrid")`, `plt.rcParams` para tamanho de figura e fontes
- Definir paleta de cores para label (0=azul/cinza, 1=vermelho/laranja)
- Definir `SILVER_PATH`
- Função utilitária para formatar valores monetários em eixos (K/M/B)

### Célula 2 — Carregar datasets Silver

- Ler os 3 parquets Silver
- Exibir shape e `dtypes` resumidos
- Confirmar que `label_severe_incident` está presente em `incidents_master`

---

## Gráficos

### Célula 3 — **Gráfico 1: Distribuição de classes (severo vs não-severo)**

> Cobre o requisito: *distribuição de classes (ataque vs normal)*

**Tipo:** barplot horizontal com contagens + percentuais anotados. Subplot direito com pizza para proporção.

**Variáveis:** `label_severe_incident` (0 vs 1).

**O que mostrar:**
- Contagem e percentual de cada classe
- Linha de referência a 50% para destacar desbalanceamento
- Ratio de balanceamento no título

**Interpretação esperada:** label desbalanceado (~86% severo, ~14% não-severo). Um modelo naive que preveja sempre "severo" teria 86% de acurácia — acurácia não é métrica adequada. A Gold deverá usar F1, precision/recall e técnicas de balanceamento (SMOTE, `class_weight`).

---

### Célula 4 — **Gráfico 2: Taxa de severidade por vetor de ataque**

> Cobre o requisito: *valores por categoria*

**Tipo:** barplot horizontal ordenado por taxa de severidade.

**Variáveis:** `attack_vector_primary` × `label_severe_incident` — exibir % de severo por categoria.

**O que mostrar:**
- Top 10 vetores de ataque por frequência total
- Barra representa % de incidentes severos dentro de cada categoria
- Anotação de n (contagem total) em cada barra

**Interpretação esperada:** ransomware e APTs devem ter taxa de severidade mais alta que phishing genérico, validando que o label diferencia tipos de ataque. Vetores com alta taxa E alta frequência são os mais críticos para priorização.

---

### Célula 5 — **Gráfico 3: Correlação entre variáveis numéricas**

> Cobre o requisito: *correlação entre variáveis*

**Tipo:** heatmap de correlação (Pearson), máscara triangular superior.

**Variáveis:** numéricas de `incidents_master` — excluir `incident_day`, `incident_month`, flags de imputação (`data_loss_unknown`, `downtime_unknown`) e o próprio label para não enviesar.

**O que mostrar:**
- Matriz com valores anotados
- Diverging colormap (azul=negativo, vermelho=positivo)
- Destacar correlações |r| > 0.5

**Interpretação esperada:**
- `company_revenue_usd` × `employee_count` devem ter alta correlação (>0.7) — colinearidade a tratar na Gold
- `days_to_discovery` × `days_to_disclosure` podem ter correlação moderada
- `has_data_loss` e `has_downtime` correlacionam por construção com o label

---

### Célula 6 — **Gráfico 4: Evolução temporal dos incidentes por severidade**

**Tipo:** lineplot com área sombreada, segmentado por label.

**Variáveis:** `incident_year` × contagem, separado por `label_severe_incident`.

**O que mostrar:**
- Contagem anual de incidentes por classe
- Anotação em anos de pico

**Interpretação esperada:** tendência de crescimento acompanha digitalização. Verificar anos atípicos que coincidam com eventos conhecidos (WannaCry 2017, pandemia 2020). Se incidentes severos crescerem mais rápido que não-severos, ataques estão se tornando mais impactantes ao longo do tempo.

---

### Célula 7 — **Gráfico 5: Perda financeira por tipo de ataque (join exploratório)**

**Tipo:** boxplot horizontal ordenado pela mediana, escala log.

**Variáveis:** join `incidents_master` + `financial_impact` via `incident_id` → `attack_vector_primary` × `total_loss_usd`.

**O que mostrar:**
- Distribuição de `total_loss_usd` por vetor de ataque (top 8)
- Mediana anotada em cada box
- Escala log no eixo x

**Nota:** este join é exploratório — a Gold fará o join formal com validações. Exibir shape resultante antes do gráfico.

**Interpretação esperada:** ransomware deve ter mediana de perda financeira mais alta. Ataques de menor visibilidade (ex: insider threat) podem ter distribuição mais longa. Outliers bilionários identificam mega-breaches que merecem análise separada.

---

### Célula 8 — Resumo de insights e recomendações para a Gold

Célula markdown com:
- Resumo dos achados principais (um parágrafo por gráfico)
- Implicações para a modelagem na Gold (métricas a usar, features mais promissoras, colinearidade identificada)
- Limitações (desbalanceamento do label, nulos ambíguos, perspectiva temporal do `market_impact`)

---

## Requisitos de dependências

Bibliotecas necessárias (já em `requirements.txt` ou a adicionar):
- `matplotlib`
- `seaborn`

---

## Checklist de conclusão

- [ ] Notebook `notebooks/eda.ipynb` criado e executado sem erros
- [ ] **Gráfico 1:** distribuição de classes — `label_severe_incident` ✅ (requisito obrigatório)
- [ ] **Gráfico 2:** valores por categoria — `attack_vector_primary` × taxa de severidade ✅ (requisito obrigatório)
- [ ] **Gráfico 3:** correlação entre variáveis numéricas — heatmap ✅ (requisito obrigatório)
- [ ] **Gráfico 4:** evolução temporal por severidade
- [ ] **Gráfico 5:** perda financeira por tipo de ataque (join exploratório)
- [ ] Cada gráfico acompanhado de markdown com interpretação
- [ ] Insights e recomendações para Gold documentados na célula final
