# Plano de Implementação — Etapa 3: Camada Silver

## Premissas de Arquitetura

- **Silver = limpeza e tipagem por fonte.** Cada dataset Bronze validado gera um dataset Silver correspondente — sem joins entre fontes.
- **Joins pertencem à Gold.** A consolidação dos 3 datasets em uma visão analítica única é responsabilidade da camada Gold.
- **A limpeza é guiada pelo relatório de qualidade E pela análise de negócio.** O relatório fornece os fatos técnicos (% nulos, tipos, achados). A análise de negócio determina o *significado* de cada coluna e qual tratamento faz sentido. Ambos são necessários — um sem o outro leva a decisões erradas.
- **`quality_flag` orienta a limpeza.** A coluna é lida e usada para decisões por linha. Só é descartada ao final, junto com os demais metadados Bronze.
- **Nulo nem sempre é dado faltante.** Nulo pode significar "não aplicável" (ex: `ransom_demanded_usd` para incidentes que não são ransomware), "não reportado" ou "não existe". Cada caso exige tratamento diferente.

---

## Por que a análise de negócio não cabia no Bronze

Bronze reporta **fatos técnicos** com fidelidade máxima: "73.5% de `ransom_demanded_usd` é nulo" é um fato correto. Não é papel do Bronze interpretar se esse nulo é "não aplicável a ataques não-ransomware" ou "dado ausente" — isso é decisão de domínio. Bronze não toma decisões, apenas observa. Se o relatório filtrasse com lógica de negócio, estaria embutindo suposições sobre o domínio em uma camada que deve ser agnóstica à semântica.

A Silver é onde essas decisões acontecem — e sem entender o domínio, qualquer tratamento é tecnicamente arbitrário.

---

## Entradas

| Dataset | Arquivo | Linhas | Colunas |
|---------|---------|--------|---------|
| Incidentes | `data/bronze/incidents_master_validated.parquet` | 850 | 34 |
| Impacto Financeiro | `data/bronze/financial_impact (1)_validated.parquet` | 778 | 21 |
| Impacto de Mercado | `data/bronze/market_impact_validated.parquet` | 358 | 33 |

---

## Saídas

| Dataset | Arquivo |
|---------|---------|
| Incidentes Silver | `data/silver/incidents_master_silver.parquet` |
| Impacto Financeiro Silver | `data/silver/financial_impact_silver.parquet` |
| Impacto de Mercado Silver | `data/silver/market_impact_silver.parquet` |
| Decisões documentadas | `docs/silver_decisions.md` |
| Checklist anti-leakage | `docs/anti_leakage_checklist.md` |

---

## Células do Notebook (`notebooks/silver_pipeline.ipynb`)

### Célula 1 — Setup e caminhos
- Importações: `pandas`, `pathlib`, `datetime`
- `PROJECT_ROOT` via `Path.cwd()` com fallback para pasta pai se executado dentro de `notebooks/`
- Definir `BRONZE_PATH`, `SILVER_PATH`, `DOCS_PATH`
- Criar diretórios se não existirem

---

### Célula 2 — Leitura dos datasets validados
- Ler os 3 `*_validated.parquet`
- Exibir: shape, colunas com `quality_flag`, distribuição dos flags

---

### Célula 3 — Diagnóstico Bronze (resumo por dataset)
Para cada dataset:
- Total de linhas / colunas
- Distribuição de `quality_flag` (quantas linhas OK, quantas com cada rule_id)
- Top colunas com nulos

---

### Células 4, 5, 6 — Análise de negócio + decisões por dataset

Antes de qualquer transformação, é necessário entender o que cada coluna representa no domínio de cibersegurança. O relatório de qualidade fornece os fatos técnicos; a análise de negócio determina o significado. Ambos guiam as decisões.

A ordem de transformação dentro de cada célula é:
1. Deduplicação
2. Descarte de colunas (baseado na análise abaixo)
3. Imputação de nulos
4. Padronização de categorias (`trim + lower`, strings vazias → `None`)
5. Conversão de datas
6. Tratamento de datas fora do range
7. Validação de coerência temporal (apenas `incidents_master`)
8. Descarte dos metadados Bronze (`ingestion_timestamp`, `source_file`, `quality_flag`) — sempre por último

---

#### `incidents_master` — Análise de negócio por coluna

| Coluna | % Nulos | Significado de Negócio | Tipo de Nulo | Decisão | Justificativa |
|--------|---------|------------------------|--------------|---------|---------------|
| `incident_id` | 0% | Identificador único do incidente. Chave primária. | — | **Manter** | Necessário para rastreabilidade e join na Gold |
| `company_name` | 0% | Nome da empresa. Texto livre, não é categoria. | — | **Descartar** | Não é feature preditiva; alta cardinalidade (750 únicos); identificador |
| `company_revenue_usd` | 0% | Receita anual da empresa. Proxy de porte e recursos de segurança. | — | **Manter** | Feature relevante — empresas maiores têm mais recursos de defesa mas também são alvos maiores |
| `country_hq` | 0% | País sede da empresa. | — | **Manter** | Feature categórica relevante — regulação (GDPR, HIPAA), postura de segurança e exposição a grupos APT variam por país |
| `industry_primary` | 0% | Setor primário (códigos NAICS: "62"=saúde, "52"=finanças, "51"=TI etc.). | — | **Manter** | Feature importante — setores têm perfis de ataque distintos; saúde e finanças são alvos preferenciais de ransomware |
| `industry_secondary` | 82% | Setor secundário de atuação. | Não aplicável (nem toda empresa tem setor secundário) | **Descartar** | 82% de nulos estruturais; dado vetor primário presente, o secundário acrescentaria pouco com esparsidade tão alta |
| `employee_count` | 0% | Número de funcionários. | — | **Manter** | Feature de porte da empresa; superficie de ataque correlaciona com tamanho |
| `is_public_company` | 0% | Flag booleana: empresa de capital aberto. | — | **Manter** | Feature relevante — empresas públicas têm obrigações regulatórias de disclosure; diferente perfil de risco |
| `stock_ticker` | 52% | Código de bolsa da empresa. | Não aplicável (empresas privadas não têm ticker) | **Manter como `None`** | Nulo aqui é semântico: empresa privada. Não imputar. Chave de join com `market_impact` na Gold |
| `incident_date` | 0% | Data em que o incidente ocorreu (ou foi estimado). | — | **Manter + converter para datetime** | Chave temporal; base para features derivadas como `incident_year`, `days_to_discovery` |
| `incident_date_estimated` | 0% | Indica se `incident_date` é estimativa ou data exata. | — | **Manter** | Feature de qualidade da informação; incidentes com data estimada podem ter bias diferente |
| `discovery_date` | 0% | Data em que o incidente foi descoberto internamente. | — | **Manter + converter para datetime** | Feature temporal relevante; `days_to_discovery` é indicador de maturidade de detecção |
| `disclosure_date` | 0% | Data de divulgação pública. | — | **Manter + converter + usar apenas para `days_to_disclosure`** | Pode ser leakage se usada diretamente como feature (sabe-se só após o fato). Usar apenas para calcular `days_to_disclosure`, depois descartar a coluna raw |
| `attack_vector_primary` | 0% | Tipo principal de ataque (ransomware, phishing, data_breach, apt, malware etc.). | — | **Manter** | Feature mais importante do dataset — determina o perfil completo do incidente |
| `attack_vector_secondary` | 75% | Tipo secundário de ataque. | Não aplicável (nem todo ataque tem vetor secundário) | **Descartar + criar flag `has_secondary_vector`** | 75% de nulos estruturais. Em vez de manter a coluna esparsa, criar feature binária indicando se houve vetor secundário |
| `attack_chain` | 32% | Descrição detalhada da cadeia de ataque (texto livre longo). | Não documentado | **Descartar** | Texto narrativo longo, não estruturado. Não é feature para ML clássico. Candidato a NLP na Gold |
| `attributed_group` | 43% | Grupo criminoso atribuído ao ataque (REvil, Sandworm, Akira etc.). | Não atribuído / desconhecido | **Manter + imputar `"unknown"`** | Feature relevante quando presente; a ausência (unknown) é uma categoria legítima — indica ataque sem atribuição identificada |
| `attribution_confidence` | 43% | Confiança na atribuição: confirmed, probable, suspected, unknown. | Condicionado a `attributed_group` | **Manter + imputar `"unknown"` onde `attributed_group` for `"unknown"`** | Par condicional: se não há atribuição, confiança é naturalmente nula. Manter como indicador de qualidade da inteligência |
| `data_compromised_records` | 29% | Número de registros de dados comprometidos. | Não reportado OU ataque sem exfiltração de dados | **Manter + imputar mediana + criar flag `data_loss_unknown`** | Feature numérica importante para escala do breach. Nulo ambíguo: pode ser ausência de exfiltração ou dado não divulgado. Flag preserva a incerteza |
| `data_type` | 29% | Tipo de dado comprometido: PII, financial, credentials, health, mixed. | Condicionado a `data_compromised_records` | **Manter + imputar `"none"` onde `data_compromised_records == 0`, `"unknown"` nos demais** | Par condicional com `data_compromised_records`. Tipo de dado afeta severidade regulatória (GDPR prioriza PII, HIPAA prioriza health) |
| `systems_affected` | 0% | Sistemas afetados no incidente. | — | **Inspecionar antes de decidir** | 0% nulos mas 632 valores únicos em 850 linhas — pode ser texto livre ou identificador. Se for texto livre, descartar. Se for categoria, manter |
| `downtime_hours` | 51% | Horas de indisponibilidade operacional. | Não reportado OU sem downtime | **Manter + imputar mediana + criar flag `downtime_unknown`** | Feature numérica crítica para o label. Imputar 0 seria errado: 0 significa "sem downtime confirmado", mas nulo significa "não informado" — são coisas distintas. Mediana preserva a distribuição real |
| `data_source_primary` | 0% | URL/referência da fonte primária (850 valores únicos de 850 linhas). | — | **Descartar** | Identificador de fonte, não feature preditiva. Alta cardinalidade impossibilita encoding útil |
| `data_source_secondary` | 55% | Referência de fonte secundária (386 únicos de 386 não-nulos). | Não disponível | **Descartar** | Mesmo problema que `data_source_primary` — identificador de fonte, não atributo do incidente |
| `data_source_type` | 0% | Tipo da fonte: verified_media, sec_filing, cybersecurity_firm, gdpr_notification, company_pr. | — | **Manter** | Feature legítima de qualidade/origem da informação; 5 categorias bem definidas |
| `confidence_tier` | 0% | Nível de confiança do registro (1-4), atribuído internamente. | — | **Descartar (leakage)** | Metadado de curadoria interna atribuído após análise do incidente. Não observável no momento do evento |
| `quality_score` | 0% | Score de qualidade interno (50–100). | — | **Descartar (leakage severo)** | Calculado e atribuído internamente após avaliação. Fortemente correlacionado com características do incidente — usá-lo seria data leakage |
| `quality_grade` | 0% | Grade derivada do `quality_score` (Bronze/Silver/Gold). | — | **Descartar (leakage)** | Derivado direto de `quality_score` |
| `review_flag` | 92% | Anotação de revisão humana: low_quality_score, large_breach_verify. | Sem flag de revisão | **Descartar** | 92% nulos + anotação pós-ingestão. Leakage de processo interno |
| `notes` | 75% | Notas textuais livres. | Sem notas | **Descartar** | 75% nulos + texto livre não estruturado |
| `created_at` | 0% | Timestamp de criação do registro (todos iguais: 2026-02-12). | — | **Descartar** | Metadado de sistema; valor idêntico para todos os registros — zero variância, sem valor preditivo |
| `updated_at` | 0% | Timestamp de atualização (todos iguais: 2026-02-12). | — | **Descartar** | Idem |
| `ingestion_timestamp` | 0% | Metadado Bronze (adicionado pela Etapa 1). | — | **Descartar ao final** | Metadado de pipeline |
| `source_file` | 0% | Metadado Bronze (adicionado pela Etapa 1). | — | **Descartar ao final** | Metadado de pipeline |
| `quality_flag` | 0% | Metadado Bronze (adicionado pela Etapa 2). | — | **Usar durante a limpeza; descartar ao final** | Orienta decisões por linha; não é feature |

---

#### `financial_impact` — Análise de negócio por coluna

| Coluna | % Nulos | Significado de Negócio | Tipo de Nulo | Decisão | Justificativa |
|--------|---------|------------------------|--------------|---------|---------------|
| `incident_id` | 0% | Chave de join com `incidents_master`. | — | **Manter** | Rastreabilidade e join na Gold |
| `direct_loss_usd` | 0% | Perda financeira direta do incidente. | — | **Manter** | Feature principal de impacto financeiro |
| `direct_loss_method` | 0% | Método de cálculo da perda direta (estimativa, auditoria etc.). | — | **Descartar** | Metadado metodológico da fonte; não é atributo do incidente em si |
| `ransom_demanded_usd` | 74% | Valor de resgate exigido. | **Não aplicável** — só ransomware (26.8% dos incidentes) | **Manter com nulos como `None`** | Nulo é semanticamente correto para não-ransomware. Não imputar. A ausência é informação: "não foi ataque de ransom". Criar flag `is_ransomware` baseado na presença deste valor |
| `ransom_paid_usd` | 89% | Valor de resgate efetivamente pago. | **Não aplicável** — condicional a ransomware E pagamento | **Manter com nulos como `None`** | Mesma lógica. Nulo = sem pagamento ou não-ransomware. Alta esparsidade mas informação valiosa quando presente |
| `ransom_source` | 89% | Fonte da informação sobre o resgate. | Condicional a `ransom_paid_usd` | **Descartar** | Metadado de fonte, não atributo financeiro do incidente |
| `recovery_cost_usd` | 0% | Custo de recuperação e remediação. | — | **Manter** | Feature relevante de impacto operacional/financeiro |
| `legal_fees_usd` | 0% | Custos jurídicos (advogados, litígios, notificações). | — | **Manter** | Feature de impacto regulatório e jurídico |
| `regulatory_fine_usd` | 83% | Valor de multa regulatória (GDPR, HIPAA etc.). | **Não aplicável** — apenas incidentes que geraram multa | **Manter com nulos como `None` + criar flag `has_regulatory_fine`** | Nulo = sem multa (ou não reportado). Presença de multa é informação distinta de ausência. Flag binária preserva esse sinal |
| `insurance_payout_usd` | 44% | Pagamento recebido do seguro. | Não documentado / empresa sem seguro | **Manter + imputar mediana + criar flag `insurance_unknown`** | Nulo ambíguo: pode ser "empresa sem seguro" (deveria ser 0) ou "não reportado". Imputar 0 incorretamente assumiria ausência de seguro. Mediana é mais segura; flag preserva incerteza |
| `total_loss_usd` | 0% | Perda financeira total consolidada. | — | **Manter** | Feature financeira principal; soma de direto + multas + jurídico + recuperação |
| `total_loss_method` | 0% | Método de cálculo da perda total. | — | **Descartar** | Metadado metodológico |
| `total_loss_lower_bound` | 0% | Limite inferior do intervalo de confiança da perda total. | — | **Manter** | Indica incerteza na estimativa; útil para modelos que trabalham com intervalos |
| `total_loss_upper_bound` | 0% | Limite superior do intervalo de confiança. | — | **Manter** | Idem |
| `inflation_adjusted_usd` | 0% | Valor da perda ajustado pela inflação (CPI). | — | **Manter** | Permite comparação temporal correta entre incidentes de anos diferentes |
| `cpi_index_used` | 0% | Índice CPI utilizado para ajuste. | — | **Descartar** | Metadado metodológico do ajuste inflacionário |
| `notes` | 68% | Notas textuais. | Sem notas | **Descartar** | 68% nulos + texto livre |
| `created_at` | 0% | Timestamp de criação (todos iguais). | — | **Descartar** | Metadado de sistema; zero variância |
| `updated_at` | 0% | Timestamp de atualização (todos iguais). | — | **Descartar** | Idem |
| `ingestion_timestamp` | 0% | Metadado Bronze. | — | **Descartar ao final** | Metadado de pipeline |
| `source_file` | 0% | Metadado Bronze. | — | **Descartar ao final** | Metadado de pipeline |
| `quality_flag` | 0% | Metadado Bronze. | — | **Usar durante a limpeza; descartar ao final** | Orienta decisões por linha |

---

#### `market_impact` — Análise de negócio por coluna

| Coluna | % Nulos | Significado de Negócio | Tipo de Nulo | Decisão | Justificativa |
|--------|---------|------------------------|--------------|---------|---------------|
| `incident_id` | 0% | Chave de join. | — | **Manter** | Rastreabilidade e join na Gold |
| `stock_ticker` | 0% | Código de bolsa. | — | **Manter como identificador** | Não é feature preditiva, mas permite join com `incidents_master` na Gold |
| `price_7d_before` | 0% | Preço da ação 7 dias antes da divulgação. | — | **Manter** | Contexto de mercado pré-incidente; baseline legítimo |
| `price_disclosure_day` | 0% | Preço no dia da divulgação pública. | — | **Manter com ressalva** | Medido no dia do evento; pode ser feature de contexto mas requer atenção: em predição em tempo real seria leakage. Para análise retroativa, legítimo. Documentar |
| `price_1d_after` | 0% | Preço 1 dia após divulgação. | — | **Manter com ressalva** | Consequência do evento. Para análise retroativa é feature de reação de mercado. Documentar que não é observável antes do evento |
| `price_7d_after` | 0% | Preço 7 dias após. | — | **Manter com ressalva** | Idem |
| `price_30d_after` | 0% | Preço 30 dias após. | — | **Manter com ressalva** | Idem |
| `volume_avg_30d_baseline` | 0% | Volume médio de negociação nos 30 dias antes do incidente. | — | **Manter** | Baseline pré-incidente legítimo; indica liquidez normal da ação |
| `volume_disclosure_day` | 0% | Volume no dia de divulgação. | — | **Manter com ressalva** | Indicador de reação de mercado; medido no evento |
| `sector_index` | 0% | Índice setorial de referência (S&P 500 Health Care, Financials etc.). | — | **Manter** | Feature de contexto macro; 10 categorias bem definidas |
| `sector_return_same_period` | 0% | Retorno do índice setorial no mesmo período. | — | **Manter** | Controle de confounding: separa impacto do incidente do movimento geral do setor |
| `abnormal_return_1d` | 0% | Retorno anormal da ação em 1 dia (relativo ao setor). | — | **Manter** | Medida de impacto real do incidente no mercado, descontado o movimento do setor |
| `abnormal_return_7d` | 0% | Retorno anormal em 7 dias. | — | **Manter** | Idem para janela de 7 dias |
| `abnormal_return_30d` | 0% | Retorno anormal em 30 dias. | — | **Manter** | Idem para janela de 30 dias |
| `car_neg1_to_pos1` | 0% | CAR (Cumulative Abnormal Return) de -1 a +1 dias. | — | **Manter** | Métrica padrão de estudos de eventos em finanças; captura janela imediata |
| `car_0_to_7` | 0% | CAR de 0 a 7 dias. | — | **Manter** | Janela de curto prazo |
| `car_0_to_30` | 0% | CAR de 0 a 30 dias. | — | **Manter** | Janela de médio prazo |
| `car_0_to_90` | 0% | CAR de 0 a 90 dias. | — | **Manter com ressalva** | Janela longa — calculado 3 meses após o evento. Em predição, seria leakage severo. Documentar. Para análise retroativa, legítimo |
| `t_statistic_1d` | 0% | Estatística t do retorno de 1 dia. | — | **Manter** | Indica significância estatística do impacto; par com `p_value_1d` |
| `p_value_1d` | 0% | P-valor do retorno de 1 dia. | — | **Manter** | Permite filtrar impactos estatisticamente significativos |
| `t_statistic_30d` | 0% | Estatística t de 30 dias. | — | **Manter** | Idem para janela de 30 dias |
| `p_value_30d` | 0% | P-valor de 30 dias. | — | **Manter** | Idem |
| `earnings_announcement_within_7d` | 0% | Flag: houve anúncio de resultados nos 7 dias ao redor da divulgação. | — | **Manter** | Fator de confounding crítico em análise de evento — separa impacto do incidente do impacto de resultados financeiros |
| `market_cap_at_disclosure` | 0% | Capitalização de mercado no dia da divulgação. | — | **Manter** | Feature de porte da empresa no momento do incidente; complementa `company_revenue_usd` do `incidents_master` |
| `volume_ratio_disclosure` | 0% | Ratio entre volume no dia de divulgação e baseline. | — | **Manter** | Indicador de reação do mercado relativa ao normal da ação |
| `pre_incident_volatility_30d` | 0% | Volatilidade histórica 30 dias antes do incidente. | — | **Manter** | Contexto pré-incidente legítimo; ações mais voláteis reagem diferente |
| `post_incident_volatility_30d` | 0% | Volatilidade 30 dias após o incidente. | — | **Manter com ressalva** | Consequência do incidente. Para análise retroativa pode ser feature. Documentar |
| `days_to_price_recovery` | 10% | Dias até a ação recuperar o preço pré-incidente. | Recuperação não observada no período de análise | **Manter + imputar mediana** | 10% de nulos — podem representar ações que não se recuperaram no período de análise. Imputar mediana é conservador; documentar que nulos podem ser casos mais severos |
| `notes` | 74% | Notas textuais. | Sem notas | **Descartar** | 74% nulos + texto livre |
| `created_at` | 0% | Metadado de sistema (todos iguais). | — | **Descartar** | Zero variância |
| `updated_at` | 0% | Metadado de sistema (todos iguais). | — | **Descartar** | Idem |
| `ingestion_timestamp` | 0% | Metadado Bronze. | — | **Descartar ao final** | Metadado de pipeline |
| `source_file` | 0% | Metadado Bronze. | — | **Descartar ao final** | Metadado de pipeline |
| `quality_flag` | 0% | Metadado Bronze. | — | **Usar durante a limpeza; descartar ao final** | Orienta decisões por linha |

---

### Célula 7 — Criação de features derivadas

Aplicado ao `incidents_master`:
- `incident_year`, `incident_month`, `incident_day`
- `days_to_discovery = discovery_date - incident_date`
- `days_to_disclosure = disclosure_date - incident_date` — depois descartar `disclosure_date` raw
- `has_data_loss = 1 se data_compromised_records > 0`
- `has_downtime = 1 se downtime_hours > 0`
- `has_secondary_vector = 1 se attack_vector_secondary não era nulo no Bronze` (calcular antes do descarte)

---

### Célula 8 — Criar label para ML

**Label:** `label_severe_incident`

**Definição:** `1` se o incidente resultou em perda de dados (`has_data_loss == 1`) **ou** indisponibilidade operacional (`has_downtime == 1`).

**Justificativa:** representa severidade operacional tangível — dados comprometidos e/ou sistemas fora do ar são os impactos mais relevantes e mensuráveis para organizações. Não usa colunas financeiras (disponíveis apenas em outro dataset — dependeria de join prematuro). Não usa `disclosure_date` nem `quality_score` (leakage). Baseado apenas em atributos do incidente observáveis na fonte.

Exibir distribuição de classes (`value_counts`) após criação.

---

### Célula 9 — Inspeção de `systems_affected`

Verificar se `systems_affected` é texto livre ou categoria antes de decidir. Se tiver alta cardinalidade e conteúdo narrativo: descartar. Se tiver categorias repetíveis: manter.

---

### Célula 10 — Checklist anti-leakage final

Verificar que nenhuma das seguintes colunas está presente nos datasets Silver:

| Coluna | Dataset | Risco |
|--------|---------|-------|
| `quality_score` | incidents_master | Score interno calculado após o incidente |
| `quality_grade` | incidents_master | Derivado do quality_score |
| `confidence_tier` | incidents_master | Metadado de curadoria interna pós-ingestão |
| `review_flag` | incidents_master | Anotação humana posterior ao incidente |
| `disclosure_date` (raw) | incidents_master | Usada apenas para `days_to_disclosure`; coluna raw descartada |
| `data_source_primary` | incidents_master | Identificador de fonte, não atributo do incidente |
| `data_source_secondary` | incidents_master | Idem |
| `direct_loss_method` | financial_impact | Metadado metodológico |
| `total_loss_method` | financial_impact | Metadado metodológico |
| `cpi_index_used` | financial_impact | Metadado do ajuste inflacionário |
| `ransom_source` | financial_impact | Metadado de fonte; acompanharia ransom já mantido |
| `created_at` | todos | Metadado de sistema; zero variância |
| `updated_at` | todos | Idem |
| `ingestion_timestamp` | todos | Metadado Bronze |
| `source_file` | todos | Metadado Bronze |
| `quality_flag` | todos | Metadado Bronze |

---

### Célula 11 — Validação Silver

Para cada dataset Silver:
- Shape (linhas × colunas)
- % de nulos por coluna após tratamento
- Duplicatas remanescentes
- Schema final (tipos de cada coluna)
- Distribuição do label (apenas `incidents_master`)

---

### Célula 12 — Documentação de decisões

Gerar automaticamente:
- `docs/silver_decisions.md` — justificativas de cada transformação por dataset
- `docs/anti_leakage_checklist.md` — tabela com colunas removidas e motivo

---

### Célula 13 — Salvar Silver em Parquet

Salvar cada dataset limpo em `data/silver/`:
- `incidents_master_silver.parquet`
- `financial_impact_silver.parquet`
- `market_impact_silver.parquet`

Confirmar arquivos gerados com tamanho e contagem de linhas.

---

## Checklist de Conclusão

- [ ] 3 Parquets Silver gerados em `data/silver/`
- [ ] `docs/silver_decisions.md` gerado
- [ ] `docs/anti_leakage_checklist.md` gerado
- [ ] Nenhuma coluna de leakage nos datasets Silver
- [ ] Label `label_severe_incident` presente em `incidents_master_silver`
- [ ] Todas as colunas de data convertidas para `datetime`
- [ ] Nulos tratados conforme análise de negócio (não apenas pelo % técnico)
- [ ] `systems_affected` inspecionada antes de decisão final
- [ ] Colunas com nulos estruturais (ransom, regulatory_fine) mantidas com `None` e flags criadas

Verificar que nenhuma das seguintes colunas está presente nos datasets Silver:

| Coluna | Risco |
|--------|-------|
| `quality_score` | Score de avaliação calculado após o incidente |
| `quality_grade` | Derivado do quality_score |
| `review_flag` | Anotação interna posterior ao incidente |
| `disclosure_date` | Pode revelar o desfecho (sabe-se quando foi divulgado) |
| `updated_at` | Timestamp de atualização do registro; pós-evento |
| `created_at` | Metadado operacional sem valor preditivo |
| `ingestion_timestamp` | Metadado de pipeline |
| `source_file` | Metadado técnico |
| `quality_flag` | Resultado de validação |
| `data_source_primary/secondary` | Como souberam do incidente, não prediz severidade |
| `confidence_tier` | Qualidade da fonte, não atributo do incidente |

---

### Célula 10 — Validação Silver

Para cada dataset Silver:
- Shape (linhas × colunas)
- % de nulos por coluna dopo tratamento
- Duplicatas remanescentes
- Schema final (tipos de cada coluna)
- Distribuição do label (apenas `incidents_master`)

---

### Célula 11 — Documentação de decisões

Gerar automaticamente:
- `docs/silver_decisions.md`: justificativas de cada transformação por dataset
- `docs/anti_leakage_checklist.md`: tabela com colunas removidas e motivo

---

### Célula 12 — Salvar Silver em Parquet

Salvar cada dataset limpo em `data/silver/`:
- `incidents_master_silver.parquet`
- `financial_impact_silver.parquet`
- `market_impact_silver.parquet`

Confirmar arquivos gerados com tamanho e contagem de linhas.

---

## Checklist de Conclusão

- [ ] 3 Parquets Silver gerados em `data/silver/`
- [ ] `docs/silver_decisions.md` gerado
- [ ] `docs/anti_leakage_checklist.md` gerado
- [ ] Nenhuma coluna de leakage nos datasets Silver
- [ ] Label `label_severe_incident` presente em `incidents_master_silver`
- [ ] Todas as colunas de data convertidas para `datetime`
- [ ] Nulos tratados conforme o relatório de qualidade
