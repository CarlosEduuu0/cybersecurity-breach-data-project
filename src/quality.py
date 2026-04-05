"""
Validação de Qualidade dos dados da camada Bronze
"""
import re
import json
import pandas as pd
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Caminhos
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BRONZE_PATH   = PROJECT_ROOT / "data" / "bronze"
REPORTS_PATH  = PROJECT_ROOT / "reports"
REPORTS_PATH.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Critérios de erro (regras de validação)
#
# Cada regra define:
#   rule_id    - identificador único
#   name       - descrição legível
#   scope      - "all" ou nome específico do arquivo parquet
#   columns    - lista de colunas envolvidas ([] = aplica ao dataset todo)
#   check      - tipo de verificação
#   threshold  - valor numérico de corte (interpretação depende do check)
#   severity   - "info" | "alerta" | "critico"
#   condition  - string descritiva da condição de falha
# ---------------------------------------------------------------------------

RULES = [
    # --- Nulos ---
    {
        "rule_id":   "NULL_CRITICAL",
        "name":      "Coluna com nulos críticos",
        "scope":     "all",
        "columns":   [],
        "check":     "null_pct",
        "threshold": 50.0,
        "severity":  "critico",
        "condition": "% de nulos > 50%",
    },
    {
        "rule_id":   "NULL_ALERT",
        "name":      "Coluna com nulos elevados",
        "scope":     "all",
        "columns":   [],
        "check":     "null_pct",
        "threshold": 5.0,
        "severity":  "alerta",
        "condition": "% de nulos entre 5% e 50%",
    },

    # --- Duplicados ---
    {
        "rule_id":   "DUP_EXACT",
        "name":      "Linhas exatas duplicadas",
        "scope":     "all",
        "columns":   [],
        "check":     "exact_duplicates",
        "threshold": 0,
        "severity":  "critico",
        "condition": "contagem de linhas duplicadas > 0",
    },
    {
        "rule_id":   "DUP_KEY",
        "name":      "incident_id duplicado",
        "scope":     "all",
        "columns":   ["incident_id"],
        "check":     "key_duplicates",
        "threshold": 0,
        "severity":  "critico",
        "condition": "incident_id não é único no dataset",
    },

    # --- Categorias ---
    {
        "rule_id":   "CAT_CASE",
        "name":      "Inconsistência de capitalização em categorias",
        "scope":     "all",
        "columns":   [],
        "check":     "category_case",
        "threshold": 0,
        "severity":  "alerta",
        "condition": "mesmo valor com grafias diferentes (ex: 'Phishing' vs 'phishing')",
    },
    {
        "rule_id":   "CAT_RARE",
        "name":      "Categoria com frequência muito baixa",
        "scope":     "all",
        "columns":   [],
        "check":     "category_rare",
        "threshold": 1.0,
        "severity":  "info",
        "condition": "categoria aparece em < 1% dos registros (possível erro de digitação)",
    },

    # --- Datas ---
    {
        "rule_id":   "DATE_FORMAT",
        "name":      "Data em formato inválido",
        "scope":     "all",
        "columns":   [],
        "check":     "date_format",
        "threshold": 0,
        "severity":  "critico",
        "condition": "valor não parseável como data",
    },
    {
        "rule_id":   "DATE_RANGE",
        "name":      "Data fora do range esperado",
        "scope":     "all",
        "columns":   [],
        "check":     "date_range",
        "threshold": 0,
        "severity":  "alerta",
        "condition": "data anterior a 1990 ou posterior à data de hoje",
    },
    {
        "rule_id":   "DATE_ORDER",
        "name":      "Incoerência temporal entre datas",
        "scope":     "incidents_master.parquet",
        "columns":   ["incident_date", "discovery_date", "disclosure_date"],
        "check":     "date_order",
        "threshold": 0,
        "severity":  "critico",
        "condition": "incident_date > discovery_date  OU  discovery_date > disclosure_date",
    },
    {
        "rule_id":   "DATE_STR_CAST",
        "name":      "Coluna de data armazenada como string",
        "scope":     "all",
        "columns":   [],
        "check":     "date_str_cast",
        "threshold": 80.0,
        "severity":  "info",
        "condition": "coluna string cujo nome indica data e ≥80% dos valores são parseáveis como datetime — considerar conversão na Silver",
    },
]

# Regex para detectar colunas de data pelo nome — cobre qualquer dataset
# sem precisar listar manualmente. A coerência temporal (DATE_ORDER) continua
# sendo configurada explicitamente nas RULES pois é uma regra de negócio.
_DATE_COL_PATTERN = re.compile(
    r"(^date|date$|_date|_at$|^timestamp|timestamp$|_time$|^time_)",
    re.IGNORECASE,
)

# Colunas que são categorias de baixa cardinalidade (candidatas a verificação)
CATEGORICAL_MAX_UNIQUE = 40  # ignora colunas com mais valores únicos que esse limite

# Colunas que nunca devem ser tratadas como categóricas independente de cardinalidade
# (identificadores, texto livre, metadados de ingestão)
_CAT_EXCLUDE_PATTERN = re.compile(
    r"(^id$|_id$|_name$|ticker$|^notes$|^source_|^ingestion_)",
    re.IGNORECASE,
)

# Range válido para datas de negócio — configurar aqui se mudar o domínio
DATE_MIN = pd.Timestamp("1990-01-01")

# ---------------------------------------------------------------------------
# Funções de validação
# ---------------------------------------------------------------------------


def check_nulls(df: pd.DataFrame) -> list[dict]:
    """
    2.1.1 — Verifica nulos por coluna.
    Retorna uma lista de achados classificados por severidade.
    """
    findings = []
    n = len(df)
    for col in df.columns:
        null_count = df[col].isna().sum()
        null_pct   = null_count / n * 100 if n > 0 else 0.0

        if null_pct > RULES[0]["threshold"]:        # > 50% — crítico
            sev = "critico"
        elif null_pct > RULES[1]["threshold"]:      # > 5%  — alerta
            sev = "alerta"
        else:
            continue  # ok — não registra

        findings.append({
            "rule_id":    "NULL_CRITICAL" if sev == "critico" else "NULL_ALERT",
            "coluna":     col,
            "valor":      round(null_pct, 2),
            "contagem":   int(null_count),
            "severidade": sev,
            "detalhe":    f"{null_count}/{n} nulos ({null_pct:.1f}%)",
        })
    return findings


def check_duplicates(df: pd.DataFrame) -> list[dict]:
    """2.1.2 — Verifica duplicatas exatas e por chave primária declarada nas RULES."""
    findings = []
    n = len(df)

    # Duplicatas exatas (excluindo colunas de metadados de ingestão)
    data_cols = [c for c in df.columns if not _CAT_EXCLUDE_PATTERN.search(c)
                 or c == "incident_id"]
    exact_dups = df.duplicated(subset=data_cols).sum()
    if exact_dups > 0:
        findings.append({
            "rule_id":    "DUP_EXACT",
            "coluna":     "(todas)",
            "valor":      round(exact_dups / n * 100, 2),
            "contagem":   int(exact_dups),
            "severidade": "critico",
            "detalhe":    f"{exact_dups} linhas exatamente duplicadas ({exact_dups/n*100:.1f}%)",
        })

    # Duplicatas por chave — lê coluna da regra DUP_KEY
    dup_key_rule = next((r for r in RULES if r["rule_id"] == "DUP_KEY"), None)
    if dup_key_rule:
        key_cols = [c for c in dup_key_rule["columns"] if c in df.columns]
        if key_cols:
            key_dups = df.duplicated(subset=key_cols).sum()
            if key_dups > 0:
                findings.append({
                    "rule_id":    "DUP_KEY",
                    "coluna":     ", ".join(key_cols),
                    "valor":      round(key_dups / n * 100, 2),
                    "contagem":   int(key_dups),
                    "severidade": "critico",
                    "detalhe":    f"{key_dups} chave(s) duplicada(s) em {key_cols} ({key_dups/n*100:.1f}%)",
                })

    return findings


def check_categories(df: pd.DataFrame) -> list[dict]:
    """
    2.1.3 — Detecta inconsistências em colunas categóricas:
    - Variações de capitalização (ex: 'Phishing' vs 'phishing')
    - Categorias com frequência < 1% dos registros (possíveis typos)
    """
    findings = []
    n = len(df)

    cat_cols = [
        c for c in df.select_dtypes(include=["object", "string"]).columns
        if not _CAT_EXCLUDE_PATTERN.search(c)
        and not _DATE_COL_PATTERN.search(c)  # datas já tratadas em check_dates
        and df[c].nunique() <= CATEGORICAL_MAX_UNIQUE
    ]

    for col in cat_cols:
        values = df[col].dropna()
        if values.empty:
            continue

        # Variações de capitalização
        lower_map: dict[str, list[str]] = {}
        for v in values.unique():
            key = str(v).lower().strip()
            lower_map.setdefault(key, []).append(str(v))
        case_conflicts = {k: vs for k, vs in lower_map.items() if len(vs) > 1}
        if case_conflicts:
            findings.append({
                "rule_id":    "CAT_CASE",
                "coluna":     col,
                "valor":      len(case_conflicts),
                "contagem":   len(case_conflicts),
                "severidade": "alerta",
                "detalhe":    f"{len(case_conflicts)} grupo(s) com grafias mistas: "
                              + "; ".join(f"{k!r}: {vs}" for k, vs in list(case_conflicts.items())[:3]),
            })

        # Categorias raras (< 1%)
        freq = values.value_counts(normalize=True) * 100
        rare = freq[freq < RULES[5]["threshold"]]
        if not rare.empty:
            findings.append({
                "rule_id":    "CAT_RARE",
                "coluna":     col,
                "valor":      round(float(rare.min()), 3),
                "contagem":   int(len(rare)),
                "severidade": "info",
                "detalhe":    f"{len(rare)} categoria(s) com freq < 1%: "
                              + ", ".join(f"'{v}' ({p:.2f}%)" for v, p in rare.head(5).items()),
            })

    return findings


def check_type_suggestions(df: pd.DataFrame) -> list[dict]:
    """
    Sinaliza colunas string que deveriam ser convertidas para datetime na Silver.
    Detecta colunas cujo nome indica data e cujos valores são ≥80% parseáveis.
    Não converte nada — apenas informa para orientação da equipe Silver.
    """
    findings = []
    rule = next((r for r in RULES if r["rule_id"] == "DATE_STR_CAST"), None)
    if not rule:
        return findings

    threshold = rule["threshold"]

    for col in df.columns:
        if not (_DATE_COL_PATTERN.search(col)
                and pd.api.types.is_string_dtype(df[col])
                and not pd.api.types.is_bool_dtype(df[col])):
            continue
        # Já é datetime — não precisa de conversão
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            continue

        n_valid = df[col].notna().sum()
        if n_valid == 0:
            continue

        parsed  = _parse_dates(df[col])
        pct_ok  = parsed.notna().sum() / n_valid * 100

        if pct_ok >= threshold:
            sample = df[col].dropna().iloc[0] if not df[col].dropna().empty else ""
            findings.append({
                "rule_id":    "DATE_STR_CAST",
                "coluna":     col,
                "valor":      round(pct_ok, 1),
                "contagem":   int(n_valid),
                "severidade": rule["severity"],
                "detalhe":    f"{pct_ok:.0f}% dos valores são datas válidas armazenadas como string — converter para datetime na Silver (ex: {repr(sample)})",
            })

    return findings


def _parse_dates(series: pd.Series) -> pd.Series:
    """Parse para datetime e normaliza para tz-naive (remove timezone).
    Evita erros de comparação entre datetime tz-aware e tz-naive."""
    parsed = pd.to_datetime(series, errors="coerce")
    if parsed.dt.tz is not None:
        parsed = parsed.dt.tz_localize(None)
    return parsed


def check_dates(df: pd.DataFrame) -> list[dict]:
    """
    2.1.4 — Verifica datas:
    - Colunas string com nome de data que não são parseáveis
    - Datas fora do range [1990, hoje]
    - Incoerência temporal: incident_date <= discovery_date <= disclosure_date
    """
    findings = []
    today = pd.Timestamp.now()

    # Detecta colunas de data pelo nome
    # independente de nomenclatura específica
    date_cols_present = [
        c for c in df.columns
        if pd.api.types.is_string_dtype(df[c]) and not pd.api.types.is_bool_dtype(df[c])
        and _DATE_COL_PATTERN.search(c)
    ]

    for col in date_cols_present:
        parsed = _parse_dates(df[col])
        n_total    = df[col].notna().sum()
        n_unparsed = parsed.isna().sum() - df[col].isna().sum()

        if n_unparsed > 0:
            findings.append({
                "rule_id":    "DATE_FORMAT",
                "coluna":     col,
                "valor":      int(n_unparsed),
                "contagem":   int(n_unparsed),
                "severidade": "critico",
                "detalhe":    f"{n_unparsed} valor(es) não parseáveis em '{col}'",
            })

        # Range
        if n_total > 0:
            out_of_range = parsed[(parsed < DATE_MIN) | (parsed > today)].dropna()
            if not out_of_range.empty:
                findings.append({
                    "rule_id":    "DATE_RANGE",
                    "coluna":     col,
                    "valor":      len(out_of_range),
                    "contagem":   int(len(out_of_range)),
                    "severidade": "alerta",
                    "detalhe":    f"{len(out_of_range)} datas fora de [1990, hoje] em '{col}'",
                })

    # Coerência temporal — lê sequência de colunas e scope da regra DATE_ORDER
    date_order_rule = next((r for r in RULES if r["rule_id"] == "DATE_ORDER"), None)
    if date_order_rule:
        order_cols = [c for c in date_order_rule["columns"] if c in df.columns]
        if len(order_cols) >= 2:
            parsed_seq = [
                _parse_dates(df[c])
                for c in order_cols
            ]
            # Verifica que cada coluna é <= à seguinte na sequência declarada na regra
            broken_order = sum(
                ((parsed_seq[i] > parsed_seq[i + 1]).sum())
                for i in range(len(parsed_seq) - 1)
            )
            if broken_order > 0:
                findings.append({
                    "rule_id":    "DATE_ORDER",
                    "coluna":     " / ".join(order_cols),
                    "valor":      int(broken_order),
                    "contagem":   int(broken_order),
                    "severidade": date_order_rule["severity"],
                    "detalhe":    f"{broken_order} registro(s) com ordem temporal inválida em {order_cols}",
                })

    return findings


# ---------------------------------------------------------------------------
# 2.3 — Métricas de qualidade por coluna
# ---------------------------------------------------------------------------

def _col_profile(series: pd.Series) -> dict:
    """Gera perfil estatístico de uma coluna de forma agnóstica ao tipo."""
    n       = len(series)
    n_null  = int(series.isna().sum())
    n_valid = n - n_null
    profile = {
        "dtype":    str(series.dtype),
        "total":    n,
        "nulos":    n_null,
        "pct_nulos": round(n_null / n * 100, 2) if n > 0 else 0.0,
        "unicos":   int(series.nunique()),
        "min":      None,
        "max":      None,
        "mean":     None,
        "std":      None,
        "top_values": [],
    }

    if n_valid == 0:
        return profile

    # Numéricos
    if pd.api.types.is_numeric_dtype(series):
        profile["min"]  = series.min()
        profile["max"]  = series.max()
        profile["mean"] = round(float(series.mean()), 2)
        profile["std"]  = round(float(series.std()), 2)
    # Datas (já parseadas)
    elif pd.api.types.is_datetime64_any_dtype(series):
        profile["min"] = str(series.min())
        profile["max"] = str(series.max())
    # Strings que parecem datas
    elif pd.api.types.is_string_dtype(series) and not pd.api.types.is_bool_dtype(series) and _DATE_COL_PATTERN.search(series.name):
        parsed = _parse_dates(series)
        if parsed.notna().any():
            profile["min"] = str(parsed.min())
            profile["max"] = str(parsed.max())

    # Top valores para colunas categóricas (string, baixa cardinalidade)
    if (pd.api.types.is_string_dtype(series) and not pd.api.types.is_bool_dtype(series)
            and profile["unicos"] <= CATEGORICAL_MAX_UNIQUE
            and not _CAT_EXCLUDE_PATTERN.search(series.name)):
        vc = series.value_counts(dropna=True).head(5)
        profile["top_values"] = [
            {"valor": str(v), "freq": int(c), "pct": round(c / n * 100, 1)}
            for v, c in vc.items()
        ]

    return profile


def compute_quality_score(findings: list[dict]) -> float:
    """
    Calcula um score de qualidade geral de 0 a 100:
      - Cada achado crítico desconta 10 pontos
      - Cada achado de alerta desconta 3 pontos
      - Achados de info não descontam
    Score mínimo: 0.
    """
    deductions = sum(
        10 if f["severidade"] == "critico" else
        3  if f["severidade"] == "alerta"  else 0
        for f in findings
    )
    return max(0.0, round(100.0 - deductions, 1))


def profile_dataset(df: pd.DataFrame) -> dict:
    """Retorna o perfil estatístico de todas as colunas do DataFrame."""
    return {col: _col_profile(df[col]) for col in df.columns}


# ---------------------------------------------------------------------------
# Orquestrador: roda todas as regras em um DataFrame
# ---------------------------------------------------------------------------

def validate_dataset(df: pd.DataFrame, dataset_name: str) -> dict:
    """
    Aplica todas as regras de validação (2.1) e gera métricas (2.3)
    em um DataFrame. Retorna resultado completo.
    """
    findings = (
        check_nulls(df)
        + check_duplicates(df)
        + check_categories(df)
        + check_dates(df)
        + check_type_suggestions(df)
    )

    n_critico = sum(1 for f in findings if f["severidade"] == "critico")
    n_alerta  = sum(1 for f in findings if f["severidade"] == "alerta")
    n_info    = sum(1 for f in findings if f["severidade"] == "info")
    status    = "REPROVADO" if n_critico > 0 else ("ATENCAO" if n_alerta > 0 else "APROVADO")

    return {
        "dataset":       dataset_name,
        "num_linhas":    len(df),
        "num_colunas":   len(df.columns),
        "status":        status,
        "score":         compute_quality_score(findings),
        "criticos":      n_critico,
        "alertas":       n_alerta,
        "infos":         n_info,
        "findings":      findings,
        "perfil_colunas": profile_dataset(df),
        "avaliado_em":   datetime.now().isoformat(),
    }


# ---------------------------------------------------------------------------
# 2.4 — Relatório de qualidade em Markdown
# ---------------------------------------------------------------------------

def _build_recommendations(result: dict) -> list[str]:
    """Gera uma seção de recomendações concretas para a Silver com base nos achados."""
    lines: list[str] = [f"\n### 📋 Recomendações para a Silver\n"]
    perfil   = result.get("perfil_colunas", {})
    findings = result.get("findings", [])
    dataset  = result["dataset"]

    # 1. Colunas candidatas a descarte (>70% nulos)
    descarte = [c for c, p in perfil.items() if p["pct_nulos"] >= 70.0
                and c not in ("ingestion_timestamp", "source_file", "quality_flag")]
    if descarte:
        lines.append("**Colunas candidatas a descarte (≥70% nulos):**")
        for c in descarte:
            lines.append(f"- `{c}` — {perfil[c]['pct_nulos']}% nulos")
        lines.append("")

    # 2. Colunas que precisam de imputação (5-70% nulos)
    imputar = [c for c, p in perfil.items()
               if 5.0 <= p["pct_nulos"] < 70.0
               and c not in ("ingestion_timestamp", "source_file", "quality_flag")]
    if imputar:
        lines.append("**Colunas que precisam de estratégia de imputação (5–70% nulos):**")
        for c in imputar:
            dtype = perfil[c]["dtype"]
            sugestao = "mediana/média" if "float" in dtype or "int" in dtype else "moda ou categoria 'Desconhecido'"
            lines.append(f"- `{c}` — {perfil[c]['pct_nulos']}% nulos — sugestão: {sugestao}")
        lines.append("")

    # 3. Conversão de tipo (DATE_STR_CAST)
    conv = [f["coluna"] for f in findings if f["rule_id"] == "DATE_STR_CAST"]
    if conv:
        lines.append("**Converter para datetime na Silver:**")
        for c in conv:
            lines.append(f"- `{c}`")
        lines.append("")

    # 4. Datas fora do range
    datas_range = [f for f in findings if f["rule_id"] == "DATE_RANGE"]
    if datas_range:
        lines.append("**Datas fora do range válido [1990, hoje] — tratar ou remover:**")
        for f in datas_range:
            lines.append(f"- `{f['coluna']}` — {f['contagem']} registros")
        lines.append("")

    # 5. Colunas candidatas a join entre datasets
    join_cols = [c for c in perfil if c in ("incident_id", "stock_ticker", "company_name")]
    if join_cols:
        lines.append("**Colunas candidatas a join entre datasets:**")
        for c in join_cols:
            lines.append(f"- `{c}` ({perfil[c]['unicos']} valores únicos, {perfil[c]['pct_nulos']}% nulos)")
        lines.append("")

    if len(lines) == 1:
        lines.append("Nenhuma recomendação — dataset aprovado sem ressalvas.\n")

    return lines
# ---------------------------------------------------------------------------

def _sev_icon(sev: str) -> str:
    return {"critico": "🔴", "alerta": "🟡", "info": "🔵"}.get(sev, "")


def _status_badge(status: str) -> str:
    return {"APROVADO": "✅ APROVADO", "ATENCAO": "⚠️ ATENÇÃO", "REPROVADO": "❌ REPROVADO"}.get(status, status)


def generate_markdown_report(results: list[dict]) -> str:
    """Gera o conteúdo do relatório de qualidade em Markdown."""
    lines = [
        "# Relatório de Qualidade — Camada Bronze",
        f"\n**Gerado em:** {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}",
        f"\n**Datasets avaliados:** {len(results)}",
        "\n---\n",
        "## Resumo Geral\n",
        "| Dataset | Linhas | Colunas | Score | Status |",
        "|---------|--------|---------|-------|--------|",
    ]
    for r in results:
        lines.append(
            f"| {r['dataset']} | {r['num_linhas']} | {r['num_colunas']} "
            f"| {r['score']}/100 | {_status_badge(r['status'])} |"
        )

    for r in results:
        lines += [
            f"\n---\n## {r['dataset']}\n",
            f"- **Status:** {_status_badge(r['status'])}",
            f"- **Score de qualidade:** {r['score']}/100",
            f"- **Críticos:** {r['criticos']} | **Alertas:** {r['alertas']} | **Infos:** {r['infos']}",
        ]

        # Achados por severidade
        for sev in ("critico", "alerta", "info"):
            group = [f for f in r["findings"] if f["severidade"] == sev]
            if not group:
                continue
            label = {"critico": "Problemas Críticos", "alerta": "Alertas", "info": "Informações"}[sev]
            lines += [f"\n### {_sev_icon(sev)} {label}\n",
                      "| Regra | Coluna | Ocorrências | Detalhe |",
                      "|-------|--------|-------------|---------|"]
            for f in group:
                lines.append(
                    f"| `{f['rule_id']}` | `{f['coluna']}` | {f['contagem']} | {f['detalhe']} |"
                )

        # Perfil completo de todas as colunas
        perfil = r.get("perfil_colunas", {})
        if perfil:
            lines += [
                "\n### Perfil Completo das Colunas\n",
                "| Coluna | Tipo | Nulos | % Nulos | Únicos | Min | Max | Média | Desvio |",
                "|--------|------|-------|---------|--------|-----|-----|-------|--------|",
            ]
            for col, p in perfil.items():
                mean_str = p['mean'] if p.get('mean') is not None else "—"
                std_str  = p['std']  if p.get('std')  is not None else "—"
                lines.append(
                    f"| `{col}` | {p['dtype']} | {p['nulos']} | {p['pct_nulos']}% "
                    f"| {p['unicos']} | {p['min']} | {p['max']} | {mean_str} | {std_str} |"
                )

        # Top valores das colunas categóricas
        cats = {c: p for c, p in perfil.items() if p.get("top_values")}
        if cats:
            lines += ["\n### Top Valores — Colunas Categóricas\n"]
            for col, p in cats.items():
                lines.append(f"**`{col}`** ({p['unicos']} categorias)\n")
                lines += ["| Valor | Freq | % |", "|-------|------|---|"]  
                for tv in p["top_values"]:
                    lines.append(f"| {tv['valor']} | {tv['freq']} | {tv['pct']}% |")
                lines.append("")

        # Recomendações para a Silver
        lines += _build_recommendations(r)

    lines.append("\n---\n*Relatório gerado automaticamente por `src/quality.py`*\n")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 2.5 — Dataset validado com flags por linha
# ---------------------------------------------------------------------------

def flag_dataset(df: pd.DataFrame, findings: list[dict]) -> pd.DataFrame:
    """
    Adiciona a coluna `quality_flag` ao DataFrame indicando, por linha,
    quais problemas foram detectados. Valor é uma string com os rule_ids
    separados por '|', ou 'OK' se nenhum problema atingir aquela linha.

    Apenas regras que se aplicam a nível de linha são propagadas:
      - NULL_*    → linha tem nulo em coluna flagada
      - DUP_EXACT → linha é duplicata exata
      - DUP_KEY   → linha tem chave duplicada
      - DATE_*    → linha tem valor de data inválido/fora do range
    Regras de categoria (CAT_*) são por coluna, não por linha — registradas
    no relatório mas não propagadas como flag de linha.
    """
    n = len(df)
    flags = pd.Series([set() for _ in range(n)], index=df.index)

    for finding in findings:
        rule_id = finding["rule_id"]
        col     = finding["coluna"]

        # Nulos — marca linhas onde a coluna específica é nula
        if rule_id in ("NULL_CRITICAL", "NULL_ALERT") and col in df.columns:
            mask = df[col].isna()
            flags[mask] = flags[mask].apply(lambda s: s | {rule_id})

        # Duplicatas exatas
        elif rule_id == "DUP_EXACT":
            data_cols = [c for c in df.columns if c not in ("ingestion_timestamp", "source_file")]
            mask = df.duplicated(subset=data_cols, keep=False)
            flags[mask] = flags[mask].apply(lambda s: s | {rule_id})

        # Duplicatas por chave
        elif rule_id == "DUP_KEY":
            dup_key_rule = next((r for r in RULES if r["rule_id"] == "DUP_KEY"), None)
            if dup_key_rule:
                key_cols = [c for c in dup_key_rule["columns"] if c in df.columns]
                if key_cols:
                    mask = df.duplicated(subset=key_cols, keep=False)
                    flags[mask] = flags[mask].apply(lambda s: s | {rule_id})

        # Datas inválidas (formato)
        elif rule_id == "DATE_FORMAT" and col in df.columns and pd.api.types.is_string_dtype(df[col]) and not pd.api.types.is_bool_dtype(df[col]):
            parsed = _parse_dates(df[col])
            mask = df[col].notna() & parsed.isna()
            flags[mask] = flags[mask].apply(lambda s: s | {rule_id})

        # Datas fora do range
        elif rule_id == "DATE_RANGE" and col in df.columns:
            parsed = _parse_dates(df[col])
            today = pd.Timestamp.now()
            mask = (parsed < DATE_MIN) | (parsed > today)
            flags[mask] = flags[mask].apply(lambda s: s | {rule_id})

        # Ordem temporal inválida
        elif rule_id == "DATE_ORDER":
            date_order_rule = next((r for r in RULES if r["rule_id"] == "DATE_ORDER"), None)
            if date_order_rule:
                order_cols = [c for c in date_order_rule["columns"] if c in df.columns]
                if len(order_cols) >= 2:
                    parsed_seq = [
                        _parse_dates(df[c])
                        for c in order_cols
                    ]
                    broken = pd.Series(False, index=df.index)
                    for i in range(len(parsed_seq) - 1):
                        broken |= parsed_seq[i] > parsed_seq[i + 1]
                    flags[broken] = flags[broken].apply(lambda s: s | {rule_id})

    df = df.copy()
    df["quality_flag"] = flags.apply(lambda s: "|".join(sorted(s)) if s else "OK")
    return df


# ---------------------------------------------------------------------------
# 2.6 — Validação dos entregáveis
# ---------------------------------------------------------------------------
# 2.6 — Validação da qualidade
# ---------------------------------------------------------------------------

REQUIRED_PATHS = [
    REPORTS_PATH / "quality_report.json",
    REPORTS_PATH / "quality_report.md",
]


def validate_quality(results: list[dict]) -> bool:
    """Verifica se todos os entregáveis da Etapa 2 foram gerados corretamente."""
    print("\n" + "=" * 60)
    print("VALIDAÇÃO DE QUALIDADE — ETAPA 2")
    print("=" * 60)
    ok = True

    # 1. Arquivos de relatório existem
    for path in REQUIRED_PATHS:
        if path.exists() and path.stat().st_size > 0:
            print(f"[OK] {path.name} ({path.stat().st_size / 1024:.1f} KB)")
        else:
            print(f"[FALHA] {path.name} não encontrado ou vazio")
            ok = False

    # 2. JSON tem resultados para todos os datasets processados
    json_path = REPORTS_PATH / "quality_report.json"
    if json_path.exists():
        with open(json_path, encoding="utf-8") as f:
            report = json.load(f)
        n_results = len(report.get("resultados", []))
        n_expected = len(results)
        if n_results == n_expected:
            print(f"[OK] quality_report.json contém {n_results} dataset(s)")
        else:
            print(f"[FALHA] quality_report.json tem {n_results} resultado(s), esperado {n_expected}")
            ok = False

        # Verifica campos obrigatórios em cada resultado
        required_fields = {"dataset", "num_linhas", "num_colunas", "status", "score",
                           "criticos", "alertas", "infos", "findings", "perfil_colunas"}
        for r in report.get("resultados", []):
            missing = required_fields - set(r.keys())
            if missing:
                print(f"[FALHA] Campos ausentes em '{r.get('dataset', '?')}': {missing}")
                ok = False
            else:
                print(f"[OK] Campos completos em '{r['dataset']}' | score={r['score']}/100 | status={r['status']}")

    # 3. Parquets validados foram gerados
    validated_files = list(BRONZE_PATH.rglob("*_validated.parquet"))
    if validated_files:
        for vf in validated_files:
            df_check = pd.read_parquet(vf)
            if "quality_flag" in df_check.columns:
                n_ok  = (df_check["quality_flag"] == "OK").sum()
                n_bad = (df_check["quality_flag"] != "OK").sum()
                print(f"[OK] {vf.name} | OK={n_ok} | flagados={n_bad}")
            else:
                print(f"[FALHA] {vf.name} não tem coluna 'quality_flag'")
                ok = False
    else:
        print("[FALHA] Nenhum arquivo *_validated.parquet encontrado em data/bronze/ (incluindo subpastas)")
        ok = False

    print("=" * 60)
    print("RESULTADO:", "PASSOU" if ok else "FALHOU")
    print("=" * 60)
    return ok


# ---------------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------------

def run_quality() -> list[dict]:
    print("=" * 60)
    print("VALIDAÇÃO DE QUALIDADE (Bronze)")
    print("=" * 60)

    results = []

    input_files = [pf for pf in sorted(BRONZE_PATH.glob("*.parquet"))
                   if not pf.stem.endswith("_validated")]

    if not input_files:
        dated_dirs = sorted([p for p in BRONZE_PATH.iterdir() if p.is_dir()]) if BRONZE_PATH.exists() else []
        if dated_dirs:
            latest_dir = dated_dirs[-1]
            input_files = [pf for pf in sorted(latest_dir.glob("*.parquet"))
                           if not pf.stem.endswith("_validated")]
            print(f"[INFO] Usando partição Bronze mais recente: {latest_dir}")

    if not input_files:
        print("[AVISO] Nenhum arquivo parquet de entrada encontrado para validação.")

    for pf in input_files:
        if pf.stem.endswith("_validated"):
            continue  # ignora arquivos de saída desta própria etapa
        print(f"\n>>> {pf.name}")
        df = pd.read_parquet(pf)
        result = validate_dataset(df, pf.name)
        results.append(result)

        print(f"    Status : {result['status']}  |  Score: {result['score']}/100")
        print(f"    Críticos: {result['criticos']}  Alertas: {result['alertas']}  Infos: {result['infos']}")
        for f in result["findings"]:
            icon = {"critico": "[!]", "alerta": "[~]", "info": "[i]"}[f["severidade"]]
            print(f"    {icon} [{f['rule_id']}] {f['coluna']}: {f['detalhe']}")

        # 2.5 — Salva dataset com flags
        df_flagged = flag_dataset(df, result["findings"])
        out_validated = pf.parent / (pf.stem + "_validated.parquet")
        df_flagged.to_parquet(out_validated, index=False)
        n_bad = (df_flagged["quality_flag"] != "OK").sum()
        print(f"    [2.5] validado salvo: {out_validated.name} | {n_bad} linha(s) flagada(s)")

    # 2.3 — Salva JSON estruturado
    report = {
        "gerado_em":        datetime.now().isoformat(),
        "regras_aplicadas": RULES,
        "resultados":       results,
    }
    out_json = REPORTS_PATH / "quality_report.json"
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False, default=str)
    print(f"\n[OK] Relatório JSON salvo em: {out_json}")

    # 2.4 — Gera e salva relatório Markdown
    md_content = generate_markdown_report(results)
    out_md = REPORTS_PATH / "quality_report.md"
    with open(out_md, "w", encoding="utf-8") as f:
        f.write(md_content)
    print(f"[OK] Relatório Markdown salvo em: {out_md}")

    # 2.6 — Valida qualidade
    validate_quality(results)

    return results


if __name__ == "__main__":
    run_quality()
