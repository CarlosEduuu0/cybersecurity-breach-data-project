# Plano Técnico — Pessoa 1: Restruturação da EDA

> **Escopo reduzido.** A EDA já está praticamente pronta em `notebooks/eda.ipynb`. A tarefa
> consiste em reorganizar a narrativa para o padrão exigido pelo enunciado (hipótese →
> gráfico → interpretação) e preparar a estrutura de pastas da camada Gold.

---

## 1. Objetivo

Entregar a EDA orientada a hipóteses com a estrutura:

1. Bloco markdown único no topo apresentando as **3 hipóteses** de negócio.
2. Para cada gráfico (6 no total): **gráfico (code) → interpretação (markdown)** logo abaixo.
3. Cobertura obrigatória: distribuição, outliers, correlação, análise por recorte
   (tipo de ataque / setor / severidade).

---

## 2. Estado atual do notebook

`notebooks/eda.ipynb` já contém:

- 6 gráficos implementados (distribuição, taxa por vetor, correlação, série temporal,
  perda por vetor com escala log, recuperação de preço por vetor).
- 3 hipóteses formuladas, **mas localizadas no final** (célula 19, "Parte 1 — Hipóteses").
- Interpretações curtas após cada gráfico.

**Problemas a corrigir:**

- Ordem invertida: hipóteses aparecem depois dos gráficos.
- Bloco "Resumo e Recomendações" (célula 18) está no meio, antes do Gráfico 6.
- Falta amarração explícita entre cada hipótese e o(s) gráfico(s) que a sustenta(m).

---

## 3. Plano de reestruturação

### 3.1 Criar estrutura de pastas para a camada Gold

```text
data/gold/.gitkeep         # placeholder; dataset será gerado pela Pessoa 2
```

Comando:

```powershell
New-Item -ItemType Directory -Force -Path data\gold | Out-Null
New-Item -ItemType File -Force -Path data\gold\.gitkeep | Out-Null
```

### 3.2 Reordenar células do `eda.ipynb`

**Ordem alvo (cabeçalho → fim):**

| Bloco | Tipo | Conteúdo |
|-------|------|----------|
| 1 | Markdown | Título + objetivo da EDA |
| 2 | Code | Setup (imports, paleta, paths) |
| 3 | Code | Carregar 3 datasets Silver + shapes |
| 4 | **Markdown** | **Parte 1 — Hipóteses de negócio (H1, H2, H3)** ← mover do final |
| 5 | Markdown | H1 — enunciado + qual gráfico a testa |
| 6 | Code | Gráfico 1 (Distribuição de classes) |
| 7 | Markdown | Interpretação do Gráfico 1 |
| 8 | Code | Gráfico 2 (Taxa de severidade por vetor) |
| 9 | Markdown | Interpretação do Gráfico 2 |
| 10 | Markdown | H2 — enunciado + qual gráfico a testa |
| 11 | Code | Gráfico 3 (Correlação) |
| 12 | Markdown | Interpretação do Gráfico 3 |
| 13 | Code | Gráfico 4 (Série temporal por severidade) |
| 14 | Markdown | Interpretação do Gráfico 4 |
| 15 | Markdown | H3 — enunciado + qual gráfico a testa |
| 16 | Code | Gráfico 5 (Boxplot perda por vetor — escala log) |
| 17 | Markdown | Interpretação do Gráfico 5 |
| 18 | Code | Gráfico 6 (Tempo de recuperação por vetor) |
| 19 | Markdown | Interpretação do Gráfico 6 |
| 20 | Markdown | **Resumo e impacto no pipeline** (mover bloco existente para o final) |

### 3.3 Mapeamento hipótese ↔ gráficos

| Hipótese | Gráficos que a sustentam |
|----------|--------------------------|
| **H1 — Vetores sofisticados concentram severidade** | G1 (baseline de severidade), G2 (taxa por vetor) |
| **H2 — Padrões estruturais e temporais influenciam severidade** | G3 (correlação numérica), G4 (evolução anual) |
| **H3 — Impacto financeiro e de mercado variam por vetor** | G5 (perda em USD), G6 (recuperação de preço) |

### 3.4 Pequenos ajustes nas interpretações

Para cada gráfico, garantir que a interpretação contenha:

1. **Observação visual** (o que o gráfico mostra).
2. **Conclusão** (confirma/refuta a hipótese ou aponta padrão).
3. **Impacto na próxima etapa** (ex.: "exige scaling robusto", "indica encoding por frequência",
   "justifica clipping IQR em `total_loss_usd`").

---

## 4. Passo a passo de execução

Seguir exatamente nesta ordem:

```
1. Criar pasta data/gold/ com .gitkeep
   └─ New-Item -ItemType Directory -Force -Path data\gold
   └─ New-Item -ItemType File -Force -Path data\gold\.gitkeep

2. Abrir notebooks/eda.ipynb no VS Code

3. MOVER a célula "Parte 1 — Hipóteses" (atualmente a penúltima)
   para logo após a célula de carregamento dos datasets (célula 3 atual)

4. Inserir célula markdown antes do Gráfico 1 com:
   "### Hipótese 1 — Vetores sofisticados concentram severidade
    Testada pelos Gráficos 1 e 2."

5. Verificar que Gráfico 1 + Interpretação 1 ficam logo abaixo

6. Verificar que Gráfico 2 + Interpretação 2 ficam logo abaixo

7. Inserir célula markdown antes do Gráfico 3 com:
   "### Hipótese 2 — Padrões estruturais e temporais influenciam severidade
    Testada pelos Gráficos 3 e 4."

8. Verificar que Gráfico 3 + Interpretação 3 ficam logo abaixo

9. Verificar que Gráfico 4 + Interpretação 4 ficam logo abaixo

10. Inserir célula markdown antes do Gráfico 5 com:
    "### Hipótese 3 — Impacto financeiro e de mercado variam por vetor
     Testada pelos Gráficos 5 e 6."

11. Verificar que Gráfico 5 + Interpretação 5 ficam logo abaixo

12. Verificar que Gráfico 6 + Interpretação 6 ficam logo abaixo

13. Mover bloco "Resumo e Recomendações" para o FINAL do notebook

14. Em CADA interpretação, verificar/adicionar:
    a) Observação visual
    b) Conclusão (confirma/refuta hipótese)
    c) Impacto nas próximas etapas

15. Executar notebook inteiro (Restart & Run All)
    └─ Corrigir qualquer erro de célula

16. Commit: "refactor(eda): reorganizar hipóteses no topo + criar data/gold/"
```

---

## 5. Entregáveis

- `notebooks/eda.ipynb` reestruturado e executado fim-a-fim sem erros.
- `data/gold/` criada (vazia, com `.gitkeep`).
- Bloco final consolidando recomendações que alimentam o trabalho da Pessoa 2.

---

## 6. Critérios de aceitação

- [ ] Bloco de hipóteses aparece **antes** de qualquer célula de código de gráfico.
- [ ] Sequência exata: gráfico → interpretação, repetida 6 vezes.
- [ ] Cada hipótese referencia explicitamente quais gráficos a testam.
- [ ] Notebook executa sem erros do início ao fim.
- [ ] Cobertura: distribuição (G1), correlação (G3), outliers (G5, G6 — boxplots), recorte
  por vetor de ataque (G2, G5, G6).
- [ ] Pasta `data/gold/` existe no repositório.

---

## 7. Dependências

- **Nenhuma**. Esta frente é totalmente independente e não bloqueia ninguém.

---

## 8. Complexidade

⭐⭐☆☆☆ — Trabalho de reorganização e refinamento, não de implementação nova.
