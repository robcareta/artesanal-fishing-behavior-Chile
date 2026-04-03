# CLAUDE.MD -- Empirical Fisheries Science Research with Claude Code

**Project:** Aquaculture and Artisanal Fisheries Interaction in Chile — Los Lagos Region
**Institution:** University of Florida (UF)
**Field:** Fisheries Science / Marine Ecology / Environmental Economics
**Branch:** main

---

## Core Principles

- **Plan first** -- enter plan mode before non-trivial tasks; save plans to `quality_reports/plans/`
- **Verify after** -- compile and confirm output at the end of every task
- **Single source of truth** -- Paper `paper/main.tex` is authoritative; talks and supplements derive from it
- **Quality gates** -- weighted aggregate score; nothing ships below 80/100; see `quality.md`
- **Worker-critic pairs** -- every creator has a paired critic; critics never edit files
- **Auto-memory** -- corrections and preferences are saved automatically via Claude Code's built-in memory system

---

## Getting Started

1. Run `/discover --lit [topic]` to search literature
2. Run `/strategize [question]` for analytical strategy
3. Run `/analyze [dataset]` for data analysis
4. Run `/write [section]` to draft paper sections

---

## Folder Structure

```
artesanal-fishing-behavior-Chile/
├── CLAUDE.md                    # This file
├── .claude/                     # Rules, skills, agents, hooks
├── Bibliography_base.bib        # Centralized bibliography
├── paper/                       # Main LaTeX manuscript (source of truth)
│   ├── main.tex
│   ├── sections/
│   ├── figures/
│   ├── tables/
│   ├── talks/
│   └── supplementary/
├── data_raw/                    # SERNAPESCA VMS raw CSVs (6,082 files, ~13 GB)
├── data_processed/              # Processed datasets (los_lagos_filtered.csv — 65.8M rows)
├── R/                           # Legacy R scripts
├── new/                         # Active analysis scripts
│   ├── rebuild_los_lagos.R      # Rebuilds los_lagos_filtered.csv from data_raw
│   ├── analyze_filtered.R       # Stats + heatmap + time series
│   ├── effort_analysis.R        # Fishing effort: speed-threshold classification
│   ├── stats_and_plots.R        # Full descriptive report from DuckDB
│   └── outputs/                 # Generated figures and reports
├── quality_reports/             # Plans, session logs, reviews, scores
├── explorations/                # Research sandbox
└── master_supporting_docs/      # Reference papers and data docs
```

---

## Data Overview

| Dataset | Rows | Period | Source | Notes |
|---------|------|--------|--------|-------|
| `data_raw/` | ~6,082 CSVs | 2019–2026 | SERNAPESCA | 3 file formats; see rebuild_los_lagos.R |
| `data_processed/los_lagos_filtered.csv` | 65,836,684 | 2019–2026 | Rebuilt | bbox lat -44.5/-40.5, 3 source categories |
| `new/loslagos_rebuilt.duckdb` | 58.5M | 2019–2026 | DuckDB | Alternative; built from dt_all.csv |

**Key variables:** movil (vessel ID), fecha (datetime), latitud, longitud, rumbo (heading), velocidad (speed kt), source_file, category

**Effort classification:** 0.1–3.0 knots = fishing (speed threshold per Behivoke et al. 2021; Gerritsen & Lordan 2011)

---

## Commands

```bash
# Run R analysis scripts
"/c/Program Files/R/R-4.5.1/bin/Rscript.exe" new/analyze_filtered.R

# DuckDB inspection (no fread on large files — use DuckDB streaming)
# See new/inspect_loslagos_filtered.R for examples
```

**IMPORTANT — Windows memory constraint:** Never use `fread()` or `read.csv()` on files > 4 GB.
Always stream large CSVs through DuckDB with `read_csv(..., all_varchar=true, ignore_errors=true)`.

---

## Quality Thresholds

| Score | Gate | Applies To |
|-------|------|------------|
| 80 | Commit | Weighted aggregate (blocking) |
| 90 | PR | Weighted aggregate (blocking) |
| 95 | Submission | Aggregate + all components >= 80 |
| -- | Advisory | Talks (reported, non-blocking) |

---

## Skills Quick Reference

| Command | What It Does |
|---------|-------------|
| `/new-project [topic]` | Full pipeline: idea → paper (orchestrated) |
| `/discover [mode] [topic]` | Discovery: interview, literature, data, ideation |
| `/strategize [question]` | Identification strategy or pre-analysis plan |
| `/analyze [dataset]` | End-to-end data analysis |
| `/write [section]` | Draft paper sections + humanizer pass |
| `/review [file/--flag]` | Quality reviews (routes by target: paper, code, peer) |
| `/revise [report]` | R&R cycle: classify + route referee comments |
| `/talk [mode] [format]` | Create Beamer presentations |
| `/submit [mode]` | Journal targeting → package → audit → final gate |
| `/tools [subcommand]` | Utilities: commit, compile, validate-bib, journal, etc. |

---

## Current Project State

| Component | File | Status | Description |
|-----------|------|--------|-------------|
| Data pipeline | `new/rebuild_los_lagos.R` | complete | 65.8M rows, 2019–2026 |
| Descriptive stats | `new/analyze_filtered.R` | complete | Heatmap + time series |
| Effort analysis | `new/effort_analysis.R` | complete | Speed-threshold effort, spatial map |
| Paper | `paper/main.tex` | not started | Aquaculture-fisheries interaction |
| Literature | `Bibliography_base.bib` | in progress | VMS, effort, Chile fisheries |

---

## Research Context

**Research question:** How does aquaculture spatial expansion in Los Lagos (Chile) affect the fishing behavior and effort distribution of artisanal fishing fleets?

**Key finding so far:** 2019–2022 shows 14–19M pings/year with ~1,000 vessels; 2023–2026 drops sharply to <1.7M pings/year with ~175–190 vessels from the `flota_artesanal` daily reports. The `admin_2019` source (snapshot reports, 4×/day) covers 2019–2022 and dominates that period.

**Spatial focus:** Los Lagos Region (bbox lat -44.5 to -40.5, lon -75.5 to -71.5)
