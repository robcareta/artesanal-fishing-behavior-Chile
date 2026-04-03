# Domain Profile

<!--
HOW TO USE: Fill this in manually OR let /discover (interactive interview) generate it.
All agents read this file to calibrate their field-specific behavior.
Delete sections that don't apply. Add sections specific to your field.
If no field is specified, agents default to applied economics.
-->

## Field

**Primary:** Fisheries Science, Marine Ecology, Environmental Economics
**Adjacent subfields:** Spatial Ecology, Resource Economics, Conservation Biology, Aquaculture Science

---

## Target Journals (ranked by tier)

| Tier | Journals |
|------|----------|
| Top field | Fish and Fisheries, ICES Journal of Marine Science, Journal of Applied Ecology |
| Strong field | Fisheries Research, Marine Policy, Aquaculture, Ocean & Coastal Management |
| Specialty | Ciencias Marinas, Fisheries Management and Ecology, Reviews in Aquaculture |
| Open access | PLOS ONE, Frontiers in Marine Science |

---

## Common Data Sources

| Dataset | Type | Access | Notes |
|---------|------|--------|-------|
| SERNAPESCA VMS | Admin / panel | Public | 15-min pings, 2019–2026, Los Lagos. movil, fecha, lat, lon, speed, heading. 3 source formats (admin_2019, flota_artesanal, artesanal_legacy). |
| SERNAPESCA aquaculture concessions | Spatial polygons | Public | Species, area, concession boundaries |
| SERNAPESCA catch statistics | Annual aggregates | Public | By region, species, fleet type |
| Global Fishing Watch | AIS / VMS | Public API | Chile published VMS 2020+; useful for cross-validation |
| GADM Chile Level 1 | Admin boundaries | Public | geodata::gadm("CHL", level=1) |

---

## Common Identification Strategies

| Strategy | Typical Application | Key Assumption to Defend |
|----------|-------------------|------------------------|
| Spatial DiD | Before/after aquaculture expansion vs. control zones | Parallel trends in fishing effort absent treatment |
| Spatial regression | Fishing effort ~ distance to nearest concession + controls | No endogenous concession placement near already-declining areas |
| Speed-threshold effort | Classify pings as fishing (0.1–3 kt) vs. steaming | Threshold calibrated for artisanal vessels (Behivoke et al. 2021) |
| Panel vessel FE | Vessel-year FE on effort metrics | Unobservable vessel heterogeneity absorbed |

---

## Field Conventions

- **VMS effort units:** fishing hours, fishing days, or fishing pings — always specify metric and cite justification
- **Speed threshold:** cite Gerritsen & Lordan (2011) for industrial; Behivoke et al. (2021) for artisanal; discuss gear heterogeneity
- **Spatial resolution:** 0.05° (~5 km) for regional maps; 0.01° for hotspot analysis
- **Source heterogeneity:** admin_2019 (4×/day snapshots) ≠ flota_artesanal (daily); treat as separate sources or include source fixed effect
- **Displacement vs. reduction:** always test whether fishing effort displaced spatially or genuinely reduced near aquaculture
- **Date formats:** SERNAPESCA mixes DD/MM/YYYY and YYYY-MM-DD — verify year extraction logic explicitly

---

## Notation Conventions

| Symbol | Meaning | Anti-pattern |
|--------|---------|-------------|
| $E_{it}$ | Fishing effort for vessel $i$ at time $t$ | Don't use $e$ without subscripts |
| $d_{ij}$ | Distance from vessel ping $i$ to concession $j$ | |
| $F_{it}$ | Binary fishing indicator (1 = fishing ping) | |
| $h_{it}$ | Fishing hours for vessel-period $it$ | |

---

## Seminal References

| Paper | Why It Matters |
|-------|---------------|
| Gerritsen & Lordan (2011) ICES JMS | Gold standard VMS speed-threshold for industrial trawlers |
| Hintzen et al. (2012) Fisheries Research | VMStools R package — standard EU VMS pipeline |
| Behivoke et al. (2021) Ecological Indicators | Random forest effort detection for artisanal/small-scale vessels |
| Peel & Good (2011) CJFAS | HMM approach for VMS activity classification |
| Walker & Bez (2019) Royal Society Open Science | State-space model for small-scale fisheries GPS data |
| Natale et al. (2015) PLOS ONE | AIS-based fishing effort mapping — precursor to GFW |

---

## Field-Specific Referee Concerns

- "Are speed thresholds appropriate for mixed-gear artisanal fleets?" — discuss gear heterogeneity, cite artisanal-specific methods
- "Is the post-2022 observation drop a data artifact or real?" — explain source switch from admin_2019 to flota_artesanal
- "Endogeneity of concession placement" — concessions may be placed where fishing was already low; address with spatial controls or IV
- "Temporal imbalance" — 2019–2022 from high-frequency snapshots vs. 2022–2026 daily; robustness check restricted to flota_artesanal only
- "Displacement vs. reduction" — referees expect spatial test of whether effort moved, not just declined
- "External validity" — Los Lagos is salmon-dominated; findings may not generalize to other Chilean regions

---

## Quality Tolerance Thresholds

| Quantity | Tolerance | Rationale |
|----------|-----------|-----------|
| Coordinates | 0.001° (~100m) | VMS GPS precision |
| Speed | 0.01 kt | Instrument precision |
| Fishing hours | 0.25 h | 15-min ping interval |
