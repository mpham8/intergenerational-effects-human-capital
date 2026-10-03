# Preliminary Results Triage

This is the `cleanup` branch, based on preprocessing commit `8bd1e5b` (`first round results`). It keeps the September 15 preliminary results and the source needed to inspect or reproduce that Del Boca run. It is a reduced diagnostic snapshot, not a corrected model. Merging this branch back would also remove the unrelated and historical files omitted here.

## Retained Files

| Location | Purpose |
| --- | --- |
| `parameter-estimation/prelim results/Del_Boca_Prelim_results_20260915_095148/` | Five original result files: optimizer estimates, target moments, full saved chain, Excel summaries, and caveats/run notes. |
| `parameter-estimation/Attanasio Replication/VDE DelBoca Export/` | One copy of the bridge and four joint-mixture exports. The default Del Boca input path points here. |
| `parameter-estimation/Del_Boca_Replication/Del_Boca_Prelim.jl` | Main preliminary implementation; uses the tau-workaround loader. |
| `parameter-estimation/Del_Boca_Replication/Rev3_Data.jl` | Bridge/mixture loading, validation, and transformations. Retained whole so the historical behavior remains inspectable. |
| `parameter-estimation/Del_Boca_Replication/Rev3_Tau_Workaround_Data.jl` | Whole-household rejection sampling for feasible time shares. |
| `parameter-estimation/Del_Boca_Replication/Julia_DelBoca_SMM.jl` | Two-stage SMM and weighting. Its fallback include now selects `Del_Boca_Prelim.jl`, the only implementation retained. |
| `parameter-estimation/Del_Boca_Replication/Julia_adaptive_metropolis.jl` | Metropolis chain, serialization and summaries, including the known accepted-only reporting issue. |
| `parameter-estimation/Del_Boca_Replication/Rev3_Tau_Workaround_Smoke_Tests.jl` | Reduced data, policy, threaded simulation, optimizer and bridge checks. |
| `parameter-estimation/Del_Boca_Replication/Rev3_Tau_Workaround_Dry_Run.jl` | Short complete Del Boca estimation, including Metropolis. |
| `parameter-estimation/Del_Boca_Replication/Rev3_Tau_Workaround_Substantive_Run.jl` | Full-run driver for later reproduction; not needed for initial triage. |
| `dsge-simulations/new_CES_solver.jl` | Actual policy solver used by this run. |
| `parameter-estimation/Project.toml` and `Manifest.toml` | Existing Julia environment; dependencies were not re-resolved for cleanup. |
| Six R files in `parameter-estimation/Attanasio Replication/` | Master, measurement specification, factor estimator, EM helper, procedures, and income-instrument production specification for upstream source inspection. |

Older implementations, benchmarks, historical runs, the DSGE notebook, preprocessing datasets, duplicate export copies, V2 and the handoff are not included. The six R scripts do not establish which exact source version was executed inside the VDE; compare that version before attributing problems to transformations.

## Start With Small Checks

From this branch's repository root:

```sh
cd parameter-estimation
julia --project=. -e 'using Pkg; Pkg.instantiate()'
julia --project=. --threads=4 Del_Boca_Replication/Rev3_Tau_Workaround_Smoke_Tests.jl
```

For a short complete Del Boca run, including the short Metropolis chain:

```sh
julia --project=. --threads=4 Del_Boca_Replication/Rev3_Tau_Workaround_Dry_Run.jl
```

Both use the included exports, not restricted NLSY microdata. `IEHC_ATTANASIO_OUTPUT_DIR` can point to another complete export directory. The dry-run driver writes a fresh timestamped directory; `IEHC_TAU_WORKAROUND_DRY_RUN_OUTPUT_DIR` can override it. Do not point it at the preserved results. No new substantive estimation was performed to create this branch.

The R suite is retained for diagnosis, not called by the Del Boca drivers. Running it requires R packages and a source CSV provided through `IEHC_NLSY_DATA_FILE`, plus a separate `IEHC_ATTANASIO_OUTPUT_DIR`. No microdata are bundled here. The fake CSV remains in the original preprocessing checkout at `data-preprocessing/Fake Merged Data/Fake_Merged_Data_with_cex_education_expenditure.csv`; use an absolute path to that external file for local upstream tests. Fresh Attanasio technology and mixture exports must come from the same run.

## What To Investigate

The second-stage optimizer returned `[100, -1.3193123, 0.05, 0.95, 0.95]` for `[Delta, perceived_rho, perceived_theta_h_1, perceived_theta_h_2, perceived_theta_h_3]`. Delta and all theta_h estimates are at their bounds. A completed run is not evidence that the implementation is economically coherent.

1. Check the meaning and units of exported income versus wages, and their consistency with normalized CEX spending. Family income is not automatically a wage rate.
2. Resolve the time-share distribution. The workaround retained 10,000 of 38,305 draws and changes the joint distribution without re-estimating the bridge. Child-directed time and leisure remain conflated.
3. Compare shared mean-input policy grids against household-specific solutions on a small sample. Check grid coverage and extrapolation.
4. Audit fixed initial child HC and expected-shock forward transitions against the intended heterogeneous stochastic simulation.
5. Summarize the saved full Metropolis chain, including repeated states after rejections. The existing spreadsheet summarizes accepted proposals only. Resolve objective/inference scaling before presenting intervals as valid uncertainty estimates.
6. Use known-parameter recovery tests before another full estimation. Inspect individual moment residuals; the weighted objective alone can conceal large time/investment misses.

The only estimation-source change introduced by cleanup is the SMM fallback include. Numerical fixes are intentionally deferred so this branch remains a baseline for controlled comparisons.
