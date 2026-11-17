# Estimation Pipeline

The worktree contains the runnable Attanasio and Del Boca pipeline and the fully merged fake NLSY/CEX input. The repo review and checklist record fixes and validation.

## Attanasio

In `parameter-estimation/Attanasio Replication/`, `Master File.r` runs the estimation. `OurR_MeasurementSpec_No4.r` prepares the data and defines the measurements; `Our_Estimate_FactorModel.R`, `Our_estim_meas_model.R`, and `Our_Procedures.R` estimate and simulate the latent-factor distribution; `Our_Production_Functions_No4_Incomeinst.r` estimates production technology; and `Our_Export_Rev3.R` writes the Del Boca handoff.

`data-preprocessing/Fake Merged Data/Fake_Merged_Data_with_cex_education_expenditure.csv` is the fully processed fake dataset, including CEX education expenditure. Attanasio uses it by default; `IEHC_NLSY_DATA_FILE` selects another input.

## Del Boca

In `parameter-estimation/Del_Boca_Replication/`, `Rev3_Tau_Workaround_Substantive_Run.jl` runs the preliminary estimation. It uses `Del_Boca_Prelim.jl` for household simulation, `Rev3_Data.jl` for the exported inputs, `Julia_DelBoca_SMM.jl` for two-stage SMM, and `Julia_adaptive_metropolis.jl` for the saved chain and summaries. `Rev3_Tau_Workaround_Data.jl` retains its old name for existing calls but now draws the full population without rejection. `dsge-simulations/new_CES_solver.jl` solves parental choices. `Project.toml` and `Manifest.toml` record the Julia environment.

## Handoff and Results

Attanasio and Del Boca both default to `Our Most Recent Output` in the Attanasio folder. Use the same `IEHC_ATTANASIO_OUTPUT_DIR` for both stages when selecting another folder. The handoff is the production bridge, mixture probabilities and means, two covariance matrices, involvement calibration, and schema-4 manifest.

Del Boca writes parameter estimates, target moments, run notes, and chain artifacts. `Rev3_Full_Pipeline_Bootstrap.jl` is the separate inference entrypoint; it needs the original input and an explicitly specified sampling-cluster column. Historical results and exports are not runnable inputs for this version.

## Validation and estimation settings

The preliminary activity-index model is connected throughout the pipeline. Reading anchors the first two periods; performance participation links the last two with the same estimated loading and intercept. Help learning numbers supplies the third early-period activity measurement. These identify an involvement index, not hours of childcare. A common conversion is calibrated as pooled mean involvement divided by pooled observed nonwork and is fixed during SMM. Both perceived and true production use this conversion, while utility still uses physical nonwork. Column 6 and its SMM moments now mean educational involvement.

Review 3a uses the same factor-mean and overlapping-anchor restrictions in fitting and export, including the proxy-loading adjustment to exported covariances. Review 3b no longer clips activity draws or rejects households. The bounded structural index is a moment-matching approximation to the unbounded factor distribution, not an exact distributional model. The loader stops when the configured conversion cannot attain the target means and variances. `IEHC_INVOLVEMENT_TEST_MODE=1` uses one for explicitly labeled computational tests; `IEHC_INVOLVEMENT_SCALE` selects an explicit sensitivity calibration. Normal runs use the exported data calibration.

Family resources equal fixed family income plus maternal earnings at chosen labor. Attanasio uses maternal wage and salary earnings (`LABOR_INCOME`) to calibrate the earnings rate when reliable and predicts it from maternal education within each age period otherwise. All budget and spending inputs use annual real 2026 dollars. The two components reproduce reported family income at observed labor, including when maternal hours are zero. No spouse or transfer model, denominator floor, or involvement-to-labor conversion is used. Annual hours still use `52*40`, with overtime capped at one.

Schema 4 exports both budget components jointly with the factor distribution. Historical exports cannot be relabeled or reused. `budget_calibration_diagnostics.csv` reports wage predictions, inconsistent inputs, and cases where predicted wages were constrained by the observed budget. The reliability threshold is `IEHC_MIN_RELIABLE_WAGE_HOURS` (default 208 annual hours). The full-pipeline cluster bootstrap refits the wage rule in each resample. No restricted-data results have been regenerated.

The small computational corrections also place `y_summary.csv` and `EMstart1.Rdata` in the output folder, preserve launch-relative input paths, and seed R through `IEHC_R_SEED`. The seed and package versions are recorded with each run; this is not a dependency lockfile. Covariance diagnostics now handle numerical singularity using absolute eigenvalues and retain the separate negative-eigenvalue warning.

The new budget was tested at zero, near-zero, and positive hours, including inconsistent maternal-earnings records. Attanasio completed on the unmodified fake input with 2,641 complete child panels, and the resulting bundle completed a reduced Del Boca run. A separate synthetic panel completed the full-pipeline reference fit and two cluster resamples. R checks and 95 Julia assertions passed. These checks do not validate the wage model or economic estimates in the restricted data.

The EM measurement indexing in `Our_estim_meas_model.R` keeps a separate list of observed measurements for each household. Covariance slices remain matrices when a household has only one observed measurement. Focused R checks covered complete and mixed-missingness data.

Use the Julia environment recorded by `parameter-estimation/Project.toml` and `Manifest.toml`. R needs the packages loaded by the Attanasio scripts, including `digest` for export. The new budget distribution and any accompanying diagnostics require fresh VDE disclosure review. `IEHC_RUN_BOOTSTRAP=0` skips both Attanasio-only factor and production bootstraps and writes point-estimate tables.

Simulation uses fixed household/period production shocks. Household-specific policies are the default throughout SMM and Metropolis: each household chooses using its own income-capacity and public-input trajectories. The solve uses one parent-education point when education is constant across periods. `IEHC_PARENT_POLICY_MODE=shared` explicitly opts into the faster approximation at sample-average trajectories. `benchmark_parent_policies(households, parameters)` compares the modes on a small batch using the same child grid and shocks. Household-specific solves still cost more; full-sample runtime and approximation error have not been measured.

Attanasio evaluates the nested CES in logs, including the zero-exponent limit, and keeps the lowest-loss converged solution from four starts. Main fits at a bound write objective profiles and wider-bound sensitivity checks alongside the start and fit diagnostics. The bridge uses the original bounds. Synthetic tests recovered interior parameters and detected a deliberately constrained fit; the historical restricted-data boundary estimates have not been reassessed.

SMM point estimates are the DX-NES minimizers. Metropolis summaries are descriptive, not standard errors. The full-pipeline bootstrap resamples clusters, then refits Attanasio, the wage prediction, the involvement calibration, and both SMM stages. Its inputs include `IEHC_BOOTSTRAP_CLUSTER_COLUMN`, optional `IEHC_BOOTSTRAP_STRATA_COLUMN`, `IEHC_FULL_BOOTSTRAP_REPS`, and `IEHC_FULL_BOOTSTRAP_OUTPUT_DIR`. Percentile intervals are opt-in and subject to safeguards; substantive coverage has not been validated. New checkpoints resume complete estimator state; older checkpoints cannot be resumed safely.
