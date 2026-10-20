# Repository review

## Implementation status for findings 4–11 (October 20, 2026)

The findings below describe the reviewed baseline. The current implementation addresses them as follows:

- **4 — fixed:** Realized production paths use seeded household/period normal draws, transformed with the exported residual standard deviations. Draws stay fixed across parameter evaluations and thread counts; later choices condition on realized child human capital. Policy solution still integrates uncertainty by quadrature.
- **5 — approximation retained explicitly:** `benchmark_parent_policies` compares shared and household-specific policies on identical grids and shocks. `IEHC_PARENT_POLICY_MODE=household` enables household-specific wage/public-input trajectories throughout estimation. The default `shared` mode remains a computational approximation. A synthetic heterogeneous-household test finds different choices; it does not establish the approximation error for the full estimation sample.
- **6 — bookkeeping fixed; inference limitation retained:** Summaries, burn-in, ESS, plots and adaptive covariance use the full chain, including rejected states. Burn-in excludes sample/covariance warmup. Excel and plot labels state that chain spread is uncalibrated. DX-NES minimizers remain the SMM point estimates; changing labels does not validate uncertainty or propagate first-stage estimation error.
- **7 — fixed:** Production estimation exports the technology bridge and all four mixture CSVs from the same fitted distribution, with a run manifest and SHA-256 checksums. Julia rejects interrupted or inconsistent new bundles; preserved VDE bundles without a manifest remain readable. Production bootstrapping and bootstrap tables honor `IEHC_RUN_BOOTSTRAP`; disabled runs write point estimates only. The exporter requires the R package `digest`.
- **8 — diagnostics added; bounds retained:** All six production fits must report convergence. `production_fit_diagnostics.csv` records objective values, iteration counts, boundary parameters and Jacobian condition numbers. Boundary estimates produce warnings. No economic justification was found for mechanically widening bounds. Identification, objective profiles, and substantive bound sensitivity still require research.
- **9 — fixed for new checkpoints:** Versioned checkpoints store every estimator field, including the RNG, households, stage, exact counters and histories. Loading validates dimensions, parameter definitions, target moments, technology, code, manifest and simulation configuration. Legacy checkpoints remain readable as historical artifacts but are refused for resumption because they lack required state.
- **10 — fixed:** Sample replacement immediately recomputes the current state's loss, and the sampling loop refreshes its cached value. Household adaptation stops at covariance-warmup end and is disabled in stage two. The two-step driver fixes the maximum household sample before either optimization stage.
- **11 — fixed:** Acceptance includes reverse/forward proposal densities for lognormal, truncated-normal, beta and custom proposals, including overrides under mixed/adaptive strategies. Custom functions must provide `sampler(rng, current)` and `logpdf(to, from)`.

Validation: `parameter-estimation/tests/runtests.jl` exercises stochastic dispersion, deterministic/threaded simulation, policy comparisons, repeated-state summaries, known-target sampling, exact checkpoint resumption, and sample-loss refreshes. `parameter-estimation/tests/test_exports.R` tests export integrity, the complete bootstrap-disabled production control flow with fixed fit values, and real nonlinear-fit convergence/boundary diagnostics. Full EM/SMM estimation has not been rerun; saved estimates have not been regenerated.

### 1. High: EM indexing uses the wrong measurements

- **Bug:** `apply(y, 1, scanNA)` returns a matrix for complete data. The code expects a list, so `comp[[i]]` returns one column index instead of all observed columns for household `i`.
- **Effect:** The E/M steps use the wrong data. Scalar covariance slices may also cause errors.
- **Fix:** Build a list explicitly: `lapply(seq_len(nrow(y)), function(i) which(!is.na(y[i, ])))`. Check complete and partially missing rows.
- **Code:** `Our_estim_meas_model.R:196`, used at lines 76–84 and 130–159. Complete-case filtering: `OurR_MeasurementSpec_No4.r:301`.

### 2. High: Monetary units and investment means are inconsistent

#### Income and spending use different units (modeling concern)

- **Problem:** Spending is annual expenditure divided by median labor income. Income is standardized log total family income, then exponentiated and used as a wage multiplied by parent human capital and labor.
- **Why it matters:** These transformations do not put resources and spending in common units. No compensating conversion appears in this path.
- **Next step:** Define a common monetary unit and an explicit mapping from observed income to wages. Check both sides of the budget in those units.
- **Code:** `OurR_MeasurementSpec_No4.r:94–109`, `:165`, `:249–261`; `Rev3_Tau_Workaround_Data.jl:67–78`; `Del_Boca_Prelim.jl:249`.

#### Investment means are changed after fitting (confirmed bug)

- **Bug:** The code replaces the second mixture component's input means to force the overall log mean to zero. Investment is logged spending in defined monetary units, so its mean should not be arbitrarily zero.
- **Effect:** Exported investment no longer preserves its fitted spending level. Changing only one component can also change the mixture's shape.
- **Fix:** Preserve fitted means for monetary inputs; center only variables whose definition requires it.
- **Code:** `Our_estim_meas_model.R:399–400`; spending inputs: `OurR_MeasurementSpec_No4.r:249–251`.

### 3. High: Time-share restrictions and filtering are inconsistent

#### Baseline restriction conflicts with physical shares (confirmed bug)

- **Bug:** The first-period log time-share anchor is forced to have mixture mean zero. A physical share is at most one, so its log is at most zero and generally has a negative mean. Returned factor means also do not reproduce the same baseline restriction used in fitting.
- **Effect:** The fitted restrictions and exported time distribution are inconsistent with each other and with the intended scale.
- **Fix:** Keep the physical time scale, reconcile baseline restrictions, and verify that reconstructed measurement means match the fitted means.
- **Code:** `OurR_MeasurementSpec_No4.r:367–377`; `Our_estim_meas_model.R:320–325`, `:389–392`.

#### Filtering changes the population (modeling concern)

- **Problem:** The saved run kept 10,000 of 38,305 joint draws (26.106%), rejecting any draw with a time share above one.
- **Why it matters:** Conditioning can change correlated variables and mixture weights. The downstream population differs from the unfiltered population used upstream.
- **Next step:** Estimate a consistent bounded-time model and bridge, or explicitly treat results as conditional on the filter.
- **Scope:** The rejection sampler correctly implements the stated condition. This is a modeling concern, not a rejection-sampling bug.
- **Code:** `Rev3_Tau_Workaround_Data.jl:26–48`; retained count: `Del_Boca_Prelim_results_SUBSTANTIVE_RUN.txt`.

### 4. High: Simulation removes production-shock dispersion

**Type:** Modeling or numerical concern; behavior confirmed, effect on estimates requires validation.

- **Problem:** Every household receives the mean shock multiplier, `exp(0.5 * residual_sd^2)`, instead of a shock draw. The estimator still targets child-human-capital standard deviations.
- **Why it matters:** This removes shock-driven variation. Other household differences still generate variation, but parameters may compensate for the missing shocks. Feeding mean states into later nonlinear decisions creates another approximation.
- **Next step:** Use household/period shock draws held fixed across parameter evaluations, or propagate the full distribution. If mean paths are intended, use targets consistent with them.
- **Code:** `Del_Boca_Prelim.jl:271–280`; standard-deviation targets: `Rev3_Data.jl:234`.

### 5. High: Shared policies restrict household-specific responses

**Type:** Modeling or numerical concern; behavior confirmed, effect on estimates requires validation.

- **Problem:** One policy uses average wages and public inputs. It varies only with child and parent human capital; household wages affect the budget cap afterward.
- **Why it matters:** This approximates individual optimization and can change investment choices and counterfactual responses.
- **Next step:** Benchmark against household-specific solutions. Add wage/public-input states or household groups if the error is material.
- **Code:** `Del_Boca_Prelim.jl:169–175`, `:232–249`.

### 6. High: MCMC summaries and uncertainty are unreliable

#### Summaries discard repeated states (confirmed bug)

- **Bug:** Estimates, intervals, effective sample sizes, and plots use `accepted_chain` instead of the full `chain`.
- **Effect:** Summaries ignore how long the chain stays at each state and apply burn-in in accepted-move units.
- **Fix:** Use the full chain for summaries, burn-in, diagnostics, and adaptive covariance. Use accepted moves only for acceptance diagnostics.
- **Scope:** Saved run notes acknowledge this problem in the Excel summaries. The separate optimizer parameter CSV is unaffected by this particular bug.
- **Code:** `Julia_adaptive_metropolis.jl:370`, `:436`, `:791`, `:945`.

#### Uncertainty scale is unvalidated (inferential concern)

- **Problem:** The weighting matrix is trace-normalized, loss is divided by the number of moments, and acceptance uses `exp(-loss/2)`.
- **Why it matters:** Rescaling the loss leaves its minimizer unchanged but changes the sampled distribution. Its spread is not automatically a valid standard error or calibrated posterior interval.
- **Next step:** Decide whether MCMC serves optimization or inference, then validate the scale and uncertainty for that purpose.
- **Code:** `Julia_DelBoca_SMM.jl:74–75`; `Del_Boca_Prelim.jl:408–411`; `Julia_adaptive_metropolis.jl:577`.

### 7. High: Fresh estimates do not flow reliably into Julia

#### Required mixture exports are missing (confirmed bug)

- **Bug:** R writes `trueFM.R` and the technology bridge, but repository code does not export the four mixture CSVs Julia requires.
- **Effect:** Julia fails with a fresh export directory or uses the preserved exports in its default directory.
- **Fix:** Export and validate the bridge, probabilities, means, and two covariance CSVs together. Record which run produced them.
- **Code:** `Our_Estimate_FactorModel.R:74`; `Our_Production_Functions_No4_Incomeinst.r:69`; `Rev3_Data.jl:391–399`.

#### Bootstrap switch is ignored in production estimation (confirmed bug; medium severity)

- **Bug:** `IEHC_RUN_BOOTSTRAP=0` skips the factor bootstrap, but production estimation still loads `allbootFM.R` and builds bootstrap tables.
- **Effect:** A fresh run fails; an old output directory can supply stale estimates.
- **Fix:** Use the same switch for production bootstrapping and its tables.
- **Code:** `Master File.r:80`; `Our_Production_Functions_No4_Incomeinst.r:303`, `:376`, `:458`.

### 8. Medium: Technology estimates hit fitting bounds

**Type:** Modeling or numerical concern; behavior confirmed, effect on estimates requires validation.

- **Problem:** All three `theta_h` and `rho` estimates equal the upper bound `0.99`; period-2 `mu` equals the lower bound `-5`.
- **Why it matters:** Results may depend on the chosen bounds. This alone does not prove a bug or convergence failure.
- **Next step:** Check convergence, identification, objective profiles, and sensitivity to defensible alternative bounds.
- **Evidence:** `VDE DelBoca Export/attanasio_rev3_bridge.csv:2–4`; bounds: `Our_Production_Functions_No4_Incomeinst.r:129–131` and equivalent period routines.

## Other confirmed code issues

### 9. Medium: Checkpoints do not restore the full estimator state

- **Bug:** Checkpoints omit the random generator state, households, household counts, stage flag, and histories. Restored counters also count the initial state as an iteration/acceptance.
- **Effect:** A resumed run can compare losses from different samples and cannot reproduce uninterrupted execution.
- **Fix:** Restore the complete state and exact counters, validate inputs, and recompute the current loss if the sample changes.
- **Code:** `Julia_adaptive_metropolis.jl:819–931`.

### 10. Medium: Changing the household sample leaves an old loss in memory

- **Bug:** Increasing the household count replaces the simulation sample without recalculating the current parameter vector's loss.
- **Effect:** The next acceptance decision compares losses from different samples.
- **Fix:** Recompute the current loss immediately after changing households; define when sample adaptation stops.
- **Scope:** The saved substantive runner uses a fixed household count, so this path does not affect it.
- **Code:** `Julia_adaptive_metropolis.jl:418–426`, `:577`, `:612–615`.

### 11. High on custom-proposal paths: MCMC acceptance misses a correction

- **Bug:** Acceptance uses only the loss difference, even for asymmetric lognormal, truncated-normal, or beta proposals.
- **Effect:** These configurations generally sample the wrong target distribution.
- **Fix:** Include the reverse/forward proposal-density ratio, or use symmetric proposals with out-of-bounds rejection.
- **Scope:** The standalone entrypoint configures custom proposals; the saved substantive runner does not. This defect is not established for that saved run.
- **Code:** `Julia_adaptive_metropolis.jl:577`, `:1273–1291`; custom overrides also apply under the adaptive strategy.

## Smaller issues

- **Budget floors:** Investment and consumption can total `2e-8` when resources are zero. Enforce feasibility. This edge case was reproduced, but not established in the saved run. (`Del_Boca_Prelim.jl:250–251`.)
- **Solver documentation:** The description says continuation uses true technology; code passes perceived production. Align the description with the intended model. (`new_CES_solver.jl:420–422`.)
- **Condition number:** An auxiliary diagnostic divides by the wrong eigenvalue and returns `1` for positive-definite covariance matrices. Use the smallest relevant eigenvalue and handle singularity. (`Julia_adaptive_metropolis.jl:1154`.)
- **Guide:** Remove the unfinished placeholder and paragraph; describe CEX as an observed input and document complete-case filtering. (`Attanasio_Guide.md`.)
- **R reproducibility:** No dependency lockfile or `set.seed` was found. Two bootstrap samples are only enough for a smoke run.
- **Relative paths:** Normalize input/output overrides before `setwd`; otherwise a path such as `data/input.csv` can become `data/data/input.csv`. (`Master File.r`.)
- **Output location:** Write `y_summary.csv` and `EMstart1.Rdata` to the output directory, not the input directory. (`OurR_MeasurementSpec_No4.r:302`, `:393`.)
