# Repository review

## Implementation status for finding 1 (November 3, 2026)

- **1 — fixed:** `Our_estim_meas_model.R` now builds an explicit list of observed measurement indices for each household using `lapply`. The two E-step covariance slices use `drop=FALSE`, preserving matrix dimensions when only one measurement is observed. This restores the intended indexing without changing the statistical model or export interfaces.

Validation: Focused R regression checks passed for complete data, mixed missingness including a single observed measurement, and equal observation counts with different missing columns. Checks compared E-step probabilities and log likelihood, M-step means, covariances and weights, and the next E-step against reference calculations. The original `apply` simplification was also reproduced. These temporary checks are separate from the checked-in suite. Full estimation has not been rerun; saved estimates have not been regenerated with this fix.

## Implementation status for findings 4–11 (October 20, 2026)

The findings below describe the reviewed baseline. The current implementation addresses them as follows:

Current computational follow-up: the R master seeds production via `IEHC_R_SEED`
and records session versions; the measurement script preserves launch-resolved
paths and writes its summary/initialization to the output directory. Relative and
absolute paths with spaces and seed replay passed. Symmetric covariance diagnostics
use absolute eigenvalues and relative singularity tolerance; a preexisting returned
matrix scope error was also corrected. Solver continuation documentation now matches
perceived production. These changes do not calibrate inference or lock R dependencies.

- **4 — fixed:** Realized production paths use seeded household/period normal draws, transformed with the exported residual standard deviations. Draws stay fixed across parameter evaluations and thread counts; later choices condition on realized child human capital. Policy solution still integrates uncertainty by quadrature.
- **5 — fixed in the default estimator:** Households optimize at their own income-capacity and public-input trajectories in both SMM stages, weighting simulations, and Metropolis. Constant parent education is evaluated at the household's own grid point. `IEHC_PARENT_POLICY_MODE=shared` is an explicit opt-in approximation. The benchmark compares modes using the same child grid and shocks; full-sample runtime and approximation error remain unmeasured.
- **6 — bookkeeping fixed; full-pipeline bootstrap implemented:** Summaries, burn-in, ESS, plots and adaptive covariance use the full chain, including rejected states. Chain spread remains descriptive. A separate cluster bootstrap reestimates Attanasio, the involvement calibration, and both Del Boca SMM stages, so first-stage sampling uncertainty can enter the replicate spread. Full-data inference and coverage remain unvalidated.
- **7 — fixed:** Production estimation exports the technology bridge, four mixture CSVs, and involvement calibration with a schema-4 manifest and SHA-256 checksums. Julia rejects interrupted, inconsistent, and older bundles. Both settings of `IEHC_RUN_BOOTSTRAP` were tested: disabled writes only point-estimate tables; enabled produced two complete fake-data resamples and uncertainty tables. The exporter requires the R package `digest`.
- **8 — numerical fixes and diagnostics implemented and tested:** All six production fits use a stable log-domain CES and select the lowest residual sum of squares among the converged fits from four starting points. Main fits at a bound generate an objective profile and one-sided bound sensitivity check. Production bounds stay unchanged; the alternative estimates are not exported to Del Boca. The historical boundary estimates have not been reassessed.
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

**2a implemented and tested.** Expenditures, family income and maternal wage earnings use annual real 2026 dollars. Resources are fixed family income plus maternal earnings at chosen labor. R uses the mother's own earnings when reliable; otherwise it predicts an earnings rate by age period and maternal education and constrains the prediction to the reported budget when necessary. At zero hours all family income is fixed. The rule reproduces observed family income at observed hours, keeps zero-hour households, and records inconsistent reports and binding predictions rather than silently clipping raw earnings. The 2,080-hour denominator, capped labor share, and child production specification are unchanged.

Schema 4 carries the jointly fitted earnings rate and fixed-income inputs; it rejects older bundles. The synthetic solver uses both inputs in decisions and realized spending. The full-pipeline bootstrap refits the wage rule in every cluster resample. On the unchanged fake input, all 2,641 complete child panels were retained, including 874 zero-hour records. Attanasio then completed and its export ran both reduced Del Boca stages and a short chain. R checks, 95 Julia assertions, and a synthetic bootstrap reference plus two resamples passed. Wage comparability, prediction sensitivity and substantive estimation on the restricted data remain open validation work.

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

**Current implementation: preliminary activity-index model.** The computational 3a correction is retained: fitting and export reconstruct the same means. Reading anchors periods 1-2; performance participation has a shared estimated loading and intercept in periods 2-3, and help learning numbers supplies the third earliest-period indicator. Cross-covariances with the period-3 proxy are divided by its fitted loading. Both main and bootstrap paths use these restrictions.

The preliminary 3b path exports a pooled mean-ratio kappa alongside the jointly fitted budget inputs in schema 4. Del Boca draws the unfiltered joint population; column 6 is educational involvement. Perceived and true production use kappa times physical nonwork, while utility and the budget retain physical labor. The old workaround filename is a compatibility wrapper, not a rejection sampler. Necessary moment-feasibility tests reject incompatible calibrations without changing the targets. Unit kappa is explicitly test-only unless selected as a sensitivity override. This is a moment-matching approximation, not a claim that a bounded structural index reproduces an unbounded distribution exactly.

The fake data now completes Attanasio and reduced Del Boca under the updated schema-4 budget. The 28-variable joint distribution and calibration require new VDE disclosure review. Historical substantive results remain unchanged. Indicator quality and identification are empirical validation questions, not established by the computational tests.

The findings below document the original reviewed specification, not the current activity-index implementation.

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
- **Fix and verification:** Household-specific solving is now the default. Synthetic regression checks cover future-input effects on current choices, direct policy agreement, budget equality, threaded reproducibility, and shared-mode opt-in. Use the household mode as the reference before adopting any faster approximation. Substantive comparisons remain unmeasured.
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
- **Fix:** Keep the chain descriptive and use `Rev3_Full_Pipeline_Bootstrap.jl` for sampling uncertainty. It resamples the original panel by a specified cluster, optionally within strata, then rebuilds the complete estimator. No replicate is silently replaced. The runner records failures and withholds ordinary percentile limits if a fit fails, the samples are too few, a fit hits a bound, or a local SMM moment rank check fails. Percentile limits are opt-in and conditional on the input CEX imputations and fixed simulation draws. The pairs cluster design requires an appropriate unit of independent sampling; it is not a design-based survey-weighted estimator. [Cluster bootstrap reference](https://doi.org/10.1016/j.jeconom.2022.04.001); [why boundaries matter for the bootstrap](https://doi.org/10.1111/1468-0262.00114).
- **Code:** `Julia_DelBoca_SMM.jl:74–75`; `Del_Boca_Prelim.jl:408–411`; `Julia_adaptive_metropolis.jl:577`.

Verification: the actual pipeline completed a reference fit and two resamples
on a synthetic observed panel with 210 family clusters in two strata. Each fit
recomputed kappa and both SMM stages. The summary recorded both replications
and did not emit interval limits. The test was rerun successfully under the new
2a rule, including reestimation of the wage prediction. This verifies the
implementation, not coverage or full-run cost.

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
- **Implementation and verification:** The fit uses the exact Cobb-Douglas limit at zero and avoids evaluating large powers directly. Saved diagnostics report each start, boundary side, and raw/column-scaled Jacobian condition numbers. At each active bound, the profile fixes the parameter at successive values and refits the others. Sensitivity relaxes one bound at a time for diagnosis only. A synthetic run with the real six fits recovered known interior technology and correctly flagged a deliberately constrained parameter; the sensitivity fit improved RSS without changing the exported bound. This is not an assessment of the historical VDE estimates. Restricted-data identification and bound sensitivity still require a fresh fit.
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

- **Budget floors: fixed and tested.** The earlier implementation could produce positive spending with zero resources. A fresh zero-resource test verifies zero realized consumption and investment even when the policy wants to invest.
- **Solver documentation: fixed.** Continuation is described as perceived, realized production as true; both use the activity conversion.
- **Condition number:** An auxiliary diagnostic divides by the wrong eigenvalue and returns `1` for positive-definite covariance matrices. Use the smallest relevant eigenvalue and handle singularity. (`Julia_adaptive_metropolis.jl:1154`.)
- **Guide: updated.** Lists all supporting sources, the activity anchors, observed CEX, the schema-4 budget handoff, and remaining validation limitations.
- **R reproducibility: partial.** Configurable production seed and session versions are recorded and seed replay is tested. No dependency lockfile. Two bootstrap samples are only enough for a smoke run.
- **Relative paths: fixed and tested.** The master resolves paths before setwd; measurement preparation retains them. Tests include relative/absolute paths and spaces.
- **Output location: fixed and tested.** `y_summary.csv` and `EMstart1.Rdata` go to the configured output directory.
