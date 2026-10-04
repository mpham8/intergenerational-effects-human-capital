# Preliminary Project Code and Results

This branch contains the files used for the Attanasio and preliminary Del Boca estimation, the fully merged fake NLSY/CEX data, and the saved first-round results.

## Attanasio

In `parameter-estimation/Attanasio Replication/`, `Master File.r` runs the estimation. `OurR_MeasurementSpec_No4.r` prepares the data and defines the measurements; `Our_Estimate_FactorModel.R`, `Our_estim_meas_model.R`, and `Our_Procedures.R` estimate and simulate the latent-factor distribution; and `Our_Production_Functions_No4_Incomeinst.r` estimates production technology and writes the Del Boca bridge. `Attanasio_Guide.md` provides additional background.

`data-preprocessing/Fake Merged Data/Fake_Merged_Data_with_cex_education_expenditure.csv` is the most processed fake dataset, including CEX education expenditure. Attanasio uses it by default; `IEHC_NLSY_DATA_FILE` selects another input. New Attanasio output is separate from the preserved VDE exports.

## Del Boca

In `parameter-estimation/Del_Boca_Replication/`, `Rev3_Tau_Workaround_Substantive_Run.jl` runs the full preliminary estimation. It uses `Del_Boca_Prelim.jl` for household simulation, `Rev3_Data.jl` for the exported inputs, `Rev3_Tau_Workaround_Data.jl` for feasible time-share draws, `Julia_DelBoca_SMM.jl` for two-stage SMM, and `Julia_adaptive_metropolis.jl` for the saved chain and summaries. `dsge-simulations/new_CES_solver.jl` solves parental choices. `Project.toml` and `Manifest.toml` record the Julia environment.

## Inputs and Results

`parameter-estimation/Attanasio Replication/VDE DelBoca Export/` holds the real-data production bridge, mixture probabilities, component means, and two covariance matrices used by Del Boca. These are separate from the bundled fake data; fake-data estimates should not be mixed with these exports.

`parameter-estimation/prelim results/Del_Boca_Prelim_results_20260915_095148/` holds the parameter estimates, target moments, saved chain, Excel summary, and run notes. The run notes explain the preliminary results and their limitations. These saved results have not been changed.
