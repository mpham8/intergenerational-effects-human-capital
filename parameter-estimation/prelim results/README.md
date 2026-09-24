# Preliminary Results

`Del_Boca_Prelim_results_20260915_095148/` contains the first exploratory Del Boca run: optimizer estimates, empirical target moments, the saved Metropolis chain, an Excel summary, and run notes. `VDE DelBoca Export/` contains the Attanasio bridge and four mixture CSVs used as inputs. These exports are also retained in `Attanasio Replication/VDE DelBoca Export/` for the scripts' default input path.

The implementation is `Del_Boca_Replication/Del_Boca_Prelim.jl`, formerly `Julia_DelBoca_Implementation_Rev3_Tau_Workaround.jl`. From `parameter-estimation/`, run its tests with `julia --project=. --threads=auto Del_Boca_Replication/Rev3_Tau_Workaround_Smoke_Tests.jl`. The corresponding full-run entrypoint is `Del_Boca_Replication/Rev3_Tau_Workaround_Substantive_Run.jl`; it writes a new timestamped output directory rather than overwriting these preserved results.

These are preliminary results. The tau workaround rejects entire synthetic household draws with a time share above one in any period, changing the joint distribution without re-estimating the Attanasio bridge. The Excel Metropolis summaries use accepted proposals only; correct chain summaries must retain repeated states after rejections. See `Del_Boca_Prelim_results_SUBSTANTIVE_RUN.txt` in the results folder for details. File renaming did not rerun or change the estimates.
