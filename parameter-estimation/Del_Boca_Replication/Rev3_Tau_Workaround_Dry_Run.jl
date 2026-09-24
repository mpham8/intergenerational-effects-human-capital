using CSV
using DataFrames
using Dates

include(joinpath(@__DIR__, "Del_Boca_Prelim.jl"))
include(joinpath(@__DIR__, "Julia_adaptive_metropolis.jl"))

const DRY_RUN_SEED = parse(Int, get(ENV, "IEHC_DRY_RUN_SEED", "20260725"))
const DRY_RUN_STAGE_EVALUATIONS =
    parse(Int, get(ENV, "IEHC_DRY_RUN_STAGE_EVALUATIONS", "24"))
const DRY_RUN_POPULATION = parse(Int, get(ENV, "IEHC_DRY_RUN_POPULATION", "8"))
const DRY_RUN_WEIGHTING_DRAWS =
    parse(Int, get(ENV, "IEHC_DRY_RUN_WEIGHTING_DRAWS", "4"))
const DRY_RUN_MCMC_ITERATIONS =
    parse(Int, get(ENV, "IEHC_DRY_RUN_MCMC_ITERATIONS", "50"))

function dry_run_output_dir()
    configured = get(ENV, "IEHC_TAU_WORKAROUND_DRY_RUN_OUTPUT_DIR", "")
    isempty(configured) || return abspath(configured)
    stamp = Dates.format(now(), "yyyymmdd_HHMMSS")
    return joinpath(@__DIR__, "tau_workaround_dry_run_output", stamp)
end

function write_dry_run_summary(output_dir, data, result)
    summary = DataFrame(
        parameter=PARAMETER_NAMES,
        stage1_value=result["stage1_optimizer"].parameters,
        stage2_value=result["stage2_optimizer"].parameters,
    )
    CSV.write(joinpath(output_dir, "rev3_tau_workaround_parameter_summary.csv"), summary)
    CSV.write(
        joinpath(output_dir, "rev3_tau_workaround_empirical_moments.csv"),
        DataFrame(moment=1:NUM_MOMENTS, value=data.empirical_moments),
    )
    open(joinpath(output_dir, "DRY_RUN_ONLY.txt"), "w") do io
        println(io, "Computational validation of the Rev3 tau workaround only.")
        println(io, "These are not substantive estimates.")
        println(io, "Input: ", data.filepath)
        println(io, "Households: ", data.n_households)
        println(io, "Mixture component counts: ", data.component_counts)
        println(io, "Mixture proposals: ", data.proposals)
        println(io, "Rejected proposals: ", data.rejected_proposals)
        println(io, "Acceptance rate: ", data.acceptance_rate)
        println(io, "Synthetic household seed: ", data.seed)
        println(io, "Julia threads: ", Threads.nthreads())
        println(io, "Stage-2 objective: ", result["stage2_optimizer"].objective)
    end
end

function main()
    input_dir = attanasio_output_dir()
    isdir(input_dir) || error("IEHC_ATTANASIO_OUTPUT_DIR does not exist: $input_dir")
    output_dir = dry_run_output_dir()
    mkpath(output_dir)

    println("REV3 TAU-WORKAROUND DRY RUN ONLY")
    println("Outputs are computational checks, not final estimates.")
    println("Attanasio export: $input_dir")
    println("Output: $output_dir")
    println("Julia threads: $(Threads.nthreads())")

    load_and_set_true_technology!(joinpath(input_dir, REV3_BRIDGE_FILENAME))
    dry_ids = parse(Int, get(ENV, "IEHC_DRY_RUN_IDS", "1000"))
    data = load_rev3_tau_workaround_data(
        input_dir; maximum_households=dry_ids, seed=DRY_RUN_SEED,
    )

    estimator = SMMAdaptiveMetropolis(
        initial_params=copy(parameters),
        empirical_moments=data.empirical_moments,
        households=data.households,
        initial_households=data.n_households,
        max_households=data.n_households,
        household_increase_threshold=0.0,
        adaptation_start_time=20,
        random_seed=DRY_RUN_SEED,
    )

    result = cd(output_dir) do
        run_two_step_smm!(
            estimator;
            stage1_iterations=0,
            stage2_iterations=DRY_RUN_MCMC_ITERATIONS,
            stage1_dxnes_evaluations=DRY_RUN_STAGE_EVALUATIONS,
            stage2_dxnes_evaluations=DRY_RUN_STAGE_EVALUATIONS,
            dxnes_population_size=DRY_RUN_POPULATION,
            weighting_draws=DRY_RUN_WEIGHTING_DRAWS,
            optimizer_threads=max(1, Threads.nthreads() - 1),
            save_every=DRY_RUN_MCMC_ITERATIONS,
            save_prefix="rev3_tau_workaround_dry_run",
        )
    end
    write_dry_run_summary(output_dir, data, result)
    println("Tau-workaround dry run completed successfully: $output_dir")
    return output_dir
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end
