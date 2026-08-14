using CSV
using DataFrames
using Dates

include(joinpath(@__DIR__, "Julia_adaptive_metropolis.jl"))

const SUBSTANTIVE_SEED = parse(Int, get(ENV, "IEHC_SUBSTANTIVE_SEED", "20260728"))
const SUBSTANTIVE_STAGE_EVALUATIONS =
    parse(Int, get(ENV, "IEHC_SUBSTANTIVE_STAGE_EVALUATIONS", string(DXNES_MAX_EVALUATIONS)))
const SUBSTANTIVE_POPULATION =
    parse(Int, get(ENV, "IEHC_SUBSTANTIVE_POPULATION", string(DXNES_POPULATION_SIZE)))
const SUBSTANTIVE_WEIGHTING_DRAWS =
    parse(Int, get(ENV, "IEHC_SUBSTANTIVE_WEIGHTING_DRAWS", string(SMM_WEIGHTING_DRAWS)))
const SUBSTANTIVE_MCMC_ITERATIONS =
    parse(Int, get(ENV, "IEHC_SUBSTANTIVE_MCMC_ITERATIONS", "10000"))
const SUBSTANTIVE_BOOTSTRAP_SAMPLES =
    parse(Int, get(ENV, "IEHC_SUBSTANTIVE_BOOTSTRAP_SAMPLES", "2"))

function substantive_output_dir()
    configured = get(ENV, "IEHC_SUBSTANTIVE_OUTPUT_DIR", "")
    isempty(configured) || return abspath(configured)
    stamp = Dates.format(now(), "yyyymmdd_HHMMSS")
    return joinpath(@__DIR__, "substantive_output", stamp)
end

function executable_on_path(names::Vector{String})
    for name in names
        path = Sys.which(name)
        path === nothing || return path
    end
    return nothing
end

function run_attanasio_substantive_stage(output_dir::String)
    rscript = executable_on_path(Sys.iswindows() ? ["Rscript.exe", "Rscript"] : ["Rscript"])
    rscript === nothing && error("Rscript is not available.")
    attanasio_dir = normpath(joinpath(@__DIR__, "..", "Attanasio Replication"))
    run_output = joinpath(output_dir, "attanasio")
    mkpath(run_output)

    # A zero dry-run limit makes Attanasio use every balanced household. The
    # bootstrap count remains configurable for replication on the Windows VDE.
    withenv(
        "IEHC_NLSY_DATA_FILE" => nlsy_data_file(),
        "IEHC_ATTANASIO_SCRIPT_DIR" => attanasio_dir,
        "IEHC_ATTANASIO_OUTPUT_DIR" => run_output,
        "IEHC_DRY_RUN_IDS" => "0",
        "IEHC_BOOTSTRAP_SAMPLES" => string(SUBSTANTIVE_BOOTSTRAP_SAMPLES),
        "IEHC_RUN_BOOTSTRAP" => "1",
    ) do
        run(`$rscript $(joinpath(attanasio_dir, "Master File.r"))`)
    end
    return joinpath(run_output, REV3_BRIDGE_FILENAME)
end

function write_substantive_summary(output_dir, data, result, elapsed_seconds)
    summary = DataFrame(
        parameter=PARAMETER_NAMES,
        stage1_value=result["stage1_optimizer"].parameters,
        stage2_value=result["stage2_optimizer"].parameters,
    )
    CSV.write(joinpath(output_dir, "rev3_parameter_summary.csv"), summary)
    CSV.write(
        joinpath(output_dir, "rev3_empirical_moments.csv"),
        DataFrame(moment=1:NUM_MOMENTS, value=data.empirical_moments),
    )
    open(joinpath(output_dir, "SUBSTANTIVE_RUN.txt"), "w") do io
        println(io, "Full-sample substantive estimation run.")
        println(io, "Input: ", data.filepath)
        println(io, "Households: ", data.n_households)
        println(io, "Qualifying CEX observations by period: ", data.qualifying_cex)
        println(io, "Minimum CEX coverage: ", data.cex_minimum_coverage)
        println(io, "Attanasio bootstrap samples: ", SUBSTANTIVE_BOOTSTRAP_SAMPLES)
        println(io, "DX-NES evaluations per stage: ", SUBSTANTIVE_STAGE_EVALUATIONS)
        println(io, "DX-NES population: ", SUBSTANTIVE_POPULATION)
        println(io, "Weighting draws: ", SUBSTANTIVE_WEIGHTING_DRAWS)
        println(io, "Adaptive-Metropolis iterations: ", SUBSTANTIVE_MCMC_ITERATIONS)
        println(io, "Julia threads: ", Threads.nthreads())
        println(io, "Stage-2 objective: ", result["stage2_optimizer"].objective)
        println(io, "Total elapsed seconds: ", elapsed_seconds)
    end
end

function main()
    input_file = nlsy_data_file()
    isfile(input_file) || error("IEHC_NLSY_DATA_FILE does not exist: $input_file")
    output_dir = substantive_output_dir()
    mkpath(output_dir)
    start_time = time()

    println("FULL-SAMPLE SUBSTANTIVE ESTIMATION")
    println("Input: $input_file")
    println("Output: $output_dir")
    println("Julia threads: $(Threads.nthreads())")

    bridge_file = run_attanasio_substantive_stage(output_dir)
    load_and_set_true_technology!(bridge_file)
    data = load_rev3_data(input_file)

    estimator = SMMAdaptiveMetropolis(
        initial_params=copy(parameters),
        empirical_moments=data.empirical_moments,
        households=data.households,
        initial_households=data.n_households,
        max_households=data.n_households,
        household_increase_threshold=0.0,
        adaptation_start_time=100,
        random_seed=SUBSTANTIVE_SEED,
    )

    result = cd(output_dir) do
        run_two_step_smm!(
            estimator;
            stage1_iterations=0,
            stage2_iterations=SUBSTANTIVE_MCMC_ITERATIONS,
            stage1_dxnes_evaluations=SUBSTANTIVE_STAGE_EVALUATIONS,
            stage2_dxnes_evaluations=SUBSTANTIVE_STAGE_EVALUATIONS,
            dxnes_population_size=SUBSTANTIVE_POPULATION,
            weighting_draws=SUBSTANTIVE_WEIGHTING_DRAWS,
            optimizer_threads=max(1, Threads.nthreads() - 1),
            save_every=100,
            save_prefix="rev3_substantive",
        )
    end
    elapsed_seconds = time() - start_time
    write_substantive_summary(output_dir, data, result, elapsed_seconds)
    println("Substantive estimation completed successfully in $(round(elapsed_seconds / 3600; digits=2)) hours: $output_dir")
    return output_dir
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end
