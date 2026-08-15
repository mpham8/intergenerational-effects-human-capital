using CSV
using DataFrames
using Dates

include(joinpath(@__DIR__, "Julia_adaptive_metropolis.jl"))

const DRY_RUN_SEED = 20260725
const DRY_RUN_STAGE_EVALUATIONS = 24
const DRY_RUN_POPULATION = 8
const DRY_RUN_WEIGHTING_DRAWS = 4
const DRY_RUN_MCMC_ITERATIONS = 50

function executable_on_path(names::Vector{String})
    for name in names
        path = Sys.which(name)
        path === nothing || return path
    end
    return nothing
end

function dry_run_output_dir()
    configured = get(ENV, "IEHC_DRY_RUN_OUTPUT_DIR", "")
    isempty(configured) || return abspath(configured)
    stamp = Dates.format(now(), "yyyymmdd_HHMMSS")
    return joinpath(@__DIR__, "dry_run_output", stamp)
end

function run_attanasio_dry_stage(output_dir::String)
    rscript = executable_on_path(Sys.iswindows() ? ["Rscript.exe", "Rscript"] : ["Rscript"])
    rscript === nothing && error(
        "Rscript is not available. Install R and the packages listed in Master File.r.",
    )
    required_packages = [
        "R.utils", "ks", "gdata", "corpcor", "minpack.lm", "Matrix", "foreign",
        "scales", "openxlsx",
    ]
    package_expression = """
    missing <- setdiff(c($(join(repr.(required_packages), ", "))), rownames(installed.packages()))
    if (length(missing)) stop(paste("Missing R packages:", paste(missing, collapse=", ")))
    """
    run(`$rscript -e $package_expression`)

    attanasio_dir = normpath(joinpath(@__DIR__, "..", "Attanasio Replication"))
    master_file = joinpath(attanasio_dir, "Master File.r")
    run_output = joinpath(output_dir, "attanasio")
    mkpath(run_output)
    dry_ids = get(ENV, "IEHC_DRY_RUN_IDS", "1000")
    withenv(
        "IEHC_NLSY_DATA_FILE" => nlsy_data_file(),
        "IEHC_ATTANASIO_SCRIPT_DIR" => attanasio_dir,
        "IEHC_ATTANASIO_OUTPUT_DIR" => run_output,
        "IEHC_DRY_RUN_IDS" => dry_ids,
        "IEHC_BOOTSTRAP_SAMPLES" => "1",
        "IEHC_RUN_BOOTSTRAP" => "1",
    ) do
        run(`$rscript $master_file`)
    end
    return joinpath(run_output, REV3_BRIDGE_FILENAME)
end

function write_dry_run_summary(output_dir, data, result)
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
    open(joinpath(output_dir, "DRY_RUN_ONLY.txt"), "w") do io
        println(io, "Computational validation only; these are not substantive estimates.")
        println(io, "Input: ", data.filepath)
        println(io, "Households: ", data.n_households)
        println(io, "Qualifying CEX observations by period: ", data.qualifying_cex)
        println(io, "Minimum CEX coverage: ", data.cex_minimum_coverage)
        println(io, "Julia threads: ", Threads.nthreads())
        println(io, "Stage-2 objective: ", result["stage2_optimizer"].objective)
    end
end

function main()
    input_file = nlsy_data_file()
    isfile(input_file) || error("IEHC_NLSY_DATA_FILE does not exist: $input_file")
    output_dir = dry_run_output_dir()
    mkpath(output_dir)

    println("DRY RUN ONLY: outputs are computational checks, not final estimates.")
    println("Input: $input_file")
    println("Output: $output_dir")
    println("Julia threads: $(Threads.nthreads())")

    bridge_file = run_attanasio_dry_stage(output_dir)
    load_and_set_true_technology!(bridge_file)
    dry_ids = parse(Int, get(ENV, "IEHC_DRY_RUN_IDS", "1000"))
    data = load_rev3_data(input_file; maximum_households=dry_ids)

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
            save_prefix="rev3_dry_run",
        )
    end
    write_dry_run_summary(output_dir, data, result)
    println("Dry run completed successfully: $output_dir")
    return output_dir
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end
