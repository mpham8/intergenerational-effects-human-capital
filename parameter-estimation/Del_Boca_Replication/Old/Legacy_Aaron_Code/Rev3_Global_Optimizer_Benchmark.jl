using BlackBoxOptim
using CSV
using DataFrames
using Optim
using Random
using Statistics

include(joinpath(@__DIR__, "Julia_DelBoca_Implementation_Rev3.jl"))

const BENCHMARK_DATA_FILE = get(
    ENV,
    "REV3_BENCHMARK_DATA_FILE",
    "/Users/joshualeinwand/Desktop/Summer/Expenditure Per Student/outputs/Fake_Merged_Data_with_cex_education_expenditure.csv",
)
const BENCHMARK_OUTPUT_DIR = get(
    ENV,
    "REV3_BENCHMARK_OUTPUT_DIR",
    joinpath(@__DIR__, "optimizer_benchmark_rev3_15min"),
)
const BENCHMARK_LOWER_BOUNDS = copy(DXNES_LOWER_BOUNDS)
const BENCHMARK_UPPER_BOUNDS = copy(DXNES_UPPER_BOUNDS)
const BENCHMARK_INITIAL_GUESS = copy(parameters_to_optimize)
const BENCHMARK_SECONDS_PER_OPTIMIZER = parse(
    Float64, get(ENV, "REV3_BENCHMARK_SECONDS", "900"),
)
const BENCHMARK_SEED = 20260714

mutable struct BenchmarkTrace
    optimizer::String
    started_at::Float64
    lock::ReentrantLock
    elapsed_seconds::Vector{Float64}
    mse::Vector{Float64}
    best_mse::Vector{Float64}
end

BenchmarkTrace(name::String) = BenchmarkTrace(
    name, time(), ReentrantLock(), Float64[], Float64[], Float64[],
)

function load_rev3_benchmark_data(filepath::String=BENCHMARK_DATA_FILE)
    return load_rev3_data(filepath)
end

function record_evaluation!(trace::BenchmarkTrace, mse::Float64)
    lock(trace.lock) do
        push!(trace.elapsed_seconds, time() - trace.started_at)
        push!(trace.mse, mse)
        previous_best = isempty(trace.best_mse) ? Inf : trace.best_mse[end]
        push!(trace.best_mse, min(previous_best, mse))
    end
    return mse
end

function moment_mse(
    parameter_values,
    data,
    trace::Union{BenchmarkTrace, Nothing}=nothing;
    household_workers::Int=0,
)::Float64
    parameters = Float64.(collect(parameter_values))
    if any(parameters .< BENCHMARK_LOWER_BOUNDS) || any(parameters .> BENCHMARK_UPPER_BOUNDS)
        mse = 1e6
    else
        try
            # Match the current Rev3 stage-one SMM criterion: moment errors are
            # standardized before their mean square is computed. Stage two is
            # intentionally excluded because its bootstrap weights depend on
            # the first-stage estimate and would differ across optimizers.
            mse = rev3_smm_objective(
                parameters,
                data.empirical_moments,
                data.households;
                standardize_moments=true,
                household_workers=household_workers,
            )
            isfinite(mse) || (mse = 1e6)
        catch error
            @warn "Objective evaluation failed" parameters exception=(error, catch_backtrace())
            mse = 1e6
        end
    end
    return trace === nothing ? mse : record_evaluation!(trace, mse)
end

function result_rows(trace::BenchmarkTrace)::DataFrame
    order = sortperm(trace.elapsed_seconds)
    mse = trace.mse[order]
    return DataFrame(
        optimizer=fill(trace.optimizer, length(order)),
        evaluation=collect(1:length(order)),
        elapsed_seconds=trace.elapsed_seconds[order],
        mse=mse,
        best_mse=accumulate(min, mse),
    )
end

function run_optim_samin(data; seed::Int=BENCHMARK_SEED)
    Random.seed!(seed)
    trace = BenchmarkTrace("Optim.jl SAMIN")
    objective = x -> moment_mse(x, data, trace; household_workers=0)
    result = Optim.optimize(
        objective,
        BENCHMARK_LOWER_BOUNDS,
        BENCHMARK_UPPER_BOUNDS,
        BENCHMARK_INITIAL_GUESS,
        Optim.SAMIN(verbosity=0),
        Optim.Options(
            iterations=1_000_000_000,
            time_limit=BENCHMARK_SECONDS_PER_OPTIMIZER,
            x_abstol=0.0,
            f_abstol=0.0,
            show_trace=false,
        ),
    )
    return trace, Optim.minimizer(result), Optim.minimum(result)
end

function run_optim_particle_swarm(data; seed::Int=BENCHMARK_SEED)
    Random.seed!(seed)
    trace = BenchmarkTrace("Optim.jl Particle Swarm")
    objective = x -> moment_mse(x, data, trace; household_workers=0)
    swarm_size = 12
    result = Optim.optimize(
        objective,
        BENCHMARK_INITIAL_GUESS,
        Optim.ParticleSwarm(BENCHMARK_LOWER_BOUNDS, BENCHMARK_UPPER_BOUNDS, swarm_size),
        Optim.Options(
            iterations=1_000_000_000,
            time_limit=BENCHMARK_SECONDS_PER_OPTIMIZER,
            show_trace=false,
        ),
    )
    return trace, Optim.minimizer(result), Optim.minimum(result)
end

function run_blackbox(
    data,
    method::Symbol,
    label::String;
    threaded::Bool=false,
    seed::Int=BENCHMARK_SEED,
)
    Random.seed!(seed)
    trace = BenchmarkTrace(label)
    household_workers = threaded ? 1 : 0
    objective = x -> moment_mse(x, data, trace; household_workers=household_workers)
    search_range = collect(zip(BENCHMARK_LOWER_BOUNDS, BENCHMARK_UPPER_BOUNDS))
    options = (
        Method=method,
        SearchRange=search_range,
        NumDimensions=length(BENCHMARK_INITIAL_GUESS),
        PopulationSize=12,
        MaxTime=BENCHMARK_SECONDS_PER_OPTIMIZER,
        TraceMode=:silent,
    )
    result = if threaded
        # BlackBoxOptim reserves one Julia thread to coordinate the evaluator.
        BlackBoxOptim.bboptimize(objective; options..., NThreads=max(1, Threads.nthreads() - 1))
    else
        BlackBoxOptim.bboptimize(objective; options...)
    end
    return trace, BlackBoxOptim.best_candidate(result), BlackBoxOptim.best_fitness(result)
end

function run_finalist_stability(seeds::Vector{Int})
    mkpath(BENCHMARK_OUTPUT_DIR)
    data = load_rev3_benchmark_data()
    moment_mse(BENCHMARK_INITIAL_GUESS, data; household_workers=0)
    stability = DataFrame(
        optimizer=String[], seed=Int[], best_mse=Float64[],
        elapsed_seconds=Float64[], evaluations=Int[],
    )
    for seed in seeds
        println("Stability seed $seed: Optim.jl Particle Swarm")
        particle_trace, _, particle_mse = run_optim_particle_swarm(data; seed=seed)
        push!(stability, (
            "Optim.jl Particle Swarm", seed,
            min(particle_mse, minimum(particle_trace.best_mse)),
            maximum(particle_trace.elapsed_seconds), length(particle_trace.mse),
        ))

        println("Stability seed $seed: BlackBoxOptim DX-NES (threaded)")
        dxnes_trace, _, dxnes_mse = run_blackbox(
            data,
            :dxnes,
            "BlackBoxOptim DX-NES (threaded)";
            threaded=true,
            seed=seed,
        )
        push!(stability, (
            "BlackBoxOptim DX-NES (threaded)", seed,
            min(dxnes_mse, minimum(dxnes_trace.best_mse)),
            maximum(dxnes_trace.elapsed_seconds), length(dxnes_trace.mse),
        ))
    end
    CSV.write(joinpath(BENCHMARK_OUTPUT_DIR, "finalist_stability.csv"), stability)
    return stability
end

function run_optimizer_benchmark()
    mkpath(BENCHMARK_OUTPUT_DIR)
    Random.seed!(BENCHMARK_SEED)
    load_and_set_true_technology!()
    data = load_rev3_benchmark_data()
    println("Loaded $(data.n_households) households from $BENCHMARK_DATA_FILE")
    println("Julia threads available: $(Threads.nthreads())")
    println("Time budget per optimizer: $(BENCHMARK_SECONDS_PER_OPTIMIZER) seconds")
    println("Empirical moments: $(data.empirical_moments)")

    # Compile the full objective before timing so the first optimizer does not
    # absorb Julia's one-time compilation cost.
    warmup_mse = moment_mse(BENCHMARK_INITIAL_GUESS, data; household_workers=0)
    println("Warm-up MSE: $warmup_mse")

    benchmark_runs = [
        ("Optim.jl SAMIN", () -> run_optim_samin(data)),
        ("Optim.jl Particle Swarm", () -> run_optim_particle_swarm(data)),
        ("BlackBoxOptim Adaptive DE", () -> run_blackbox(
            data, :adaptive_de_rand_1_bin_radiuslimited, "BlackBoxOptim Adaptive DE",
        )),
        ("BlackBoxOptim DX-NES (threaded)", () -> run_blackbox(
            data, :dxnes, "BlackBoxOptim DX-NES (threaded)"; threaded=true,
        )),
    ]

    curves = DataFrame()
    summary = DataFrame(
        optimizer=String[], best_mse=Float64[], elapsed_seconds=Float64[],
        evaluations=Int[], Delta=Float64[], perceived_rho=Float64[],
        perceived_theta_h_1=Float64[], perceived_theta_h_2=Float64[],
        perceived_theta_h_3=Float64[],
    )

    for (name, run_optimizer) in benchmark_runs
        println("\nStarting $name")
        trace, best_parameters, best_mse = run_optimizer()
        trace_rows = result_rows(trace)
        append!(curves, trace_rows; cols=:union)
        push!(summary, (
            name,
            min(best_mse, minimum(trace.best_mse)),
            maximum(trace.elapsed_seconds),
            length(trace.mse),
            Float64(best_parameters[1]), Float64(best_parameters[2]),
            Float64(best_parameters[3]), Float64(best_parameters[4]),
            Float64(best_parameters[5]),
        ))
        println("$name completed: best MSE=$(summary.best_mse[end]), evaluations=$(length(trace.mse))")

        # Persist each completed run so a later interruption cannot discard the
        # preceding 15-minute optimizer results.
        CSV.write(joinpath(BENCHMARK_OUTPUT_DIR, "optimizer_mse_over_time.csv"), curves)
        CSV.write(joinpath(BENCHMARK_OUTPUT_DIR, "optimizer_summary.csv"), summary)
    end

    sort!(summary, :best_mse)
    CSV.write(joinpath(BENCHMARK_OUTPUT_DIR, "optimizer_mse_over_time.csv"), curves)
    CSV.write(joinpath(BENCHMARK_OUTPUT_DIR, "optimizer_summary.csv"), summary)
    CSV.write(joinpath(BENCHMARK_OUTPUT_DIR, "empirical_moments.csv"), DataFrame(
        moment=1:length(data.empirical_moments), value=data.empirical_moments,
    ))
    println("\nBenchmark ranking:")
    show(stdout, "text/plain", summary)
    println()
    return summary, curves
end

if abspath(PROGRAM_FILE) == @__FILE__
    run_optimizer_benchmark()
end
