using Test, Statistics, Random, LinearAlgebra, Serialization
include(joinpath(@__DIR__, "..", "Del_Boca_Replication", "Julia_adaptive_metropolis.jl"))

@testset "Production shocks and policy approximation" begin
    set_true_technology!(test_true_technology())
    h = ones(4, 3, 7)
    h[:, :, 4:7] .= 0
    @test production_shock_draws(4) == production_shock_draws(8)[1:4, :]
    policy = solve_parent_policy(copy(parameters), h)
    n = 2000
    shocks = production_shock_draws(n)
    paths = [solve_household_from_policy(policy, h[1, :, :]; shock_draws=shocks[i, :]) for i in 1:n]
    @test std([path[1, 4] for path in paths]) > 0
    deterministic = solve_household_from_policy(policy, h[1, :, :]; shock_draws=zeros(3))[1, 4]
    @test std(log.([path[1, 4] / deterministic for path in paths])) ≈ 0.1 atol=0.006
    a = solve_households(copy(h), copy(parameters), 1)
    b = solve_households(copy(h), copy(parameters), 0)
    @test a == b
    @test a == solve_households(copy(h), copy(parameters), 1)
    @test maximum(abs.(a[:, :, 4] + a[:, :, 5] - h[:, :, 1] .* h[:, :, 2] .* (1 .- a[:, :, 6]))) < 1e-12
    benchmark = benchmark_parent_policies(h[1:2, :, :], copy(parameters))
    @test benchmark.max_choice_difference < 1e-10
    heterogeneous = copy(h[1:2, :, :])
    heterogeneous[1, :, 2] .= 0.4
    heterogeneous[2, :, 2] .= 2.0
    heterogeneous[2, :, 3] .= 1.5
    benchmark = benchmark_parent_policies(heterogeneous, copy(parameters))
    @test benchmark.max_choice_difference > 1e-4
    println("Heterogeneous policy benchmark: max choice difference = ", benchmark.max_choice_difference)
end

# Use a known objective to isolate the sampling kernel from costly household DP.
# This override is confined to the test process.
const TEST_OBJECTIVE = Ref{Function}((est, p) -> 0.0)
function compute_smm_objective(est::SMMAdaptiveMetropolis, p::Vector{Float64})
    return TEST_OBJECTIVE[](est, p)
end
function toy_estimator(; seed=14, spec=nothing, strategy="custom", adaptation=1000000, max_households=2)
    custom = spec === nothing ? Dict{String, Dict{String, Any}}() : Dict("x" => Dict{String, Any}(spec))
    return SMMAdaptiveMetropolis(parameter_names=["x"], parameter_bounds=[(0.1, 1.0)],
        initial_params=[0.5], empirical_moments=ones(18), households=ones(2, 3, 7),
        initial_households=2, max_households=max_households, random_seed=seed,
        custom_proposal_dists=custom, proposal_strategy=strategy, adaptation_start_time=adaptation)
end
function quiet_run(est, n)
    redirect_stdout(devnull) do
        run_mcmc!(est; n_iterations=n, save_every=n+1)
    end
end

@testset "Full-chain summaries and adaptation" begin
    est = toy_estimator(adaptation=2)
    est.chain = vcat([[0.2] for _ in 1:250], [[0.8] for _ in 1:50])
    est.accepted_chain = [[0.2], [0.8]]
    summary = get_parameter_estimates(est)
    @test summary["mean"][1] ≈ 0.35
    @test summary["n_samples"] == 200
    update_adaptive_covariance!(est)
    @test est.mean_params[1] ≈ 0.3
    @test haskey(compute_diagnostics(est), "effective_sample_size")
    @test !check_parameter_constraints(est, [NaN])
end

@testset "Hastings correction recovers a uniform target" begin
    TEST_OBJECTIVE[] = (est, p) -> 0.0
    specs = [Dict("type" => "lognormal", "sigma" => 0.7),
             Dict("type" => "truncated_normal", "scale" => 0.25, "range" => (0.1, 1.0)),
             Dict("type" => "beta", "alpha" => 1.2, "beta" => 1.5, "loc" => 0.1, "scale" => 0.9)]
    for strategy in ("custom", "mixed", "adaptive"), spec in specs
        est = toy_estimator(spec=spec, strategy=strategy)
        quiet_run(est, 30000)
        draws = first.(est.chain[5001:end])
        @test mean(draws) ≈ 0.55 atol=0.035
        @test var(draws) ≈ 0.9^2 / 12 atol=0.015
    end
    est = toy_estimator(spec=specs[1])
    @test proposal_log_ratio(est, [0.3], [0.7]) ≈ log(0.7 / 0.3)
    @test_throws ArgumentError toy_estimator(spec=Dict("type" => "custom_function", "sampler" => identity))
end

@testset "Complete checkpoints reproduce uninterrupted execution" begin
    TEST_OBJECTIVE[] = (est, p) -> 10 * (p[1] - 0.65)^2
    uninterrupted = toy_estimator(adaptation=10)
    split = toy_estimator(adaptation=10)
    quiet_run(uninterrupted, 120)
    quiet_run(split, 40)
    mktempdir() do dir
        prefix = joinpath(dir, "checkpoint")
        save_checkpoint(split, prefix)
        resumed = toy_estimator(seed=99, adaptation=50)
        load_checkpoint!(resumed, prefix)
        @test resumed.current_iteration == 40
        @test resumed.n_accepted == count(split.acceptance_history)
        quiet_run(resumed, 80)
        for field in (:chain, :accepted_chain, :smm_values, :acceptance_history,
                      :household_count_history, :covariance_matrix, :mean_params,
                      :parameter_std_history, :simulated_households)
            @test getfield(resumed, field) == getfield(uninterrupted, field)
        end
        @test rand(resumed.rng) == rand(uninterrupted.rng)
        data = open(deserialize, "$(prefix)_data.jls")
        data["current_iteration"] += 1
        open(io -> serialize(io, data), "$(prefix)_data.jls", "w")
        @test_throws ErrorException load_checkpoint!(resumed, prefix)
        data["checkpoint_version"] = 0
        open(io -> serialize(io, data), "$(prefix)_data.jls", "w")
        @test_throws ErrorException load_checkpoint!(resumed, prefix)
    end
end

@testset "Sample replacement refreshes the current loss and stops at warmup" begin
    TEST_OBJECTIVE[] = (est, p) -> Float64(est.num_current_households)
    est = toy_estimator(adaptation=100, max_households=4)
    quiet_run(est, 1)
    increase_household_count!(est)
    @test est.num_current_households == 3
    @test est.smm_values[end] == 3.0
    @test compute_smm_objective(est, est.chain[end]) == est.smm_values[end]
    est.parameter_std_history = [zeros(1) for _ in 1:5]
    est.increase_household_spacer = 10
    @test should_increase_households(est)
    est.current_iteration = 100
    @test !should_increase_households(est)
    est.current_iteration = 1
    est.is_second_stage = true
    @test !should_increase_households(est)
end

@testset "Bundle integrity" begin
    mktempdir() do dir
        write(joinpath(dir, "rev3_export_in_progress"), "interrupted")
        @test_throws ErrorException validate_attanasio_bundle(dir)
    end
end
