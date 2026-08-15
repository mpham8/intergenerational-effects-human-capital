using LinearAlgebra
using Random
using Statistics

if !isdefined(@__MODULE__, :optimize_parameters_dxnes)
    include(joinpath(@__DIR__, "Julia_DelBoca_Implementation_Rev3.jl"))
end

# This file owns SMM orchestration. Rev3 owns household simulation, moment
# construction, the shared loss function, parameter bounds, and threaded DX-NES.
const SMM_WEIGHTING_DRAWS = 20
const SMM_WEIGHTING_RIDGE = 1e-6
const SMM_STAGE1_EVALUATIONS = DXNES_MAX_EVALUATIONS
const SMM_STAGE2_EVALUATIONS = DXNES_MAX_EVALUATIONS

function simulate_moments(
    parameter_values::Vector{Float64},
    households::Array{Float64, 3};
    household_workers::Int=0,
)::Vector{Float64}
    solved = solve_households(copy(households), parameter_values, household_workers)
    return moments(solved)
end

function objective(
    parameter_values::Vector{Float64},
    empirical_moments::Vector{Float64},
    weighting_matrix::Matrix{Float64},
    households::Array{Float64, 3},
    num_workers::Int=1,
)::Float64
    return rev3_smm_objective(
        parameter_values,
        empirical_moments,
        households;
        weighting_matrix=weighting_matrix,
        standardize_moments=true,
        household_workers=num_workers,
    )
end

function bounded_objective(
    parameter_values::Vector{Float64},
    empirical_moments::Vector{Float64},
    weighting_matrix::Matrix{Float64},
    households::Array{Float64, 3},
    lower_bounds::Vector{Float64}=DXNES_LOWER_BOUNDS,
    upper_bounds::Vector{Float64}=DXNES_UPPER_BOUNDS,
    num_workers::Int=1,
)::Float64
    if any(parameter_values .< lower_bounds) || any(parameter_values .> upper_bounds)
        return 1e12
    end
    return objective(
        parameter_values, empirical_moments, weighting_matrix, households, num_workers,
    )
end

function compute_weighting_matrix(
    moment_draws::Matrix{Float64};
    ridge::Float64=SMM_WEIGHTING_RIDGE,
)::Matrix{Float64}
    size(moment_draws, 1) >= 2 || error("At least two moment draws are required.")
    ridge > 0.0 || error("The weighting ridge must be positive.")

    moment_covariance = Matrix(cov(moment_draws, dims=1))
    decomposition = eigen(Symmetric(moment_covariance))
    regularized_values = max.(decomposition.values, ridge)
    weighting_matrix = decomposition.vectors * Diagonal(1.0 ./ regularized_values) *
                       decomposition.vectors'
    weighting_matrix = Matrix(Symmetric(weighting_matrix))

    # Normalizing the trace keeps the magnitude of the pseudo-likelihood stable
    # when adaptive Metropolis switches from identity to optimal SMM weights.
    return weighting_matrix .* (size(weighting_matrix, 1) / tr(weighting_matrix))
end

function bootstrap_moment_draws(
    parameter_values::Vector{Float64},
    households::Array{Float64, 3};
    draws::Int=SMM_WEIGHTING_DRAWS,
    empirical_moments::Union{Nothing, Vector{Float64}}=nothing,
    rng::AbstractRNG=Random.default_rng(),
)::Matrix{Float64}
    draws >= 2 || error("At least two weighting draws are required.")
    n_households = size(households, 1)
    moment_draws = zeros(draws, NUM_MOMENTS)
    moment_scale = empirical_moments === nothing ? ones(NUM_MOMENTS) :
                   max.(abs.(empirical_moments), 1e-8)

    for draw in 1:draws
        indices = rand(rng, 1:n_households, n_households)
        bootstrap_households = households[indices, :, :]
        moment_draws[draw, :] = simulate_moments(
            parameter_values, bootstrap_households; household_workers=0,
        ) ./ moment_scale
    end
    return moment_draws
end

function two_step_smm(
    empirical_moments::Vector{Float64},
    initial_guess::Vector{Float64},
    households::Array{Float64, 3},
    tolerances::Vector{Float64}=[0.01, 0.001],
    num_workers::Int=0;
    stage1_evaluations::Int=SMM_STAGE1_EVALUATIONS,
    stage2_evaluations::Int=SMM_STAGE2_EVALUATIONS,
    weighting_draws::Int=SMM_WEIGHTING_DRAWS,
    population_size::Int=DXNES_POPULATION_SIZE,
    random_seed::Union{Int, Nothing}=nothing,
    return_details::Bool=false,
)
    # tolerances remains in the signature for compatibility with previous
    # callers. Global DX-NES is controlled by evaluation budgets, not gradients.
    length(tolerances) == 2 || error("Expected two legacy tolerance values.")
    bounded_initial = clamp.(initial_guess, DXNES_LOWER_BOUNDS, DXNES_UPPER_BOUNDS)
    optimizer_threads = num_workers <= 0 ? max(1, Threads.nthreads() - 1) : num_workers
    rng = random_seed === nothing ? Random.default_rng() : MersenneTwister(random_seed)

    stage1_weighting = Matrix{Float64}(I, NUM_MOMENTS, NUM_MOMENTS)
    println("SMM stage 1: threaded DX-NES with identity weighting")
    stage1 = optimize_parameters_dxnes(
        empirical_moments,
        households;
        lower_bounds=DXNES_LOWER_BOUNDS,
        upper_bounds=DXNES_UPPER_BOUNDS,
        max_evaluations=stage1_evaluations,
        population_size=population_size,
        random_seed=random_seed,
        optimizer_threads=optimizer_threads,
        weighting_matrix=stage1_weighting,
        standardize_moments=true,
        initial_guess=bounded_initial,
    )

    println("Estimating the stage-2 weighting matrix from bootstrap moment draws")
    moment_draws = bootstrap_moment_draws(
        stage1.parameters,
        households;
        draws=weighting_draws,
        empirical_moments=empirical_moments,
        rng=rng,
    )
    stage2_weighting = compute_weighting_matrix(moment_draws)

    println("SMM stage 2: threaded DX-NES with the estimated weighting matrix")
    stage2_seed = random_seed === nothing ? nothing : random_seed + 1
    stage2 = optimize_parameters_dxnes(
        empirical_moments,
        households;
        lower_bounds=DXNES_LOWER_BOUNDS,
        upper_bounds=DXNES_UPPER_BOUNDS,
        max_evaluations=stage2_evaluations,
        population_size=population_size,
        random_seed=stage2_seed,
        optimizer_threads=optimizer_threads,
        weighting_matrix=stage2_weighting,
        standardize_moments=true,
        initial_guess=stage1.parameters,
    )

    details = (
        parameters=stage2.parameters,
        objective=stage2.objective,
        stage1=stage1,
        stage2=stage2,
        stage1_weighting=stage1_weighting,
        stage2_weighting=stage2_weighting,
        moment_draws=moment_draws,
    )
    return return_details ? details : (details.parameters, details.objective)
end

function grid_search_smm(
    empirical_moments::Vector{Float64},
    households::Array{Float64, 3},
    step_sizes::Dict=Dict(),
    tolerances::Vector{Float64}=[0.01, 0.001];
    kwargs...,
)
    @warn "grid_search_smm now delegates to global DX-NES; step_sizes is ignored."
    return two_step_smm(
        empirical_moments, parameters, households, tolerances; kwargs...,
    )
end

function bootstrap_confidence_intervals(
    empirical_moments::Vector{Float64},
    initial_guess::Vector{Float64},
    households::Array{Float64, 3};
    bootstrap_samples::Int=10,
    stage1_evaluations::Int=250,
    stage2_evaluations::Int=250,
    weighting_draws::Int=10,
    population_size::Int=DXNES_POPULATION_SIZE,
    random_seed::Union{Int, Nothing}=nothing,
)
    bootstrap_samples >= 2 || error("At least two bootstrap samples are required.")
    rng = random_seed === nothing ? Random.default_rng() : MersenneTwister(random_seed)
    estimates = zeros(bootstrap_samples, length(PARAMETER_NAMES))
    n_households = size(households, 1)

    for sample in 1:bootstrap_samples
        indices = rand(rng, 1:n_households, n_households)
        bootstrap_households = households[indices, :, :]
        sample_seed = random_seed === nothing ? nothing : random_seed + 2 * sample
        estimate, _ = two_step_smm(
            empirical_moments,
            initial_guess,
            bootstrap_households;
            stage1_evaluations=stage1_evaluations,
            stage2_evaluations=stage2_evaluations,
            weighting_draws=weighting_draws,
            population_size=population_size,
            random_seed=sample_seed,
        )
        estimates[sample, :] = estimate
    end

    lower = [quantile(estimates[:, i], 0.025) for i in axes(estimates, 2)]
    upper = [quantile(estimates[:, i], 0.975) for i in axes(estimates, 2)]
    return lower, upper
end

function condition_number(
    parameter_values::Vector{Float64},
    households::Array{Float64, 3};
    epsilon::Float64=1e-5,
)::Float64
    base_moments = simulate_moments(parameter_values, households)
    jacobian = zeros(length(base_moments), length(parameter_values))
    for parameter_index in eachindex(parameter_values)
        perturbed = copy(parameter_values)
        perturbed[parameter_index] += epsilon
        jacobian[:, parameter_index] =
            (simulate_moments(perturbed, households) - base_moments) / epsilon
    end
    return cond(jacobian)
end

function main()
    Random.seed!(42)
    households = create_households(NUMBER_OF_HOUSEHOLDS)
    empirical_households = solve_households(copy(households), parameters_to_optimize, 0)
    empirical_moments = moments(empirical_households)
    result = two_step_smm(
        empirical_moments,
        parameters,
        households;
        random_seed=42,
        return_details=true,
    )
    println("Final DX-NES SMM objective: $(result.objective)")
    for (name, value) in zip(PARAMETER_NAMES, result.parameters)
        println("$name: $value")
    end
    return result
end

if abspath(PROGRAM_FILE) == @__FILE__
    main()
end
