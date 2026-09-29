# Imports
using CMAEvolutionStrategy


# Import from implementation file
include("Julia_DelBoca_Implementation_Rev2.jl")

# Constants

const MU_WAGE_RATE_GROWTH = 0.03  # Example value, adjust as needed
const ETA_STD_WAGE_RATE_GROWTH = 0.01  # Example value, adjust as needed

# Model
const NUM_PERIODS = 3
const NUM_MOMENTS = 18
const NUM_PARAMETERS = 16
const HOUSEHOLD_COLUMN_NAMES = [
    "Parent Human Capital",
    "Wage Rate Trajectory",
    "Government Inputs",
    "Optimal Leisure",
    "Optimal Consumption",
    "Optimal Parental Investment",
    "Child Human Capital"
]

# MCMC
const INITIAL_PARAMS = Dict(
  :θ1h=>0.55, :θ1e=>0.3, :θ1g=>0.15,
  :θ2h=>0.55, :θ2e=>0.3, :θ2g=>0.15,
  :θ3h=>0.55, :θ3e=>0.3, :θ3g=>0.15,
  :ρ1=>-1, :ρ2=>-1, :ρ3=>-1,
  :A1=>1.0, :A2=>1.0, :A3=>1.0,
  :fh=>50
) # close, but not equal to 'empirical' params


function create_households(num_households::Int)::Array{Float64, 3}
    # 7 columns, 3 for inputs (parent HC, wage rate, and govt), and 4 for solver (consumption, leisure, investment, child HC)
    # Households dimensions: number, period, household variable
    households = zeros(num_households, NUM_PERIODS, 7)

    # Parents' human capital
    # TODO: substitute this with draws from an observed proxy for parents' human capital
    parents_hc = randn(num_households, 1) .* 0.2 .+ 1.0
    households[:, :, 1] .= parents_hc

    # Wage rate
    wage_rates = ones(num_households)
    households[:, 1, 2] = wage_rates
    for i in 1:NUM_PERIODS
        wage_rates = (1 + MU_WAGE_RATE_GROWTH) * wage_rates + randn(num_households) * ETA_STD_WAGE_RATE_GROWTH
        households[:, i, 2] = wage_rates
    end

    # Government inputs
    # TODO: create
    government_input_means = [0.0005, 0.019, 0.017]
    government_input_stds = [0.0001, 0.004, 0.003]
    for i in 2:NUM_PERIODS
        households[:, i, 3] = randn(num_households) * government_input_stds[i] .+ government_input_means[i]
    end

    households[households .< 0] .= 0
    return households
end

function solve_one(args)::Array{Float64, 2}
    # expects (params_dict::Dict{Symbol,Real}, hh::Array{Float64,2})
    params_dict, hh = args
    # solve_household is expected to accept a Dict{Symbol,Real} as its first argument
    solved = solve_household(params_dict, hh)
    return solved
end

function solve_households(households::Array{Float64, 3}, parameters::Union{Vector{<:Real}, Dict{Symbol,Real}}, num_workers::Int=0)::Array{Float64, 3}
    """
    Solves a numerical system for each household in parallel using threads.

    Arguments
    ---------
    households : Array{Float64, 3}
        A 3D array representing the households to be solved. Each row corresponds to a household.
    parameters : Vector{Float64} or Dict{Symbol,Real}
        A vector of parameters or a parameter dictionary required for solving the system for each household.
    num_workers : Int, optional
        Number of threads to use. If 0, uses available threads divided by number of chains.

    Returns
    -------
    Array{Float64, 3}
        The updated households array with the solution results assigned to the appropriate columns.
    """
    if num_workers <= 0
        num_workers = max(1, Sys.CPU_THREADS)
    end

    # Normalize parameters: allow passing either a vector or a dict.
    params_dict = nothing
    if isa(parameters, AbstractVector)
        # vector_to_params_dict is defined elsewhere in this file
        params_dict = vector_to_params_dict(parameters, INITIAL_PARAMS, PARAM_ORDER)
    elseif isa(parameters, Dict)
        params_dict = parameters
    else
        error("parameters must be a Vector or Dict")
    end

    results = Vector{Array{Float64, 2}}(undef, size(households, 1))

    Threads.@threads for i in 1:size(households, 1)
        # pass the dict to the worker routine; solve_one expects a tuple (dict, household_slice)
        results[i] = solve_one((params_dict, households[i, :, :]))
    end
    print(results)

    for i in 1:length(results)
        households[i, :, 4:end] = results[i]
    end
    return households
end

function moments(households::Array{Float64, 3})::Vector{Float64}
    moments = zeros(NUM_MOMENTS)
    # The columns for each respective moment (adjusted for 1-based indexing)
    moment_leisure = 5
    moment_expenditure = 6
    moment_child_hc = 7
    for i in 1:NUM_PERIODS
        moments[i] = mean(households[:, i, moment_leisure])
        moments[i + NUM_PERIODS] = mean(households[:, i, moment_expenditure])
        moments[i + 2*NUM_PERIODS] = mean(households[:, i, moment_child_hc])
        moments[i + 3*NUM_PERIODS] = std(households[:, i, moment_leisure])
        moments[i + 4*NUM_PERIODS] = std(households[:, i, moment_expenditure])
        moments[i + 5*NUM_PERIODS] = std(households[:, i, moment_child_hc])
    end

    for i in 1:NUM_MOMENTS
        if moments[i] == 0
            # silently continue; zeros may cause problems for SMM but handled elsewhere
        end
    end

    return moments
end

struct Estimator
    simulated_households::Array{Float64,3}
    empirical_moments::Vector{Float64}
    weighting_matrix::Array{Float64,2}
end

# Initialize estimator before using compute_objective
const NUM_HOUSEHOLDS = 500
simulated_households = create_households(NUM_HOUSEHOLDS)
empirical_households = create_households(NUM_HOUSEHOLDS) # Replace with actual empirical data
solved_empirical_households = solve_households(empirical_households, INITIAL_PARAMS)
empirical_moments = moments(solved_empirical_households) # Replace with actual empirical moments
weighting_matrix = I(NUM_MOMENTS) # Identity matrix as placeholder; replace as needed

global estimator = Estimator(simulated_households, empirical_moments, weighting_matrix)

function compute_objective(params::Vector{Float64})
    """Compute objective function with standardized moments."""
    
    # Calculate simulated moments, using the same households every time
    # NOTE: using the same households every time is based on this article: https://opensourceecon.github.io/CompMethods/struct_est/SMM.html
    solved_households = solve_households(estimator.simulated_households, params)
    if solved_households == NaN
        println("WARNING: households were not solved properly")
    end
    simulated_moments = moments(solved_households)

    
    # Moment differences
    # NOTE: moments should always be POSITIVE. If moments are not positive, this line may not work
    # TO DEBUG NAN OBJECTIVE - FIGURE OUT IF MEAN OR STD IS NAN FOR SOME VALUES
    moment_diff = (simulated_moments .- estimator.empirical_moments) ./ estimator.empirical_moments
    smm_obj = transpose(moment_diff) * estimator.weighting_matrix * moment_diff
    
    
    return smm_obj
end
# CODE BELOW WAS AI GENERATED
# === CMA-ES optimizer (replacement for $SELECTION_PLACEHOLDER$) ===
# A compact, self-contained (isotropic sigma) CMA-ES implementation that
# optimizes `compute_objective(params::Vector{Float64})`.


const PARAM_ORDER = [
    :θ1h, :θ1e, :θ1g,
    :θ2h, :θ2e, :θ2g,
    :θ3h, :θ3e, :θ3g,
    :ρ1, :ρ2, :ρ3,
    :A1, :A2, :A3,
    :fh
]

function params_dict_to_vector(dict::Dict{Symbol,Real}, order::Vector{Symbol})::Vector{Float64}
    return [Float64(dict[k]) for k in order]
end

function vector_to_params_dict(vec::Vector{Real}, base::Dict{Symbol,Real}, order::Vector{Symbol})
    p = copy(base)
    for (i, k) in enumerate(order)
        p[k] = vec[i]
    end
    return p
end

# Wrap compute_objective so it accepts a vector in the same ordering as PARAM_ORDER.
function objective_from_vector(vec::Vector{Float64})::Float64
    print(vec)
    val = compute_objective(Vector(vec))
    if !isfinite(val) || isnan(val)
        return 1e30
    end
    return val
end
function rosenbrock(x)
    n = length(x)
    sum(100 * (x[2i-1]^2 - x[2i])^2 + (x[2i-1] - 1)^2 for i in 1:div(n, 2))
end

function run_cma_es_optimization()
    initial_params_vector = params_dict_to_vector(INITIAL_PARAMS, PARAM_ORDER)
    test = minimize(rosenbrock, fill(0.0, 10), 1., xtol=1e-5)
    result = minimize(
        objective_from_vector,
        initial_params_vector,
        1., 
        xtol=1e-5,
    )
    if result === nothing
        println("CMA-ES optimization did not converge.")
    else
        best_solution, best_value = result
        println("Best objective value: ", best_value)
        println("Best parameters: ", best_solution)
    end

    best_solution, best_value = result
    best_params_dict = vector_to_params_dict(best_solution, INITIAL_PARAMS, PARAM_ORDER)
    return best_params_dict
end


function main()
    sleep(1)  # Give user time to read any prior output
    best_params = run_cma_es_optimization()
    println("Optimized parameters: ", best_params)
end
main()