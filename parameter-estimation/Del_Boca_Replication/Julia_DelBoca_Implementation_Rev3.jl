# Del Boca implementation shared by the SMM and adaptive Metropolis drivers.
# Parent policies come from new_CES_solver, and DX-NES parallelizes candidate
# evaluations while each candidate applies its solved policy serially.

# Imports
using Statistics
using Random
using CSV
using DataFrames
using BlackBoxOptim
using .Threads
include(joinpath(@__DIR__, "Rev3_Data.jl"))
include(joinpath(@__DIR__, "..", "..", "dsge-simulations", "new_CES_solver.jl"))

# ---------------- CONSTANTS ------------------
# General
const NUMBER_OF_HOUSEHOLDS = 10000
const HOUSEHOLD_VARIABLES = [
    "Parent Human Capital",
    "Wage Rate Trajectory",
    "Government Inputs",
    "Optimal Consumption",
    "Optimal Parental Investment",
    "Optimal Leisure",
    "Child Human Capital Trajectory"
]
const NUM_PERIODS = 3
const NUM_MOMENTS = 18
const PARAM_OUTPUT_FILEPATH = "parameter_estimates_SMM.txt"
const METHOD_OPTIMIZATION = :dxnes

# Household-generation
const MU_WAGE_RATE_GROWTH = 0.07
const ETA_STD_WAGE_RATE_GROWTH = 0.02

# Parent-policy solver defaults
const PARENT_POLICY_BETA = 0.96
const PARENT_POLICY_ETA = 2.0
const PARENT_POLICY_GAMMA = 2.0
const PARENT_POLICY_TAX_RATE = 0.0
const PARENT_POLICY_INITIAL_CHILD_HC = 1.0
const PARENT_POLICY_GRID_POINTS = 25
const PARENT_POLICY_PARENT_H_GRID_POINTS = 10

# The true nested-CES parameters are fixed by Attanasio. Rev3 estimates the
# terminal utility weight and the technology parents perceive, as proposed in
# equation (31) of the newer methodology notes.
const DXNES_LOWER_BOUNDS = [0.1, -4.0, 0.05, 0.05, 0.05]
const DXNES_UPPER_BOUNDS = [100.0, 1.0, 0.95, 0.95, 0.95]
const DXNES_MAX_EVALUATIONS = 1000
const DXNES_POPULATION_SIZE = 12

# Actual parameters to estimate
const PARAMETER_NAMES = [
    "Delta",
    "perceived_rho",
    "perceived_theta_h_1",
    "perceived_theta_h_2",
    "perceived_theta_h_3"
]

# Initial guesses used to warm-start DX-NES and adaptive Metropolis.
const parameters = [
    12.0,   # Delta parameter
    -0.3,   # perceived rho
    0.35,   # perceived theta_h, period 1
    0.15,   # perceived theta_h, period 2
    0.25    # perceived theta_h, period 3
]
const parameters_to_optimize = [20.0, -0.2, 0.3, 0.2, 0.2]

function test_true_technology()
    return TrueCESTechnology(
        intercept=zeros(3),
        parent_education_coefficient=zeros(3),
        theta_h=[0.30, 0.25, 0.20],
        theta_p=fill(0.5, 3),
        ces_delta=fill(0.5, 3),
        mu=fill(0.5, 3),
        rho=fill(-0.1, 3),
        control_function_coefficient=zeros(3),
        residual_sd=fill(0.1, 3),
    )
end

const ACTIVE_TRUE_TECHNOLOGY =
    Ref{Union{Nothing, TrueCESTechnology}}(test_true_technology())

function set_true_technology!(technology::TrueCESTechnology)
    ACTIVE_TRUE_TECHNOLOGY[] = validate_true_technology(technology)
    return technology
end

function load_and_set_true_technology!(filepath::String=attanasio_bridge_file())
    return set_true_technology!(load_true_technology(filepath))
end

function true_technology()
    technology = ACTIVE_TRUE_TECHNOLOGY[]
    technology === nothing && error("No true Attanasio technology has been loaded.")
    return technology
end

# ----------------- FUNCTIONS ---------------------

function load_latent_factors(filepath::String)::Vector{Float64}
    # Random parameters, since we don't have any data
    # TODO: replace with actual data loading from filepath
    means_leisure = [2.0, 1.0, 2.0, 3.0]
    stds_leisure = [0.5, 0.2, 0.5, 0.5]
    means_parental_investment = [5.0, 4.0, 3.0, 2.0]
    stds_parental_investment = [1.0, 0.5, 0.3, 0.2]
    means_child_hc = [2.0, 3.0, 4.0, 5.0]
    stds_child_hc = [0.5, 1.0, 1.5, 2.0]

    return vcat(means_leisure, means_parental_investment, means_child_hc,
                stds_leisure, stds_parental_investment, stds_child_hc)
end

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

function child_hc_grid(initial_child_hc::Float64, parent_hc::Float64)::Vector{Float64}
    lower = max(1e-4, 0.25 * min(initial_child_hc, parent_hc))
    upper = max(2.0, 4.0 * max(initial_child_hc, parent_hc))
    return collect(range(lower, upper, length=PARENT_POLICY_GRID_POINTS))
end

function parent_hc_grid(households::Array{Float64, 3})::Vector{Float64}
    observed_parent_hc = max.(vec(households[:, 1, 1]), 1e-8)
    lower = minimum(observed_parent_hc)
    upper = maximum(observed_parent_hc)
    if isapprox(lower, upper)
        lower = max(1e-4, 0.9 * lower)
        upper = 1.1 * upper
    end
    return collect(range(lower, upper, length=PARENT_POLICY_PARENT_H_GRID_POINTS))
end

function bilinear_policy_interp(policy_values, hc_grid, parent_h_grid, t, hc, parent_h)
    hc_hi = clamp(searchsortedfirst(hc_grid, hc), 2, length(hc_grid))
    hc_lo = hc_hi - 1
    parent_hi = clamp(searchsortedfirst(parent_h_grid, parent_h), 2, length(parent_h_grid))
    parent_lo = parent_hi - 1

    hc_weight = (hc - hc_grid[hc_lo]) / (hc_grid[hc_hi] - hc_grid[hc_lo])
    parent_weight = (parent_h - parent_h_grid[parent_lo]) /
                    (parent_h_grid[parent_hi] - parent_h_grid[parent_lo])
    low_parent_value = (1.0 - hc_weight) * policy_values[t, hc_lo, parent_lo] +
                       hc_weight * policy_values[t, hc_hi, parent_lo]
    high_parent_value = (1.0 - hc_weight) * policy_values[t, hc_lo, parent_hi] +
                        hc_weight * policy_values[t, hc_hi, parent_hi]
    return (1.0 - parent_weight) * low_parent_value + parent_weight * high_parent_value
end

function solve_parent_policy(parameter_values::Vector{Float64}, households::Array{Float64, 3})
    length(parameter_values) == 5 || error(
        "Expected [Delta, perceived_rho, perceived_theta_h_1, perceived_theta_h_2, perceived_theta_h_3].",
    )

    delta, perceived_rho, theta_1, theta_2, theta_3 = Float64.(parameter_values)
    perceived_theta_h = [theta_1, theta_2, theta_3]
    technology = true_technology()

    # For large-N SMM, the expensive dynamic-programming solve should happen
    # once per parameter vector, not once per household. The new CES solver
    # spans both child and parent human capital. Solving this
    # common 2D policy once preserves parent-HC heterogeneity while retaining
    # the large-N advantage over solving dynamic programming per household.
    representative_parent_hc = max(mean(households[:, 1, 1]), 1e-8)
    wages = max.(vec(mean(households[:, :, 2], dims=1)), 1e-8)
    public_inputs = max.(vec(mean(households[:, :, 3], dims=1)), 1e-8)
    transfers = zeros(NUM_PERIODS)
    tau = fill(PARENT_POLICY_TAX_RATE, NUM_PERIODS)

    hc_grid = child_hc_grid(PARENT_POLICY_INITIAL_CHILD_HC, representative_parent_hc)
    parent_grid = parent_hc_grid(households)

    return (
        policy=compute_parent_policy(
            hc_grid,
            parent_grid,
            wages,
            transfers,
            public_inputs,
            perceived_theta_h,
            technology.theta_p,
            technology.theta_p,
            perceived_rho,
            technology.intercept,
            technology.theta_h,
            technology.theta_p,
            technology.theta_p,
            technology.rho,
            technology.intercept;
            beta=PARENT_POLICY_BETA,
            eta=PARENT_POLICY_ETA,
            gamma=PARENT_POLICY_GAMMA,
            # In the project notes, production productivity is ln(A)=d+X'delta+u.
            # The existing SMM parameter named Delta is therefore treated as the
            # parent's terminal value weight on child human capital, not as d.
            # Delta values terminal child human capital. Earlier periods value
            # investment through continuation, rather than receiving Delta in
            # every period's flow payoff.
            b=[0.0, 0.0, delta],
            tau=tau,
            sigma_h=technology.residual_sd,
            perceived_theta_p=technology.theta_p,
            perceived_ces_delta=technology.ces_delta,
            perceived_mu=technology.mu,
            perceived_parent_education_coefficient=
                technology.parent_education_coefficient,
            true_theta_p=technology.theta_p,
            true_ces_delta=technology.ces_delta,
            true_mu=technology.mu,
            true_parent_education_coefficient=
                technology.parent_education_coefficient,
            min_e=1e-8,
            periods=NUM_PERIODS,
        ),
        terminal_weight=delta,
        true_technology=technology,
        perceived_rho=perceived_rho,
        perceived_theta_h=perceived_theta_h,
    )
end

function solve_household_from_policy(parent_policy, household::Matrix{Float64})::Matrix{Float64}
    policy = parent_policy.policy
    solved = zeros(NUM_PERIODS, 4)
    current_child_hc = PARENT_POLICY_INITIAL_CHILD_HC

    for t in 1:NUM_PERIODS
        # Applying a pre-solved policy is cheap: interpolate labor and desired
        # investment from the common policy grid, then recompute each household's
        # budget and child-HC transition using its own wage, parent HC, and
        # public input. This keeps the large-N flow parallel while avoiding an
        # expensive policy solve inside every threaded iteration.
        parent_hc = max(Float64(household[t, 1]), 1e-8)
        wage = max(Float64(household[t, 2]), 1e-8)
        public_input = max(Float64(household[t, 3]), 1e-8)

        labor = bilinear_policy_interp(
            policy.l_policy, policy.hc_grid, policy.parent_h_grid, t, current_child_hc, parent_hc,
        )
        desired_investment = bilinear_policy_interp(
            policy.e_policy, policy.hc_grid, policy.parent_h_grid, t, current_child_hc, parent_hc,
        )
        labor = clamp(labor, 0.0, 1.0 - 1e-8)
        resources = (1.0 - PARENT_POLICY_TAX_RATE) * wage * parent_hc * labor
        investment = min(max(desired_investment, 1e-8), max(resources - 1e-8, 1e-8))
        consumption = max(resources - investment, 1e-8)
        technology = parent_policy.true_technology
        true_productivity = exp(
            technology.intercept[t] +
            technology.parent_education_coefficient[t] * parent_hc,
        )
        deterministic_child_hc = hc_production(
            current_child_hc,
            investment,
            1.0 - labor,
            public_input,
            true_productivity,
            technology.theta_h[t],
            technology.theta_p[t],
            technology.theta_p[t],
            technology.rho[t];
            theta_p=technology.theta_p[t],
            ces_delta=technology.ces_delta[t],
            mu=technology.mu[t],
        )
        # The new solver integrates a multiplicative lognormal shock when it
        # rolls forward the true state. Use that same expected transition here
        # so simulated household paths match the state law used to solve policy.
        next_child_hc =
            deterministic_child_hc * exp(0.5 * technology.residual_sd[t]^2)

        # Stable household contract: consumption, monetary investment,
        # leisure, and realized child human capital occupy columns 4 through 7.
        solved[t, :] = [consumption, investment, 1.0 - labor, next_child_hc]
        current_child_hc = max(next_child_hc, 1e-8)
    end

    return solved
end

function solve_household(parameters::Vector{Float64}, household::Matrix{Float64})::Matrix{Float64}
    household_batch = zeros(1, size(household, 1), size(household, 2))
    household_batch[1, :, :] = household
    parent_policy = solve_parent_policy(parameters, household_batch)
    return solve_household_from_policy(parent_policy, household)
end

function solve_one(args)::Array{Float64, 2}
    parent_policy, hh = args
    # A household solve now applies a reusable parent policy instead of
    # recomputing dynamic programming for this household. That is the key
    # large-N runtime improvement: O(1) policy solves per SMM parameter vector
    # instead of O(number of households).
    solved = solve_household_from_policy(parent_policy, hh)
    return solved
end

function solve_households(households::Array{Float64, 3}, parameters::Vector{Float64}, num_workers::Int=0)::Array{Float64, 3}
    """
    Solves households in parallel using a single reusable parent policy.

    Arguments
    ---------
    households : Array{Float64, 3}
        A 3D array representing the households to be solved. Each row corresponds to a household.
    parameters : Vector{Float64}
        A vector of parameters required for solving the system for each household.
    num_workers : Int, optional
        Number of Julia threads to use. If 0, uses all threads available to Julia.

    Returns
    -------
    Array{Float64, 3}
        The updated households array with the solution results assigned to the appropriate columns.
    """
    worker_count = num_workers <= 0 ? Threads.nthreads() : min(num_workers, Threads.nthreads())
    parent_policy = solve_parent_policy(parameters, households)
    results = Vector{Array{Float64, 2}}(undef, size(households, 1))

    # A value of one is used when an outer optimizer already evaluates
    # candidates in parallel; this avoids nested-thread oversubscription.
    if worker_count == 1
        for i in 1:size(households, 1)
            results[i] = solve_one((parent_policy, households[i, :, :]))
        end
    else
        # Explicit chunks honor num_workers. This also lets callers reserve
        # threads for other chains instead of always consuming every Julia thread.
        chunk_size = cld(size(households, 1), worker_count)
        @sync for worker in 1:worker_count
            first_household = (worker - 1) * chunk_size + 1
            last_household = min(worker * chunk_size, size(households, 1))
            first_household > last_household && continue
            Threads.@spawn for i in first_household:last_household
                results[i] = solve_one((parent_policy, households[i, :, :]))
            end
        end
    end

    for i in 1:length(results)
        households[i, :, 4:end] = results[i]
    end
    return households
end

function moments(households::Array{Float64, 3})::Vector{Float64}
    moments = zeros(NUM_MOMENTS)
    # The columns for each respective moment (adjusted for 1-based indexing)
    moment_leisure = 6
    moment_expenditure = 5
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
            # # FOR DEBUGGING PURPOSES
            # println("WARNING: moment # ", i, " is 0. ")
            # println("Here is a sample of households: ")
            # for i = 1:1
            #     println("Household ", i, ": ", repr("text/plain", households[i, :, :]))
            # end
            # println("This may cause problems for evaluating the SMM function.")
        end
    end

    return moments
end

function rev3_smm_objective(
    parameter_values,
    empirical_moments::Vector{Float64},
    households::Array{Float64, 3};
    weighting_matrix::Union{Nothing, AbstractMatrix}=nothing,
    standardize_moments::Bool=false,
    household_workers::Int=1,
)::Float64
    length(empirical_moments) == NUM_MOMENTS ||
        error("Expected $NUM_MOMENTS empirical moments, received $(length(empirical_moments)).")
    if weighting_matrix !== nothing
        size(weighting_matrix) == (NUM_MOMENTS, NUM_MOMENTS) ||
            error("The weighting matrix must be $NUM_MOMENTS by $NUM_MOMENTS.")
    end

    try
        parameters = Float64.(collect(parameter_values))
        # DX-NES evaluates parameter vectors concurrently. Each evaluation gets
        # its own household copy and runs household application serially, which
        # prevents data races and nested-thread oversubscription.
        solved = solve_households(copy(households), parameters, household_workers)
        simulated_moments = moments(solved)
        moment_difference = simulated_moments - empirical_moments
        if standardize_moments
            moment_scale = max.(abs.(empirical_moments), 1e-8)
            moment_difference ./= moment_scale
        end
        loss = if weighting_matrix === nothing
            mean(abs2, moment_difference)
        else
            dot(moment_difference, weighting_matrix * moment_difference) / NUM_MOMENTS
        end
        return isfinite(loss) ? loss : 1e12
    catch error
        @warn "Rev3 SMM objective evaluation failed" exception=(error, catch_backtrace())
        return 1e12
    end
end

function rev3_moment_mse(
    parameter_values,
    empirical_moments::Vector{Float64},
    households::Array{Float64, 3},
)::Float64
    return rev3_smm_objective(parameter_values, empirical_moments, households)
end

function optimize_parameters_dxnes(
    empirical_moments::Vector{Float64},
    households::Array{Float64, 3};
    lower_bounds::Vector{Float64}=DXNES_LOWER_BOUNDS,
    upper_bounds::Vector{Float64}=DXNES_UPPER_BOUNDS,
    max_evaluations::Int=DXNES_MAX_EVALUATIONS,
    population_size::Int=DXNES_POPULATION_SIZE,
    random_seed::Union{Int, Nothing}=nothing,
    optimizer_threads::Int=max(1, Threads.nthreads() - 1),
    weighting_matrix::Union{Nothing, AbstractMatrix}=nothing,
    standardize_moments::Bool=false,
    initial_guess::Union{Nothing, Vector{Float64}}=nothing,
    trace_mode::Symbol=:compact,
)
    length(lower_bounds) == length(PARAMETER_NAMES) ||
        error("Expected $(length(PARAMETER_NAMES)) lower bounds.")
    length(upper_bounds) == length(PARAMETER_NAMES) ||
        error("Expected $(length(PARAMETER_NAMES)) upper bounds.")
    all(upper_bounds .> lower_bounds) || error("Every upper bound must exceed its lower bound.")
    max_evaluations > 0 || error("max_evaluations must be positive.")
    population_size >= 2 || error("population_size must be at least two.")
    if initial_guess !== nothing
        length(initial_guess) == length(PARAMETER_NAMES) ||
            error("Expected $(length(PARAMETER_NAMES)) initial parameters.")
        all((lower_bounds .<= initial_guess) .& (initial_guess .<= upper_bounds)) ||
            error("The initial guess must lie inside the parameter bounds.")
    end
    random_seed === nothing || Random.seed!(random_seed)

    objective = candidate -> rev3_smm_objective(
        candidate,
        empirical_moments,
        households;
        weighting_matrix=weighting_matrix,
        standardize_moments=standardize_moments,
        household_workers=1,
    )
    search_range = collect(zip(lower_bounds, upper_bounds))
    options = (
        Method=METHOD_OPTIMIZATION,
        SearchRange=search_range,
        NumDimensions=length(PARAMETER_NAMES),
        PopulationSize=population_size,
        MaxFuncEvals=max_evaluations,
        TraceMode=trace_mode,
    )

    # BlackBoxOptim reserves one Julia thread to coordinate its workers. Fall
    # back to serial DX-NES when Julia was launched with only one thread.
    available_optimizer_workers = min(optimizer_threads, Threads.nthreads() - 1)
    result = if available_optimizer_workers > 0 && initial_guess === nothing
        BlackBoxOptim.bboptimize(objective; options..., NThreads=available_optimizer_workers)
    elseif available_optimizer_workers > 0
        BlackBoxOptim.bboptimize(
            objective, initial_guess; options..., NThreads=available_optimizer_workers,
        )
    elseif initial_guess === nothing
        BlackBoxOptim.bboptimize(objective; options...)
    else
        BlackBoxOptim.bboptimize(objective, initial_guess; options...)
    end

    best_objective = Float64(BlackBoxOptim.best_fitness(result))
    return (
        parameters=Float64.(BlackBoxOptim.best_candidate(result)),
        objective=best_objective,
        mse=best_objective,
        optimizer_result=result,
    )
end

function simulate_10000_households()
    """
    Simulate 10,000 households, solve them, and save results to a CSV file.
    """
    # Number of households to simulate
    num_households = 5000
    
    # Parameters for solving (same as in tests)
    simulation_parameters = [20.0, -0.2, 0.3, 0.2, 0.2]
    
    # Start timing
    start_time = time()
    
    # Create households
    println("Creating $num_households households...")
    households = create_households(num_households)
    
    # Solve households (using all available workers)
    println("Solving households...")
    solved_households = solve_households(households, simulation_parameters, 0)
    
    # Calculate elapsed time
    elapsed_time = time() - start_time
    println("Total execution time: $(round(elapsed_time, digits=3)) seconds")
    
    # Prepare data for CSV
    # Create column names
    column_names = [:Household_ID, :Period]
    append!(column_names, [Symbol(replace(var, " " => "_")) for var in HOUSEHOLD_VARIABLES])
    
    # Create empty vectors for each column
    column_data = [[] for _ in column_names]
    
    # Create DataFrame with proper constructor
    output_data = DataFrame([name => (i <= 2 ? Int64[] : Float64[]) for (i, name) in enumerate(column_names)])
    
    # Populate the DataFrame
    num_periods = size(solved_households, 2)
    for hh_id in 1:num_households
        for period in 1:num_periods
            # Create row as a vector with proper types
            row_data = Any[hh_id - 1, period - 1]  # Integers for ID columns
            append!(row_data, solved_households[hh_id, period, :])  # Float64 for data columns
            push!(output_data, row_data)
        end
    end
    
    # Save to CSV
    output_file = "julia_households_results.csv"
    CSV.write(output_file, output_data, delim=",", writeheader=true)
    println("Results saved to $output_file")
end


# Run the simulation
#simulate_10000_households()
