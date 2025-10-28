#=
Bijan Taheri (O'Connell Lab)
Summer 2025

This code runs an adaptive Metropolis algorithm, almost entirely based on the paper below: 
Haario, Heikki, et al. “An Adaptive Metropolis Algorithm.” Bernoulli, vol. 7, no. 2, 2001, pp. 223–42. JSTOR, https://doi.org/10.2307/3318737. Accessed 1 Aug. 2025.

The first section of the code defines a class for an adaptive Metropolis algorithm using SMM. 
The second section of the code has some helper functions used by the class. 
The final section of the code runs the adaptive Metropolis algorithm. 

The algorithm uses a two-step SMM in conjunction with adaptive Metropolis to estimate parameters for the DSGE. 

Here are some more resources to read in case you wanted to learn more: 
- A great explanation on SMM: https://opensourceecon.github.io/CompMethods/struct_est/SMM.html
- A presentation that touches on adaptive Metropolis: https://www2.stat.duke.edu/courses/Fall21/sta601.001/slides/09-adaptive-metropolis.html#42
- A paper providing rationale for 2.38^2/d as the scaling factor: Gareth O. Roberts. Jeffrey S. Rosenthal. "Optimal scaling for various Metropolis-Hastings algorithms." Statist. Sci. 16 (4) 351 - 367, November 2001. \\
https://doi.org/10.1214/ss/1015346320.

Dependencies: 
- numpy
- pandas
- matplotlib
- scipy

TODO: add the proper variables to reflect the changed equations (this also involves changing the "solve_system.py" function, or Julia equivalent)
- NOTE: this will also involve changing the number of periods and moments
TODO: implement "greedy" start suggested by Haario et al. (2001)?
TODO: implement other efficiency procedures in Haario et al. (2001)
TODO: implement more rigorous diagnostics (many of which are described here: https://www2.stat.duke.edu/courses/Fall21/sta601.001/slides/09-adaptive-metropolis.html#1)
TODO: ensure first-stage SMM is not used when calculating final parameter distributions
=#

using LinearAlgebra
using Statistics
using Random
using Distributions
using DataFrames
using XLSX
using Plots
using Serialization

# Import from implementation file
include("Julia_DelBoca_Implementation_Rev2.jl")

# Constants

# Model
const NUM_PERIODS = 3
const NUM_MOMENTS = 18
const NUM_PARAMETERS = 6
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
const INITIAL_PARAMS = [10, -0.3, -0.1, 0.4, 0.15, 0.1] # close, but not equal to 'empirical' params
const INITIAL_HOUSEHOLDS = 100 # number of households starting off
const MAX_HOUSEHOLDS = 10000 # maximum number of households reached
const HOUSEHOLD_INCREASE_THRESHOLD = 0.03 # when the number of households are increased
const ADAPTATION_START_TIME = 1000 # time after which adaptive covariance starts 
const EPSILON = 0.05 # used in calculating proposal covariance matrix. Should be small with respect to moments
const PROPOSAL_STRATEGY = "adaptive" # choose between 'adaptive' (purely adaptive), 'mixed' (custom distributions + some adaptivity), and 'custom' (purely custom)
# NOTE: I am unsure if the 'mixed' function is statistically rigorous. 'Adaptive' and 'custom' should be though
const STAGE1_ITERATIONS = 10000 # weighting matrix = identity
const STAGE2_ITERATIONS = 5000 # weighting matrix = inverse of covariance matrix of hat(theta_1)
const SAVE_PREFIX = "first_test_run_adaptive" # also ending of estimator diagnostics graph filename
const RANDOM_SEED = 42 # random seed to base the simulation off of. Select "nothing" to not have a set seed

# Proposal distributions
# For now, they are all truncated normal dists
# NOTE: not used under "adaptive" setting
const PROPOSAL_DISTS = Dict(
    "Delta" => Dict(
        "type" => "truncated_normal", 
        "scale" => 30, # scale = std
        "range" => (1, 100)
    ), 
    "rho_val" => Dict(
        "type" => "truncated_normal", 
        "scale" => 0.2,
        "range" => (-4, 1),
    ), 
    "rho_e_val" => Dict(
        "type" => "truncated_normal", 
        "scale" => 0.2, 
        "range" => (-4, 1)
    ), 
    "theta_1" => Dict(
        "type" => "truncated_normal", 
        "scale" => 0.05,
        "range" => (0, 1)
    ), 
    "theta_2" => Dict(
        "type" => "truncated_normal", 
        "scale" => 0.05,
        "range" => (0, 1)
    ), 
    "theta_3" => Dict(
        "type" => "truncated_normal", 
        "scale" => 0.05,
        "range" => (0, 1)
    )
)

# =============================================================
# DEFINING THE ADAPTIVE METROPOLIS STRUCT
# =============================================================
# 
# In Julia, we use a mutable struct instead of a class for the estimator.
mutable struct SMMAdaptiveMetropolis
    # Parameter information
    parameter_names::Vector{String}
    parameter_bounds::Vector{Tuple{Float64, Float64}}
    initial_params::Vector{Float64}
    
    # Data
    empirical_moments::Vector{Float64}
    
    # Simulation parameters
    num_initial_households::Int
    num_max_households::Int
    household_increase_threshold::Float64
    num_current_households::Int
    simulated_households::Any
    
    # Random state
    random_seed::Union{Int, Nothing}
    rng::AbstractRNG
    
    # Proposal distributions
    custom_proposal_dists::Dict{String, Dict{String, Any}}
    proposal_strategy::String
    
    # Chain storage
    chain::Vector{Vector{Float64}}
    accepted_chain::Vector{Vector{Float64}}
    smm_values::Vector{Float64}
    acceptance_history::Vector{Bool}
    household_count_history::Vector{Int}
    
    # Adaptation parameters
    adaptation_start::Int
    increase_household_spacer::Int
    epsilon::Float64
    current_iteration::Int
    n_accepted::Int
    covariance_matrix::Union{Matrix{Float64}, Nothing}
    mean_params::Union{Vector{Float64}, Nothing}
    
    # SMM components
    weighting_matrix::Matrix{Float64}
    moment_variances::Union{Vector{Float64}, Nothing}
    is_second_stage::Bool
    
    # Diagnostics
    parameter_std_history::Vector{Vector{Float64}}
    computation_times::Vector{Float64}
end

"""
Simulated Method of Moments estimator using Adaptive Metropolis-Hastings.
Implements two-step SMM with adaptive household count and comprehensive diagnostics.
"""
function SMMAdaptiveMetropolis(;
    parameter_names::Union{Vector{String}, Nothing} = nothing,
    parameter_bounds::Union{Vector{Tuple{Float64, Float64}}, Nothing} = nothing,
    initial_params::Union{Vector{Float64}, Nothing} = nothing,
    empirical_moments::Union{Vector{Float64}, Nothing} = nothing,
    initial_households::Int = 1000,
    max_households::Int = 10000,
    household_increase_threshold::Float64 = 0.05,
    adaptation_start_time::Int = 1000, 
    epsilon::Float64 = 0.01, 
    custom_proposal_dists::Union{Dict{String, Dict{String, Any}}, Nothing} = nothing, 
    proposal_strategy::String = "adaptive",
    random_seed::Union{Int, Nothing} = nothing
)
    """
    Initialize the SMM Adaptive Metropolis estimator.
    
    Parameters:
    -----------
    parameter_names : Vector{String}
        Names of parameters being estimated
    parameter_bounds : Vector{Tuple{Float64, Float64}}
        Lower and upper bounds for each parameter
    initial_params : Vector{Float64}
        Starting values for parameters
    empirical_moments : Vector{Float64}
        Observed moments to match (if nothing, will generate placeholder)
    initial_households : Int
        Starting number of households for simulation
    max_households : Int
        Maximum number of households
    household_increase_threshold : Float64
        Threshold for parameter std decrease to increase households
    adaptation_start : Int
        As described in Haario et al. (2001), the t_0 at which covariance adaptation for proposals begins
    epsilon: Float64
        Epsilon parameter in Haario et al. (2001). Should be small relative to parameters, ensuring our covariance matrix does not become singular
    random_seed : Union{Int, Nothing}
        Random seed for reproducibility
    """
    
    # Set default parameter names, bounds, and values
    param_names = something(parameter_names, ["Delta", "rho_val", "rho_e_val", "theta_1", "theta_2", "theta_3"])
    
    param_bounds = something(parameter_bounds, [
        # Below, it's set up to change when new parameters are changed
        (1.0, 100.0),      # Delta
        (-4.0, 1.0),     # rho_1 (right now rho_val)
        (-4.0, 1.0),     # rho_2 (right now rho_e_val)
        # (-4.0, 1.0).   # rho_3
        (0.0, 1.0),        # omega_1^e (right now theta_1)
        (0.0, 1.0),        # omega_1^g (right now theta_2)
        (0.0, 1.0),        # omega_1^h (right now theta_3)
        # (0.0, 1.0),        # omega_2^e
        # (0.0, 1.0),        # omega_2^g
        # (0.0, 1.0),        # omega_2^h
        # (0.0, 1.0),        # omega_3^e
        # (0.0, 1.0),        # omega_3^g
        # (0.0, 1.0),        # omega_3^h
    ])
    
    init_params = something(initial_params, [10, -0.29, -0.11, 0.41, 0.16, 0.11])
    
    # Initialize random state
    rng = if random_seed !== nothing 
        Random.seed!(random_seed)
        Random.MersenneTwister(random_seed)
    else
        Random.GLOBAL_RNG
    end
    
    # Initialize simulation
    num_current_households = initial_households
    simulated_households = create_households(initial_households)

    # Load or generate empirical moments
    # NOTE: once we have empirical moments, these lines need to be changed
    emp_moments = if empirical_moments !== nothing
        empirical_moments
    else
        generate_placeholder_empirical_moments(initial_households, rng)
    end

    # Load custom parameter distributions
    custom_dists = something(custom_proposal_dists, Dict{String, Dict{String, Any}}())
    
    if !isempty(custom_dists)
        validate_custom_proposals(custom_dists, param_names)
    end

    # Initialize chain storage
    chain = Vector{Vector{Float64}}()
    accepted_chain = Vector{Vector{Float64}}()
    smm_values = Vector{Float64}()
    acceptance_history = Vector{Bool}()
    household_count_history = Vector{Int}()
    
    # Initialize SMM components
    weighting_matrix = Matrix{Float64}(I, NUM_MOMENTS, NUM_MOMENTS)  # Start with identity
    
    # Initialize diagnostics
    parameter_std_history = Vector{Vector{Float64}}()
    computation_times = Vector{Float64}()
    
    estimator = SMMAdaptiveMetropolis(
        param_names,
        param_bounds,
        init_params,
        emp_moments,
        initial_households,
        max_households,
        household_increase_threshold,
        num_current_households,
        simulated_households,
        random_seed,
        rng,
        custom_dists,
        proposal_strategy,
        chain,
        accepted_chain,
        smm_values,
        acceptance_history,
        household_count_history,
        adaptation_start_time,
        0,  # increase_household_spacer
        epsilon,
        0,  # current_iteration
        0,  # n_accepted
        nothing,  # covariance_matrix
        nothing,  # mean_params
        weighting_matrix,
        nothing,  # moment_variances
        false,  # is_second_stage
        parameter_std_history,
        computation_times
    )
    
    println("Initialized SMM Adaptive Metropolis with $(length(param_names)) parameters")
    println("Starting with $(num_current_households) households")
    
    return estimator
end

function generate_placeholder_empirical_moments(initial_households::Int, rng::AbstractRNG)
    """Generate placeholder empirical moments using simulation."""
    println("Generating placeholder empirical moments...")
    # Parameters on which the empirical moments are based (i.e., any test runs should approximate something close to these parameters)
    parameters_to_optimize = [20.0, -0.2, -0.2, 0.3, 0.2, 0.2]
    
    # Averaging over multiple simulations of parameters
    empirical_moments_list = Vector{Vector{Float64}}()
    for _ in 1:2 # Increase the number of loops for greater accuracy of "empirical" parameters
        households_empirical = create_households(initial_households)
        solved_households = solve_households(copy(households_empirical), parameters_to_optimize)
        # print("Here are the empirical solved households: ")
        # println(solved_households)
        push!(empirical_moments_list, moments(solved_households))
    end
    empirical = mean(empirical_moments_list)
    
    println("Generated empirical moments successfully")
    return empirical
end

function load_empirical_moments(estimator::SMMAdaptiveMetropolis, filepath::String)
    """
    Placeholder function to load empirical moments from file.
    Replace this with your actual data loading logic.
    """
    # TODO: Implement actual moment loading
    # For now, return the generated moments
    println("TODO: Implement loading from $filepath")
    return estimator.empirical_moments
end

function check_parameter_constraints(estimator::SMMAdaptiveMetropolis, params::Vector{Float64})
    """Check if parameters satisfy bounds and theta sum constraint."""
    # Check individual bounds
    for (i, (param, (lower, upper))) in enumerate(zip(params, estimator.parameter_bounds))
        if param < lower || param > upper
            return false
        end
    end
        
    # Check theta sum constraint (theta_4 = 1 - sum(theta_1, theta_2, theta_3) >= 0)
    theta_sum = params[4] + params[5] + params[6]  # theta_1 + theta_2 + theta_3
    if theta_sum > 1.0
        return false
    end
    
    # TODO: when adding more parameters, add more constraints (sums for each period)
    return true
end

function simulate_moments(estimator::SMMAdaptiveMetropolis, params::Vector{Float64})
    """Simulate moments for given parameters."""
    households_sim = create_households(estimator.num_current_households)
    solved_households = solve_households(households_sim, params)
    return moments(solved_households)
end

function compute_smm_objective(estimator::SMMAdaptiveMetropolis, params::Vector{Float64})
    """Compute SMM objective function with standardized moments."""
    
    # Calculate simulated moments, using the same households every time
    # NOTE: using the same households every time is based on this article: https://opensourceecon.github.io/CompMethods/struct_est/SMM.html
    solved_households = solve_households(estimator.simulated_households, params)
    if solved_households == NaN
        println("WARNING: households were not solved properly")
    end
    simulated_moments = moments(solved_households)

    
    # Moment differences
    # NOTE: moments should always be POSITIVE. If moments are not positive, this line may not work
    # TO DEBUG NAN SMM OBJECTIVE - FIGURE OUT IF MEAN OR STD IS NAN FOR SOME VALUES
    moment_diff = (simulated_moments .- estimator.empirical_moments) ./ estimator.empirical_moments
    smm_obj = transpose(moment_diff) * estimator.weighting_matrix * moment_diff
    
    
    return smm_obj
end

function update_adaptive_covariance!(estimator::SMMAdaptiveMetropolis)
    """Update covariance matrix for adaptive proposals."""
    if length(estimator.accepted_chain) < estimator.adaptation_start
        return
    end
        
    # Convert to matrix
    # In Julia, we use hcat to convert vector of vectors to matrix, and transpose with '.
    chain_array = hcat(estimator.accepted_chain[end-estimator.adaptation_start+1:end]...)'
    
    # Compute empirical covariance
    # TODO: if this is a time-intensive step, use Haario et al. (2001)'s recursive formula for it
    estimator.mean_params = mean(chain_array, dims=1)[:]
    cov_matrix = cov(chain_array)
    
    # Adaptive Metropolis scaling
    # See Haario et al. (2001) and "Efficient Metropolis Jumping Rules" by Gelman et al. (1996) for more explanation
    d = length(estimator.parameter_names)
    scaling_parameter = (2.38^2) / d
    
    # Regularization for numerical stability
    # NOTE: estimator.covariance_matrix is equivalent to C_t in Haario et al. (2001)
    estimator.covariance_matrix = scaling_parameter * cov_matrix + scaling_parameter * estimator.epsilon * I
    
    # Verify positive definiteness
    if !is_positive_definite(estimator.covariance_matrix)
        println("Warning: Updated covariance matrix is not positive definite, adding extra regularization")
        estimator.covariance_matrix += estimator.epsilon * I
    end
end

# TODO: check this function in more detail and make sure it's in line with Haario et al. (2001)
function should_increase_households(estimator::SMMAdaptiveMetropolis)
    """Determine if household count should be increased."""
    if (estimator.num_current_households >= estimator.num_max_households || 
        length(estimator.parameter_std_history) < 5)
        return false
    end
        
    # If it's too soon since we last updated the households, return false
    if estimator.increase_household_spacer < 2  # i.e., less than 2 more parameter proposals have been accepted
        return false
    end
    
    # Check if parameter standard deviations have stabilized
    recent_stds = estimator.parameter_std_history[end-4:end]
    # Turning recent_stds from Vector{Vector{::Float64}} to an array (which is needed to do stds)
    recent_stds = hcat(recent_stds...)
    # Computing stds
    std_of_stds = std(recent_stds)
    
    mean_std_change = mean(std_of_stds)
    
    return mean_std_change < estimator.household_increase_threshold
end

function increase_household_count!(estimator::SMMAdaptiveMetropolis)
    """Increase the number of households for simulation."""
    # Calculating the number of new households
    old_count = estimator.num_current_households
    estimator.num_current_households = min(Int(estimator.num_current_households * 1.5), estimator.num_max_households)
    # Creating a new set of households
    # NOTE: we should probably just add to the old households, but this is easier logic-wise
    estimator.simulated_households = create_households(estimator.num_current_households)
    estimator.increase_household_spacer = 0
    println("Increased household count from $old_count to $(estimator.num_current_households)")
end

function compute_diagnostics(estimator::SMMAdaptiveMetropolis)
    """Compute chain diagnostics."""
    if length(estimator.accepted_chain) < 100
        return Dict{String, Any}()
    end
        
    chain_array = hcat(estimator.accepted_chain...)'
    diagnostics = Dict{String, Any}()
    
    # Acceptance rate
    diagnostics["acceptance_rate"] = estimator.n_accepted / max(1, estimator.current_iteration)
    
    # Effective sample size (simple autocorrelation-based estimate)
    ess = Vector{Float64}()
    for i in 1:size(chain_array, 2)
        param_chain = chain_array[:, i]
        param_centered = param_chain .- mean(param_chain)
        autocorr = conv(param_centered, reverse(param_centered))
        mid_point = length(autocorr) ÷ 2 + 1
        autocorr = autocorr[mid_point:end]
        autocorr = autocorr ./ autocorr[1]
        
        # Find first negative autocorrelation or use length
        tau = 1.0
        for j in 2:min(length(autocorr), length(param_chain)÷4)
            if autocorr[j] <= 0
                break
            end
            tau += 2 * autocorr[j]
        end
        
        push!(ess, length(param_chain) / (2 * tau))
    end
        
    diagnostics["effective_sample_size"] = ess
    
    # R-hat (simplified version - would need multiple chains for full implementation)
    diagnostics["rhat_available"] = false
    
    return diagnostics
end

function compute_optimal_weighting_matrix!(estimator::SMMAdaptiveMetropolis)
    """Compute optimal weighting matrix with robust PSD enforcement."""
    weighting_matrix, diagnostics, success = compute_optimal_weighting_matrix_robust(
        estimator.accepted_chain,
        params -> simulate_moments(estimator, params),
        NUM_MOMENTS,
        min_samples_multiplier=2,
        max_param_vectors=500,
        subsample_target=200
    )

    if success
        estimator.weighting_matrix = weighting_matrix
        # Save diagnostics if desired
        save_weighting_matrix_diagnostics(
            diagnostics,
            Matrix{Float64}(I, NUM_MOMENTS, NUM_MOMENTS),  # placeholder for original_cov
            Matrix{Float64}(I, NUM_MOMENTS, NUM_MOMENTS),  # placeholder for final_cov  
            weighting_matrix
        )
    else
        println("Using identity weighting matrix")
        estimator.weighting_matrix = Matrix{Float64}(I, NUM_MOMENTS, NUM_MOMENTS)
    end
end

function propose_parameters(estimator::SMMAdaptiveMetropolis, current_params::Vector{Float64})
    """Proposing parameters in our adaptive Metropolis algorithm"""
    if estimator.proposal_strategy == "custom"
        proposal = propose_custom_parameters(
            current_params, estimator.parameter_names, estimator.custom_proposal_dists,
            estimator.rng
        )
    elseif estimator.proposal_strategy == "mixed"
        proposal = propose_mixed_parameters(
            current_params, estimator.parameter_names, estimator.custom_proposal_dists,
            estimator.covariance_matrix, 
            use_adaptive=(estimator.covariance_matrix !== nothing && 
                        length(estimator.accepted_chain) > estimator.adaptation_start),
            estimator.rng
        )
    else  # "adaptive"
        proposal = propose_adaptive_parameters(
            current_params, estimator.parameter_names, estimator.custom_proposal_dists,
            estimator.covariance_matrix,
            use_adaptive=(estimator.covariance_matrix !== nothing && 
                        length(estimator.accepted_chain) > estimator.adaptation_start),
            estimator.rng
        )
    end
    return proposal
end

function run_mcmc!(estimator::SMMAdaptiveMetropolis; 
                   n_iterations::Int,
                   save_every::Int = 100,
                   save_prefix::String = "smm_mcmc")
    """
    Run the MCMC chain.
    
    Parameters:
    -----------
    n_iterations : Int
        Number of MCMC iterations
    save_every : Int
        Save checkpoint every N iterations
    save_prefix : String
        Prefix for save files
        
    Returns:
    --------
    Dict with results and diagnostics
    """
    
    println("Starting MCMC chain for $n_iterations iterations...")
    println("Stage: $(estimator.is_second_stage ? "Second (with optimal weights)" : "First (identity weights)")")
    
    # Initialize current parameters
    if isempty(estimator.chain)
        current_params = copy(estimator.initial_params)
        current_smm = compute_smm_objective(estimator, current_params)
        push!(estimator.chain, current_params)
        push!(estimator.accepted_chain, current_params)
        push!(estimator.smm_values, current_smm)
    else
        current_params = copy(estimator.chain[end])
        current_smm = estimator.smm_values[end]
    end
        
    start_time = time()
    
    for iteration in 1:n_iterations
        iter_start_time = time()
        estimator.current_iteration += 1
        
        # Propose new parameters
        proposal = propose_parameters(estimator, current_params)
            
        # Check constraints
        if !check_parameter_constraints(estimator, proposal)
            # Reject proposal
            push!(estimator.chain, current_params)
            push!(estimator.smm_values, current_smm)
            push!(estimator.acceptance_history, false)
        else
            # Evaluate SMM objective
            proposal_smm = compute_smm_objective(estimator, proposal)
            
            # Metropolis acceptance step (convert SMM to pseudo-likelihood)
            log_alpha = -0.5 * (proposal_smm - current_smm)
            alpha = min(1.0, exp(log_alpha))
            
            if rand(estimator.rng) < alpha
                # Accept proposal
                println("We finally accepted a proposal!")
                current_params = copy(proposal)
                current_smm = proposal_smm
                push!(estimator.accepted_chain, current_params)
                estimator.n_accepted += 1
                push!(estimator.acceptance_history, true)
                estimator.increase_household_spacer += 1
            else
                # Reject proposal
                push!(estimator.acceptance_history, false)
            end
                
            push!(estimator.chain, current_params)
            push!(estimator.smm_values, current_smm)
        end
            
        # Update adaptation
        if estimator.current_iteration > estimator.adaptation_start
            update_adaptive_covariance!(estimator)
        end
            
        # Track household count
        push!(estimator.household_count_history, estimator.num_current_households)
        
        # Track parameter standard deviations
        if length(estimator.accepted_chain) > 50
            recent_chain = hcat(estimator.accepted_chain[end-49:end]...)'
            param_stds = std(recent_chain, dims=1)[:]
            push!(estimator.parameter_std_history, param_stds)
            
            # Check if we should increase household count
            if should_increase_households(estimator)
                increase_household_count!(estimator)
            end
        end
            
        # Track computation time
        iter_time = time() - iter_start_time
        push!(estimator.computation_times, iter_time)
        
        # Progress reporting
        if iteration % 100 == 0
            acceptance_rate = estimator.n_accepted / estimator.current_iteration
            current_best_smm = minimum(estimator.smm_values)
            avg_time = mean(estimator.computation_times[max(1, end-99):end])
            
            println("--Iteration $(estimator.current_iteration): " *
                  "Accept Rate: $(round(acceptance_rate, digits=3)), " *
                  "Best SMM: $(round(current_best_smm, digits=6)), " *
                  "Households: $(estimator.num_current_households), " *
                  "Avg Time: $(round(avg_time, digits=3))s")
        end

        # Save checkpoint
        if iteration % save_every == 0
            save_checkpoint(estimator, save_prefix)
        end
    end
        
    total_time = time() - start_time
    println("MCMC completed in $(round(total_time, digits=2)) seconds")
    
    # Final diagnostics
    diagnostics = compute_diagnostics(estimator)
    
    return Dict(
        "chain" => hcat(estimator.chain...)',
        "accepted_chain" => hcat(estimator.accepted_chain...)',
        "smm_values" => estimator.smm_values,
        "diagnostics" => diagnostics,
        "total_time" => total_time
    )
end

function run_two_step_smm!(estimator::SMMAdaptiveMetropolis;
                          stage1_iterations::Int = 20000,
                          stage2_iterations::Int = 10000,
                          save_every::Int = 100,
                          save_prefix::String = "two_step_smm")
    """
    Run two-step SMM procedure.
    
    Stage 1: Identity weighting matrix
    Stage 2: Optimal weighting matrix computed from Stage 1
    """
    
    println("="^60)
    println("STARTING TWO-STEP SMM PROCEDURE")
    println("="^60)
    
    # Stage 1: Identity weighting matrix
    println("\nSTAGE 1: Running with identity weighting matrix...")
    estimator.is_second_stage = false
    estimator.weighting_matrix = Matrix{Float64}(I, NUM_MOMENTS, NUM_MOMENTS)
    
    stage1_results = run_mcmc!(estimator,
        n_iterations=stage1_iterations,
        save_every=save_every,
        save_prefix="$(save_prefix)_stage1"
    )
    
    # Compute optimal weighting matrix
    println("\nComputing optimal weighting matrix...")
    compute_optimal_weighting_matrix!(estimator)
    
    # Stage 2: Optimal weighting matrix
    println("\nSTAGE 2: Running with optimal weighting matrix...")
    estimator.is_second_stage = true
    
    # Reset some adaptation parameters for stage 2
    stage2_start_params = get_parameter_estimates(estimator)["mean"]
    estimator.initial_params = stage2_start_params
    
    stage2_results = run_mcmc!(estimator,
        n_iterations=stage2_iterations,
        save_prefix="$(save_prefix)_stage2"
    )
    
    println("\n" * "="^60)
    println("TWO-STEP SMM COMPLETED")
    println("="^60)
    
    return Dict(
        "stage1_results" => stage1_results,
        "stage2_results" => stage2_results,
        "final_estimates" => get_parameter_estimates(estimator)
    )
end
            
function get_parameter_estimates(estimator::SMMAdaptiveMetropolis)
    """Get parameter estimates and standard errors."""
        
    if isempty(estimator.accepted_chain)
        return Dict{String, Any}()
    end
    
    chain_array = hcat(estimator.accepted_chain...)'
    
    # Remove burnin (first 20% of samples)
    burnin = max(100, size(chain_array, 1) ÷ 5)
    post_burnin = chain_array[burnin+1:end, :]
    if size(post_burnin, 1) < 100 
        println("Too few results to get accurate estimates.")
        return Dict{String, Any}()
    end
    
    estimates = Dict{String, Any}()
    for (i, name) in enumerate(estimator.parameter_names)
        estimates[name] = Dict(
            "mean" => mean(post_burnin[:, i]),
            "std" => std(post_burnin[:, i]),
            "quantile_025" => quantile(post_burnin[:, i], 0.025),
            "quantile_975" => quantile(post_burnin[:, i], 0.975)
        )
    end
        
    # Overall summary
    estimates["mean"] = mean(post_burnin, dims=1)[:]
    estimates["std"] = std(post_burnin, dims=1)[:]
    estimates["n_samples"] = size(post_burnin, 1)
    
    return estimates
end

function save_checkpoint(estimator::SMMAdaptiveMetropolis, prefix::String = "smm_checkpoint")
    """Save current state for resuming later."""
    
    # Save heavy data to serialization
    pickle_data = Dict(
        "chain" => estimator.chain,
        "accepted_chain" => estimator.accepted_chain,
        "smm_values" => estimator.smm_values,
        "covariance_matrix" => estimator.covariance_matrix,
        "weighting_matrix" => estimator.weighting_matrix,
        "empirical_moments" => estimator.empirical_moments,
        "parameter_std_history" => estimator.parameter_std_history,
        "computation_times" => estimator.computation_times
    )
    
    open("$(prefix)_data.jls", "w") do f
        serialize(f, pickle_data)
    end
        
    # Save important scalars and diagnostics to Excel
    diagnostics = compute_diagnostics(estimator)
    
    # Create summary DataFrame
    summary_data = DataFrame(
        Metric = ["Current Iteration", "Accepted Samples", "Current Households", 
                  "Acceptance Rate", "Is Second Stage", "Best SMM Value",
                  "Average Computation Time", "Random Seed"],
        Value = [estimator.current_iteration, estimator.n_accepted, estimator.num_current_households,
                 estimator.n_accepted / max(1, estimator.current_iteration), estimator.is_second_stage,
                 isempty(estimator.smm_values) ? NaN : minimum(estimator.smm_values),
                 isempty(estimator.computation_times) ? NaN : mean(estimator.computation_times[max(1, end-99):end]),
                 estimator.random_seed]
    )
    
    # Parameter estimates
    estimates = get_parameter_estimates(estimator)
    if !isempty(estimates)
        param_data = DataFrame()
        param_data.Parameter = String[]
        param_data.Mean = Float64[]
        param_data.Std = Float64[]
        param_data.Q2_5 = Float64[]
        param_data.Q97_5 = Float64[]
        
        for name in estimator.parameter_names
            if haskey(estimates, name)
                push!(param_data.Parameter, name)
                push!(param_data.Mean, estimates[name]["mean"])
                push!(param_data.Std, estimates[name]["std"])
                push!(param_data.Q2_5, estimates[name]["quantile_025"])
                push!(param_data.Q97_5, estimates[name]["quantile_975"])
            end
        end
    else
        param_data = DataFrame()
    end
        
    # Save to Excel, conditionally including Parameter_Estimates using pairs
    sheets = ["Summary" => summary_data]
    if size(param_data, 2) > 0
        push!(sheets, "Parameter_Estimates" => param_data)
    end
    XLSX.writetable("$(prefix)_summary.xlsx", sheets...; overwrite=true)
            
    println("Checkpoint saved: $(prefix)_data.jls and $(prefix)_summary.xlsx")
end

function load_checkpoint!(estimator::SMMAdaptiveMetropolis, prefix::String = "smm_checkpoint")
    """Load previous state to resume estimation."""
    
    # Load serialized data
    try
        pickle_data = open("$(prefix)_data.jls", "r") do f
            deserialize(f)
        end
            
        estimator.chain = pickle_data["chain"]
        estimator.accepted_chain = pickle_data["accepted_chain"]
        estimator.smm_values = pickle_data["smm_values"]
        estimator.covariance_matrix = pickle_data["covariance_matrix"]
        estimator.weighting_matrix = pickle_data["weighting_matrix"]
        estimator.empirical_moments = pickle_data["empirical_moments"]
        estimator.parameter_std_history = pickle_data["parameter_std_history"]
        estimator.computation_times = pickle_data["computation_times"]
        
        # Reconstruct other variables
        estimator.current_iteration = length(estimator.chain)
        estimator.n_accepted = length(estimator.accepted_chain)

        # Verify loaded covariance matrix is positive definite
        if estimator.covariance_matrix !== nothing && !is_positive_definite(estimator.covariance_matrix)
            println("Warning: Loaded covariance matrix is not positive definite, reinitializing")
            d = length(estimator.parameter_names)
            scaling_parameter = (2.38^2) / d
            initial_variances = Float64[]
            for (i, param_name) in enumerate(estimator.parameter_names)
                if haskey(estimator.custom_proposal_dists, param_name) && haskey(estimator.custom_proposal_dists[param_name], "scale")
                    variance = estimator.custom_proposal_dists[param_name]["scale"]^2
                else
                    lower, upper = estimator.parameter_bounds[i]
                    variance = ((upper - lower) * 0.01)^2
                end
                push!(initial_variances, variance)
            end
            estimator.covariance_matrix = scaling_parameter * Diagonal(initial_variances)
        end
        
        println("Checkpoint loaded: $(prefix)_data.jls")
        println("Resumed at iteration $(estimator.current_iteration) with $(estimator.n_accepted) accepted samples")
        
    catch e
        if isa(e, SystemError)
            println("Checkpoint file $(prefix)_data.jls not found")
        else
            rethrow(e)
        end
    end
end

function plot_diagnostics(estimator::SMMAdaptiveMetropolis; save_path::Union{String, Nothing} = nothing)
    """Plot chain diagnostics."""
    if length(estimator.accepted_chain) < 10
        println("Not enough samples for plotting")
        return
    end
        
    chain_array = hcat(estimator.accepted_chain...)'
    n_params = length(estimator.parameter_names)
    
    plots_array = []
    
    for (i, name) in enumerate(estimator.parameter_names)
        # Trace plot
        p1 = plot(chain_array[:, i], title="$name - Trace Plot", 
                 xlabel="Iteration", ylabel="Value")
        
        # Histogram
        p2 = histogram(chain_array[:, i], bins=50, alpha=0.7, 
                      title="$name - Posterior Distribution",
                      xlabel="Value", ylabel="Frequency")
        
        push!(plots_array, p1, p2)
    end
    
    final_plot = plot(plots_array..., layout=(n_params, 2), size=(1200, 300*n_params))
    
    if save_path !== nothing
        savefig(final_plot, save_path)
        println("Diagnostics plot saved: $save_path")
    else
        display(final_plot)
    end
end

# HELPER FUNCTIONS
# =============================================================================
# =============================================================================

function is_positive_definite(matrix::Union{Matrix{Float64}, Nothing})
    """Check if a matrix is positive definite."""
    if matrix === nothing
        return false
    end
    try
        eigenvals = eigvals(matrix)
        return all(eigenvals .> 0)
    catch e
        return false
    end
end

# Helper function for convolution (since Julia doesn't have scipy.signal.convolve)
function conv(u::Vector{Float64}, v::Vector{Float64})
    """Simple convolution implementation."""
    m, n = length(u), length(v)
    w = zeros(m + n - 1)
    for i in 1:m
        for j in 1:n
            w[i + j - 1] += u[i] * v[j]
        end
    end
    return w
end

# =============================================================================
# WEIGHTING MATRIX DIAGNOSTICS FUNCTIONS
# =============================================================================

function compute_optimal_weighting_matrix_robust(accepted_chain::Vector{Vector{Float64}}, 
                                                simulate_moments_func::Function,
                                                num_moments::Int = 24,
                                                min_samples_multiplier::Int = 2,
                                                max_param_vectors::Int = 500,
                                                subsample_target::Int = 200)
    """
    Compute optimal weighting matrix with robust PSD enforcement.
    
    Parameters:
    -----------
    accepted_chain : Vector{Vector{Float64}}
        List of accepted parameter vectors from Stage 1
    simulate_moments_func : Callable
        Function that takes parameters and returns simulated moments
    num_moments : int
        Number of moment conditions
    min_samples_multiplier : int
        Minimum samples = min_samples_multiplier * num_moments
    max_param_vectors : int
        Maximum parameter vectors to use
    subsample_target : int
        Target number of parameter vectors to actually use
        
    Returns:
    --------
    tuple: (weighting_matrix, diagnostics_dict, success_flag)
    """
    
    min_samples = max(100, min_samples_multiplier * num_moments)
    
    if length(accepted_chain) < min_samples
        println("Warning: Need at least $min_samples samples, have $(length(accepted_chain))")
        println("Using identity weighting matrix")
        return Matrix{Float64}(I, num_moments, num_moments), Dict{String, Any}(), false
    end
        
    println("Computing moment variance-covariance matrix...")
    
    # Use recent accepted parameters
    n_params_to_use = min(max_param_vectors, length(accepted_chain))
    recent_params = accepted_chain[end-n_params_to_use+1:end]
    
    # Subsample to target number
    subsample_rate = max(1, length(recent_params) ÷ subsample_target)
    moment_sims = Vector{Vector{Float64}}()
    
    println("Using $(length(recent_params)) parameter vectors with subsample rate $subsample_rate")
    
    for i in 1:subsample_rate:length(recent_params)
        if (i-1) ÷ subsample_rate % 20 == 0
            println("  Computing moments for parameter vector $((i-1)÷subsample_rate + 1)/$(length(recent_params)÷subsample_rate)")
        end
        params = recent_params[i]
        sim_moments = simulate_moments_func(params)
        push!(moment_sims, sim_moments)
    end
        
    moment_sims_matrix = hcat(moment_sims...)'
    println("Computed $(size(moment_sims_matrix, 1)) moment vectors")
    
    # Compute variance-covariance matrix
    moment_cov = cov(moment_sims_matrix)
    
    # Enforce PSD and compute diagnostics
    weighting_matrix, diagnostics = enforce_positive_semidefinite(moment_cov, num_moments)
    
    return weighting_matrix, diagnostics, true
end

function enforce_positive_semidefinite(moment_cov::Matrix{Float64}, 
                                     num_moments::Int;
                                     min_eigenval::Float64 = 1e-8,
                                     diagonal_reg::Float64 = 1e-6)
    """
    Enforce positive semi-definiteness and compute weighting matrix.
    
    Parameters:
    -----------
    moment_cov : np.ndarray
        Moment covariance matrix
    num_moments : int
        Number of moments
    min_eigenval : float
        Minimum allowed eigenvalue
    diagonal_reg : float
        Additional diagonal regularization
        
    Returns:
    --------
    tuple: (weighting_matrix, diagnostics_dict)
    """
    
    println("Checking positive semi-definiteness...")
    eigenvals, eigenvecs = eigen(moment_cov)
    
    println("Eigenvalue range: [$(minimum(eigenvals)):.2e, $(maximum(eigenvals)):.2e]")
    
    # Count problematic eigenvalues
    negative_eigs = sum(eigenvals .< 0)
    near_zero_eigs = sum(abs.(eigenvals) .< 1e-12)
    
    if negative_eigs > 0
        println("Warning: $negative_eigs negative eigenvalues detected")
    end
    if near_zero_eigs > 0
        println("Warning: $near_zero_eigs near-zero eigenvalues detected (< 1e-12)")
    end
        
    # Eigenvalue regularization
    regularized_eigenvals = max.(eigenvals, min_eigenval)
    
    # Reconstruct PSD matrix
    moment_cov_psd = eigenvecs * Diagonal(regularized_eigenvals) * eigenvecs'
    
    # Additional diagonal regularization
    diagonal_reg_matrix = diagonal_reg * I
    moment_cov_final = moment_cov_psd + diagonal_reg_matrix
    
    # Verify final matrix is PSD
    final_eigenvals = eigvals(moment_cov_final)
    min_final_eig = minimum(final_eigenvals)
    
    if min_final_eig > 0
        println("Matrix is positive definite (min eigenvalue: $(min_final_eig):.2e)")
    else
        println("Warning: Matrix still not PSD (min eigenvalue: $(min_final_eig):.2e)")
    end
        
    # Compute condition number
    condition_number = maximum(final_eigenvals) / minimum(final_eigenvals)
    println("Condition number: $(condition_number):.2e")
    
    if condition_number > 1e12
        println("Warning: Matrix is ill-conditioned, results may be unreliable")
    end
        
    # Compute weighting matrix
    try
        weighting_matrix = inv(moment_cov_final)
        
        # Verify weighting matrix is also PSD
        w_eigenvals = eigvals(weighting_matrix)
        if minimum(w_eigenvals) > 0
            println("Optimal weighting matrix computed successfully and is positive definite")
        else
            println("Warning: Weighting matrix is not positive definite")
        end
            
    catch e
        println("Error inverting moment covariance: $e")
        println("Using identity weighting matrix")
        weighting_matrix = Matrix{Float64}(I, num_moments, num_moments)
    end
        
    # Compile diagnostics
    diagnostics = Dict{String, Any}(
        "original_eigenvalues" => eigenvals,
        "regularized_eigenvalues" => regularized_eigenvals,
        "final_eigenvalues" => final_eigenvals,
        "condition_number_original" => any(eigenvals .!= 0) ? maximum(eigenvals) / maximum(abs.(eigenvals[eigenvals .!= 0])) : Inf,
        "condition_number_final" => condition_number,
        "negative_eigenvalues" => negative_eigs,
        "near_zero_eigenvalues" => near_zero_eigs,
        "min_eigenvalue_original" => minimum(eigenvals),
        "min_eigenvalue_final" => min_final_eig
    )
    
    return weighting_matrix, diagnostics
end

function save_weighting_matrix_diagnostics(diagnostics::Dict{String, Any},
                                         original_cov::Matrix{Float64},
                                         final_cov::Matrix{Float64},
                                         weighting_matrix::Matrix{Float64};
                                         filename::String = "weighting_matrix_diagnostics.jls")
    """Save detailed weighting matrix diagnostics."""
    
    diagnostic_data = Dict(
        "diagnostics" => diagnostics,
        "original_covariance" => original_cov,
        "final_covariance" => final_cov,
        "weighting_matrix" => weighting_matrix
    )
    
    open(filename, "w") do f
        serialize(f, diagnostic_data)
    end
        
    println("Weighting matrix diagnostics saved to '$filename'")
end

# =============================================================================
# CUSTOM PROPOSAL FUNCTIONS
# =============================================================================

function validate_custom_proposals(custom_proposal_dists::Dict{String, Dict{String, Any}}, 
                                  parameter_names::Vector{String})
    """Validate custom proposal distribution specifications."""
    
    for (param_name, dist_spec) in custom_proposal_dists
        if !(param_name in parameter_names)
            throw(ArgumentError("Custom proposal specified for unknown parameter: $param_name"))
        end
        
        required_keys = ["type"]
        if !all(key in keys(dist_spec) for key in required_keys)
            throw(ArgumentError("Custom proposal for $param_name missing required keys: $required_keys"))
        end
        
        # Validate specific distribution types
        dist_type = dist_spec["type"]
        if dist_type == "normal"
            if !("std" in keys(dist_spec))
                throw(ArgumentError("Normal proposal for $param_name requires 'std' parameter"))
            end
        elseif dist_type == "lognormal"
            if !("sigma" in keys(dist_spec))
                throw(ArgumentError("Lognormal proposal for $param_name requires 'sigma' parameter"))
            end
        elseif dist_type == "beta"
            if !all(key in keys(dist_spec) for key in ["alpha", "beta", "loc", "scale"])
                throw(ArgumentError("Beta proposal for $param_name requires 'alpha', 'beta', 'loc', 'scale'"))
            end
        elseif dist_type == "truncated_normal"
            if !all(key in keys(dist_spec) for key in ["scale", "range"])
                throw(ArgumentError("Truncated normal proposal for $param_name requires 'scale', 'range'"))
            end
        elseif dist_type == "custom_function"
            if !("sampler" in keys(dist_spec))
                throw(ArgumentError("Custom function proposal for $param_name requires 'sampler' function"))
            end
        else 
            throw(ArgumentError("Unknown proposal distribution type: $dist_type"))
        end
    end
end

function propose_single_parameter(param_idx::Int, 
                                 current_value::Float64,
                                 parameter_names::Vector{String},
                                 custom_proposal_dists::Dict{String, Dict{String, Any}},
                                 rng::AbstractRNG;
                                 adaptive_cov_diagonal::Union{Vector{Float64}, Nothing} = nothing,
                                 default_scale::Float64 = 0.1)
    """
    Propose new value for a single parameter using custom or adaptive distribution.
    
    Parameters:
    -----------
    param_idx : int
        Index of parameter
    current_value : float
        Current parameter value
    parameter_names : List[str]
        List of parameter names
    custom_proposal_dists : Dict[str, Dict]
        Custom proposal specifications
    adaptive_cov_diagonal : np.ndarray, optional
        Diagonal of adaptive covariance matrix
    default_scale : float
        Default proposal scale
        
    Returns:
    --------
    float: Proposed parameter value
    """
    
    param_name = parameter_names[param_idx]
    
    # Check if we have a custom proposal for this parameter
    if haskey(custom_proposal_dists, param_name)
        dist_spec = custom_proposal_dists[param_name]
        dist_type = dist_spec["type"]
        
        if dist_type == "normal"
            std_val = dist_spec["std"]
            proposal = current_value + std_val * randn(rng)
            
        elseif dist_type == "lognormal"
            sigma = dist_spec["sigma"]
            # Sample in log space, then transform
            log_current = log(max(current_value, 1e-10))  # Avoid log(0)
            log_proposal = log_current + sigma * randn(rng)
            proposal = exp(log_proposal)
            
        elseif dist_type == "beta"
            alpha, beta_param = dist_spec["alpha"], dist_spec["beta"]
            loc, scale = dist_spec["loc"], dist_spec["scale"]
            # Sample from beta and transform to desired range
            beta_sample = rand(rng, Beta(alpha, beta_param))
            proposal = loc + scale * beta_sample
            
        elseif dist_type == "truncated_normal"
            scale = dist_spec["scale"]
            lower, upper = dist_spec["range"]
            d = Truncated(Normal(current_value, scale), lower, upper)
            proposal = rand(rng, d)

        elseif dist_type == "uniform" 
            lower, upper = dist_spec["range"]
            proposal = lower + (upper - lower) * rand(rng)
            
            
        elseif dist_type == "custom_function"
            sampler = dist_spec["sampler"]
            kwargs = get(dist_spec, "kwargs", Dict())
            proposal = sampler(current_value; kwargs...)
    
        else
            # Fallback to normal
            proposal = current_value + default_scale * randn(rng)
        end
            
    else
        # Use adaptive covariance if available
        if adaptive_cov_diagonal !== nothing
            var = adaptive_cov_diagonal[param_idx]
            proposal = current_value + sqrt(max(var, 1e-8)) * randn(rng)
        else
            # Default normal proposal
            proposal = current_value + default_scale * randn(rng)
        end
    end
            
    return proposal
end

function propose_custom_parameters(current_params::Vector{Float64},
                                 parameter_names::Vector{String},
                                 custom_proposal_dists::Dict{String, Dict{String, Any}},
                                 rng::AbstractRNG)
    """Propose new parameters using only custom distributions."""
    
    proposal = zeros(length(current_params))
    
    for (i, current_val) in enumerate(current_params)
        proposal[i] = propose_single_parameter(
            i, current_val, parameter_names, custom_proposal_dists, rng
        )
    end
        
    return proposal
end

function propose_mixed_parameters(current_params::Vector{Float64},
                                 parameter_names::Vector{String},
                                 custom_proposal_dists::Dict{String, Dict{String, Any}},
                                 covariance_matrix::Union{Nothing, Matrix{Float64}},
                                 rng::AbstractRNG; 
                                 use_adaptive::Bool=false)
    """Mix custom proposals with adaptive covariance where available."""
    
    proposal = zeros(length(current_params))
    
    if use_adaptive && covariance_matrix !== nothing
        # Start with full adaptive proposal
        adaptive_proposal = rand(rng, MvNormal(current_params, covariance_matrix))
        
        # Override with custom proposals where specified
        for (i, param_name) in enumerate(parameter_names)
            if haskey(custom_proposal_dists, param_name)
                proposal[i] = propose_single_parameter(
                    i, current_params[i], parameter_names, custom_proposal_dists, rng
                )
            else
                proposal[i] = adaptive_proposal[i]
            end
        end
    else
        # Use individual parameter proposals
        adaptive_cov_diagonal = nothing
        if covariance_matrix !== nothing
            adaptive_cov_diagonal = diag(covariance_matrix)
        end
            
        for (i, current_val) in enumerate(current_params)
            proposal[i] = propose_single_parameter(
                i, current_val, parameter_names, custom_proposal_dists, rng,
                adaptive_cov_diagonal=adaptive_cov_diagonal
            )
        end
    end
    return proposal
end

function propose_adaptive_parameters(current_params::Vector{Float64},
                                   parameter_names::Vector{String},
                                   custom_proposal_dists::Dict{String, Dict{String, Any}},
                                   covariance_matrix::Union{Nothing, Matrix{Float64}},
                                   rng::AbstractRNG; 
                                   use_adaptive::Bool=false)
    """Standard adaptive proposal with custom overrides."""
    
    if use_adaptive && covariance_matrix !== nothing
        # Use adaptive covariance as base
        base_proposal = rand(rng, MvNormal(current_params, covariance_matrix))
        
        # Override specific parameters with custom proposals
        for (i, param_name) in enumerate(parameter_names)
            if haskey(custom_proposal_dists, param_name)
                base_proposal[i] = propose_single_parameter(
                    i, current_params[i], parameter_names, custom_proposal_dists, rng
                )
            end
        end
        return base_proposal
    else
        # Early phase: use individual proposals
        proposal = zeros(length(current_params))
        adaptive_cov_diagonal = nothing
        if covariance_matrix !== nothing
            adaptive_cov_diagonal = diag(covariance_matrix)
        end
            
        for (i, current_val) in enumerate(current_params)
            proposal[i] = propose_single_parameter(
                i, current_val, parameter_names, custom_proposal_dists, rng,
                adaptive_cov_diagonal=adaptive_cov_diagonal
            )
        end
        return proposal
    end
end

# =============================================================================
# =============================================================================
# =============================================================================
# MAIN FUNCTION
# =============================================================================
# =============================================================================
# =============================================================================

function main()
    sleep(1)  # Small delay to ensure print order in some environments
    
    # Initialize estimator
    estimator = SMMAdaptiveMetropolis(
        initial_params=INITIAL_PARAMS,
        random_seed=RANDOM_SEED, 
        initial_households=INITIAL_HOUSEHOLDS, 
        max_households=MAX_HOUSEHOLDS,
        household_increase_threshold=HOUSEHOLD_INCREASE_THRESHOLD, 
        adaptation_start_time=ADAPTATION_START_TIME, 
        epsilon=EPSILON, 
        proposal_strategy=PROPOSAL_STRATEGY,
        custom_proposal_dists=PROPOSAL_DISTS,
    )

    # Run some experiments
    unsolved_households = create_households(100)
    for i = 1:5
        println("Household ", i, ": ", repr("text/plain", unsolved_households[i, :, :]))
    end
    # Testing household solver with a variety of parameters
    for i = 1:10
        parameters_random = propose_custom_parameters(
            INITIAL_PARAMS,
            estimator.parameter_names,
            estimator.custom_proposal_dists,
            estimator.rng
        )
        households = solve_households(unsolved_households, parameters_random)

        # Print a random household
        println("Parameter Set ", i, ": ", repr("text/plain", parameters_random))
        for j = 1:2
            household_idx = rand(1:size(households, 1))
            println("  Household ", household_idx, ": ", repr("text/plain", households[household_idx, :, :]))
        end
    end
    households = solve_households(unsolved_households, INITIAL_PARAMS)

    # for i = 1:5
    #     println("Household ", i, ": ", repr("text/plain", households[i, :, :]))
    # end
    exit()

    println("MEANS OF HOUSEHOLDS BY COLUMN:")
    means = mean(households, dims=1)  # means is 1 x NUM_PERIODS x NUM_PARAMETERS
    means = dropdims(means, dims=1)   # now NUM_PERIODS x NUM_PARAMETERS
    for i = 1:7
        println(HOUSEHOLD_COLUMN_NAMES[i], ": ", means[:, i])
    end
    
        
    # Run two-step SMM
    results = run_two_step_smm!(estimator,
        stage1_iterations=STAGE1_ITERATIONS,  # Reduced for example
        stage2_iterations=STAGE2_ITERATIONS,   # Reduced for example
        save_prefix=SAVE_PREFIX
    )
    
    # Get final estimates
    final_estimates = get_parameter_estimates(estimator)
    println("\nFinal Parameter Estimates:")
    println("="^50)
    for name in estimator.parameter_names
        if haskey(final_estimates, name)
            est = final_estimates[name]
            println("$(rpad(name, 12)): $(rpad(round(est["mean"], digits=4), 8)) ± $(rpad(round(est["std"], digits=4), 6)) " *
                  "[$(rpad(round(est["quantile_025"], digits=4), 7)), $(round(est["quantile_975"], digits=4))]")
        end
    end
    
    # Plot diagnostics
    plot_diagnostics(estimator, save_path="parameter-estimation/Del_Boca_Replication/estimator_diagnostics_$(SAVE_PREFIX).png")
    
    println("\nEstimation completed!")
    println("Total accepted samples: $(length(estimator.accepted_chain))")
    println("Final acceptance rate: $(round(estimator.n_accepted / estimator.current_iteration, digits=3))")
end

# Run main function if this is the main script
main()