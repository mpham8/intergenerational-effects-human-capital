using Random
using Statistics
using LinearAlgebra
using Optim
using Plots
using XLSX
using Distributed
include("Julia_solve_system.jl")


# ---------------- CONSTANTS ------------------
# General
const NUMBER_OF_HOUSEHOLDS = 2
const HOUSEHOLD_VARIABLES = [
    "Parent Human Capital",
    "Wage Rate Trajectory",
    "Government Inputs",
    "Optimal Leisure",
    "Optimal Consumption",
    "Optimal Parental Investment",
    "Child Human Capital Trajectory"
]
const NUM_PERIODS = 4
const NUM_MOMENTS = 24
const LATENT_FACTOR_FILEPATH = joinpath(dirname(@__FILE__), "Attanasio Replication", "6-26_estimation_results.xlsx")
const PARAM_OUTPUT_FILEPATH = "parameter_estimates_SMM.txt"
const METHOD_OPTIMIZATION = LBFGS()

# Household-generation
const MU_WAGE_RATE_GROWTH = 0.07
const ETA_STD_WAGE_RATE_GROWTH = 0.02

# --------------- PARAMETERS ---------------
num_workers = 0  # Number of max CPU cores to use, set to zero to use all available cores

# List of parameters
const PARAMETER_NAMES = [
    "Delta",
    "Rho_val",
    "Rho_e_val",
    "theta_1",
    "theta_2",
    "theta_3"
]

# Step sizes for grid search
const STEP_SIZES = Dict(
    "Delta" => 50.0,
    "Rho_val" => 2.0,
    "Rho_e_val" => 2.0,
    "theta_1" => 0.5,
    "theta_2" => 0.5,
    "theta_3" => 0.5
)

# Initial guesses for parameters (applicable if not using grid search)
parameters = [
    12.0,  # Delta parameter
    -0.3,  # rho
    -0.1,  # rho_e
    0.35,  # theta_1
    0.15,  # theta_2
    0.25   # theta_3
]

# Parameters to optimize (for empirical moments)
parameters_to_optimize = [20.0, -0.2, -0.2, 0.3, 0.2, 0.2]

times_elapsed = Float64[]

# ----------------- FUNCTIONS ---------------------

# A function to load the latent factors from a file
function load_latent_factors(filepath::String)
    # TODO: Implement Excel file reading
    @warn "No code exists for loading latent factors."
    return nothing
end

# A function to create the households, based on Michael's specification
function create_households(num_households::Int)::Array{Float64,3}
    households = zeros(num_households, NUM_PERIODS, 7)

    # Parents' human capital
    parents_hc = randn(num_households) * 0.2 .+ 1.0
    households[:, :, 1] .= reshape(parents_hc, num_households, 1)

    # Wage rate
    wage_rates = ones(num_households)
    households[:, 1, 2] = wage_rates
    for i in 2:NUM_PERIODS
        wage_rates = (1 + MU_WAGE_RATE_GROWTH) * wage_rates + randn(num_households) * ETA_STD_WAGE_RATE_GROWTH
        households[:, i, 2] = wage_rates
    end

    # Government inputs
    government_input_means = [0.0005, 0.019, 0.017, 0.013]
    government_input_stds = [0.0001, 0.004, 0.003, 0.002]
    for i in 1:NUM_PERIODS
        households[:, i, 3] = randn(num_households) * government_input_stds[i] .+ government_input_means[i]
    end

    households[households .< 0] .= 0
    return households
end

# A helper function to solve a single household
function solve_one(args)
    params, hh = args
    solved = solve_household(params, hh)
    return solved
end

# The function to solve all households in parallel
function solve_households(households::Array{Float64,3}, parameters::Vector{Float64}, num_workers::Int=0)::Array{Float64,3}
    if num_workers <= 0
        num_workers = nprocs()
    end

    args_iter = [(parameters, households[i, :, :]) for i in 1:size(households, 1)]
    results = pmap(solve_one, args_iter)

    for (i, solved) in enumerate(results)
        households[i, :, 4:end] = solved
    end
    return households
end

# A function to compute the moments from the households
function moments(households::Array{Float64,3})::Vector{Float64}
    moments = zeros(NUM_MOMENTS)
    moment_leisure = 4
    moment_expenditure = 5
    moment_child_hc = 6

    for i in 1:NUM_PERIODS
        moments[i] = mean(skipmissing(households[:, i, moment_leisure]))
        moments[i + NUM_PERIODS] = mean(skipmissing(households[:, i, moment_expenditure]))
        moments[i + 2 * NUM_PERIODS] = mean(skipmissing(households[:, i, moment_child_hc]))
        moments[i + 3 * NUM_PERIODS] = std(skipmissing(households[:, i, moment_leisure]))
        moments[i + 4 * NUM_PERIODS] = std(skipmissing(households[:, i, moment_expenditure]))
        moments[i + 5 * NUM_PERIODS] = std(skipmissing(households[:, i, moment_child_hc]))
    end
    return moments
end

# Helper function to check if a matrix is positive semi-definite (PSD)
function isPSD(A::Matrix{Float64}, tol::Float64=1e-8)::Bool
    E = eigvals(A)
    return all(E .> -tol)
end

# A function to compute the weighting matrix for the SMM
function compute_weighting_matrix(moment_list::Matrix{Float64})::Matrix{Float64}
    S = cov(moment_list, dims=1)
    final = pinv(S)
    if !isPSD(final)
        @warn "Covariance matrix is not positive semi-definite. This may cause errors"
    end
    return final
end

# A function to simulate moments based on the parameters and households
function simulate_moments(param_vec::Vector{Float64}, households::Array{Float64,3}, num_workers::Int=0)::Vector{Float64}
    households = solve_households(households, param_vec, num_workers)
    return moments(households)
end

# Objective function for the optimization
function objective(param_vec::Vector{Float64}, empirical::Vector{Float64}, weighting::Matrix{Float64}, households::Array{Float64,3}, num_workers::Int=0)::Float64
    ticker = time()
    simulated = simulate_moments(param_vec, households, num_workers)
    
    # Match Python: diff = (empirical - simulated) / (np.abs(empirical))
    diff = (empirical - simulated) ./ abs.(empirical)
    
    # Match Python: loss = diff.T @ weighting @ diff
    loss = dot(diff, weighting * diff)

    if loss < 0
        println("Warning: Loss is negative with a value of $loss. This may cause issues with optimization.")
    end
    println("Current parameters: ", param_vec)
    println("Current loss: ", loss)
    println("Time elapsed: ", time() - ticker)
    push!(times_elapsed, time() - ticker)
    println("Average time for simulation: ", mean(times_elapsed))
    return loss
end

# Function added for Julia to handle bounds manually in the objective function:
function bounded_objective(param_vec::Vector{Float64}, empirical::Vector{Float64}, weighting::Matrix{Float64}, households::Array{Float64,3}, bounds_lower::Vector{Float64}, bounds_upper::Vector{Float64}, num_workers::Int=0)::Float64
    # Check bounds and return large penalty if violated
    for i in 1:length(param_vec)
        if param_vec[i] < bounds_lower[i] || param_vec[i] > bounds_upper[i]
            return 1e10
        end
    end
    
    # Call original objective if within bounds
    return objective(param_vec, empirical, weighting, households, num_workers)
end

# A function to perform the two-step SMM
function two_step_smm(empirical::Vector{Float64}, initial_guess::Vector{Float64}, households::Array{Float64,3}, tolerances::Vector{Float64}=[0.01, 0.001], num_workers::Int=0)
    lower_bounds = [0.1, -4.0, -4.0, 0.05, 0.05, 0.05]
    upper_bounds = [100.0, 1.0, 1.0, 0.85, 0.85, 0.85]

    # Ensure initial guess is within bounds
    initial_guess_bounded = copy(initial_guess)
    for i in 1:length(initial_guess_bounded)
        initial_guess_bounded[i] = max(lower_bounds[i], min(upper_bounds[i], initial_guess_bounded[i]))
    end

    W1 = Matrix{Float64}(I, length(empirical), length(empirical))

    res1 = optimize(
        x -> bounded_objective(x, empirical, W1, households, lower_bounds, upper_bounds, num_workers),
        initial_guess_bounded,
        METHOD_OPTIMIZATION,
        Optim.Options(show_trace=true, g_tol=tolerances[1]);
        autodiff = :finite
    )

    if Optim.converged(res1)
        println("Successfully completed first step of SMM.")
        println("Full first optimizer message: ", res1)
        println("Initial guesses for parameters: ", Optim.minimizer(res1))
        theta_1 = Optim.minimizer(res1)

        sims = zeros(20, NUM_MOMENTS)
        for i in 1:20
            households_temp = create_households(NUMBER_OF_HOUSEHOLDS)
            sims[i, :] = simulate_moments(theta_1, households_temp, num_workers)
        end
        println("Computing more optimal weighting matrix...")
        W2 = compute_weighting_matrix(sims)

        println("Starting second step of SMM")
        res2 = optimize(
            x -> bounded_objective(x, empirical, W2, households, lower_bounds, upper_bounds, num_workers),
            theta_1,
            METHOD_OPTIMIZATION,
            Optim.Options(show_trace=true, g_tol=tolerances[2]);
            autodiff = :finite
        )

        if Optim.converged(res2)
            return Optim.minimizer(res2), Optim.minimum(res2)
        else
            println("Second step of two-step SMM failed for guess $initial_guess")
            return nothing, nothing
        end
    else
        println("First step of two-step SMM failed for guess $initial_guess")
        return nothing, nothing
    end
end

# A function to bootstrap confidence intervals for the parameter estimates
function bootstrap_confidence_intervals(empirical, initial_guess, B::Int=10, step_sizes::Dict=STEP_SIZES, tolerances::Vector{Float64}=[0.01, 0.001], grid_search::Bool=true)
    bootstrap_estimates = []
    for _ in 1:B
        idx = rand(1:NUMBER_OF_HOUSEHOLDS, NUMBER_OF_HOUSEHOLDS)
        households_empirical = create_households(NUMBER_OF_HOUSEHOLDS)[idx, :, :]
        households_empirical = solve_households(households_empirical, initial_guess)
        boot_empirical = moments(households_empirical)
        households_simulated = create_households(NUMBER_OF_HOUSEHOLDS)
        est, _ = grid_search ? grid_search_smm(boot_empirical, households_simulated, step_sizes, tolerances) :
                               two_step_smm(boot_empirical, initial_guess, households_simulated, tolerances)
        push!(bootstrap_estimates, est)
    end
    estimates = hcat(bootstrap_estimates...)
    lower = [quantile(estimates[i, :], 0.025) for i in 1:size(estimates, 1)]
    upper = [quantile(estimates[i, :], 0.975) for i in 1:size(estimates, 1)]
    return lower, upper
end

# A function to test the convergence of the parameter estimates
function convergence_test(empirical, parameters)
    household_sizes = [500, 1000, 2000, 5000, 10000]
    estimates = []
    for size in household_sizes
        households = create_households(size)
        theta, _ = two_step_smm(empirical, parameters, households)
        push!(estimates, theta)
    end
    estimates = hcat(estimates...)'
    p = plot()
    for (i, name) in enumerate(PARAMETER_NAMES)
        plot!(household_sizes, estimates[:, i], label=name)
    end
    xlabel!("Number of Households")
    ylabel!("Estimated Parameter")
    title!("Convergence of Parameter Estimates")
    savefig("Convergence_test.png")
end

# Helper function for grid search SMM
function run_smm(args)
    return two_step_smm(args...)
end

# A function to perform a grid search over initial guesses for parameters
function grid_search_smm(empirical::Vector{Float64}, households::Array{Float64,3}, step_sizes::Dict, tolerances::Vector{Float64})
    delta_range = 1.0:step_sizes["Delta"]:101.0
    rho_range = -5.0:step_sizes["Rho_val"]:0.5
    rho_e_range = -5.0:step_sizes["Rho_e_val"]:0.5
    theta_1_range = 0.1:step_sizes["theta_1"]:1.0
    theta_2_range = 0.1:step_sizes["theta_2"]:1.0
    theta_3_range = 0.1:step_sizes["theta_3"]:1.0

    initial_guesses = Vector{Float64}[]
    for rho in rho_range
        rho == 0 && continue
        for rho_e in rho_e_range
            rho_e == 0 && continue
            for theta_1 in theta_1_range
                for theta_2 in theta_2_range
                    for theta_3 in theta_3_range
                        theta_4 = 1 - (theta_1 + theta_2 + theta_3)
                        0 <= theta_4 <= 1 || continue
                        for delta in delta_range
                            push!(initial_guesses, [delta, rho, rho_e, theta_1, theta_2, theta_3])
                        end
                    end
                end
            end
        end
    end

    println("Number of initial guesses: ", length(initial_guesses))
    args_iter = [(empirical, initial_guess, households, tolerances) for initial_guess in initial_guesses]
    results = pmap(run_smm, args_iter)

    println("Multiprocessing complete. Best solution found")
    valid_results = [res for res in results if res != (nothing, nothing)]
    if isempty(valid_results)
        error("All two_step_smm calls failed. No valid results to process.")
    end

    params_list, obj_vals = zip(valid_results...)
    best_idx = argmin(obj_vals)
    best_params = params_list[best_idx]
    best_obj_val = obj_vals[best_idx]

    return best_params, best_obj_val
end

# A function to test the performance of the two-step SMM with varying numbers of processes
function performance_test(empirical, households, num_processes::Int)
    performance_times = Float64[]
    for i in 5:num_processes
        println("Number of CPUs used: $i")
        num_workers = i
        tic = time()
        param_estimates, obj_val = two_step_smm(empirical, parameters, households, [0.1, 0.1], num_workers)
        println("Estimated Parameters: ", param_estimates)
        println("Objective Function Value: ", obj_val)
        toc = time()
        println("Total time for two step simulation (in seconds): ", toc - tic)
        push!(performance_times, toc - tic)
    end

    println("Performance times for different numbers of processes:")
    for (i, time_taken) in enumerate(performance_times, 5)
        println("Processes: $i, Time taken: $time_taken seconds")
    end

    plot(5:num_processes, performance_times, marker=:circle, label="")
    xlabel!("Number of Processes")
    ylabel!("Time Taken (seconds)")
    title!("Performance of Two-Step SMM with Varying Processes")
    savefig("Performance_Test_Two_Step_SMM.png")
    return performance_times, nothing
end

# A function to determine the number of available CPUs
function available_cpu_count()
    return Sys.CPU_THREADS  # Returns number of logical CPU cores
end

# A function to compute the condition number of the Jacobian
function condition_number(param_vec::Vector{Float64}, epsilon::Float64=1e-5)::Float64
    base_moments = simulate_moments(param_vec)
    k = length(param_vec)
    n = length(base_moments)
    J = zeros(n, k)

    for j in 1:k
        perturbed = copy(param_vec)
        perturbed[j] += epsilon
        diff = simulate_moments(perturbed) - base_moments
        J[:, j] = diff / epsilon
    end

    cond = cond(J)
    println("Condition number of Jacobian: ", cond)
    return cond
end

# ------------------ MAIN FUNCTION ------------------
function main()
    tic = time()
    NUM_PROCESSES = available_cpu_count()
    println("Detected $NUM_PROCESSES CPUs available for parallel processing.")

    households_empirical = create_households(NUMBER_OF_HOUSEHOLDS)
    empirical = mean([simulate_moments(parameters_to_optimize, households_empirical) for _ in 1:2])
    println("Here are our empirical moments: ")
    println(empirical)

    open(PARAM_OUTPUT_FILEPATH, "w") do f
        write(f, "Parameters to optimize:\n")
        for i in 1:length(PARAMETER_NAMES)
            write(f, "$(PARAMETER_NAMES[i]): $(parameters_to_optimize[i])\n")
        end
    end

    println("Starting two-step SMM...")
    households_simulated = create_households(NUMBER_OF_HOUSEHOLDS)
    param_test, func = two_step_smm(empirical, parameters, households_simulated, [0.1, 0.1])
    println(param_test)
    println(func)

    open(PARAM_OUTPUT_FILEPATH, "a") do f
        write(f, "\n\n")
        write(f, "Parameter estimates from Simulated Method of Moments\n")
        for i in 1:length(PARAMETER_NAMES)
            write(f, "$(PARAMETER_NAMES[i]): $(param_test[i])\n")
        end
    end

    toc = time()
    println("Total time for two step simulation (in seconds): ", toc - tic)

    performance_test(empirical, households_simulated, 16)

    lower, upper = bootstrap_confidence_intervals(empirical, parameters, 10, STEP_SIZES, [0.1, 0.1], false)
    open(PARAM_OUTPUT_FILEPATH, "a") do f
        write(f, "Bootstrapped confidence intervals for Simulated Method of Moments\n")
        for i in 1:length(PARAMETER_NAMES)
            write(f, "$(PARAMETER_NAMES[i]): ($(lower[i]), $(upper[i]))\n")
        end
    end

    convergence_test(empirical, parameters)

    println("Starting grid search for best parameters...")
    tic = time()
    param_estimates, obj_val = grid_search_smm(empirical, households_simulated, STEP_SIZES, [0.05, 0.005])
    println("Estimated Parameters: ", param_estimates)
    println("Objective Function Value: ", obj_val)
    toc = time()
    println("Total time for grid search simulation (in seconds): ", toc - tic)
end

#if abspath(PROGRAM_FILE) == @__FILE__
#    main()
#end
println("Starting main() function...")
start_time = time()

try
    main()
    end_time = time()
    println("main() completed successfully in $(end_time - start_time) seconds")
catch e
    end_time = time()
    println("main() failed after $(end_time - start_time) seconds")
    println("Error: ", e)
    # Optionally print the full stack trace
    # println("Stack trace:")
    # showerror(stdout, e, catch_backtrace())
end