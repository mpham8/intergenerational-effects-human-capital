#Del_Boca_Implementation functions that are used in adaptive metropolis translated into Julia, now using threads instead of pmap
#Also is currently using solve_system_test3 to more quickly solve households
#Uses number of threads equal to number of CPU cores divided by 4, assuming 4 chains in adaptive metropolis

# Imports
using Statistics
using Random
using Distributed
using CSV
using DataFrames
using .Threads
include("Julia_solve_system_3period.jl")

# ---------------- CONSTANTS ------------------
# General
const NUM_CHAINS = 1
const NUMBER_OF_HOUSEHOLDS = 10000
const HOUSEHOLD_VARIABLES = [
    "Parent Human Capital",
    "Wage Rate Trajectory",
    "Government Inputs",
    "Optimal Leisure",
    "Optimal Consumption",
    "Optimal Parental Investment",
    "Child Human Capital Trajectory"
]
const NUM_PERIODS = 3
const NUM_MOMENTS = 18
const LATENT_FACTOR_FILEPATH = joinpath(dirname(@__FILE__), "Attanasio Replication", "6-26_estimation_results.xlsx")
const PARAM_OUTPUT_FILEPATH = "parameter_estimates_SMM.txt"
const METHOD_OPTIMIZATION = "L-BFGS-B"

# Household-generation
const MU_WAGE_RATE_GROWTH = 0.07
const ETA_STD_WAGE_RATE_GROWTH = 0.02

# --------------- PARAMETERS ---------------
num_workers = 0  # Number of CPU cores to use, set to zero to use all available cores

# Actual parameters to estimate
const PARAMETER_NAMES = [
    "Delta",
    "Rho_val",
    "Rho_e_val",
    "theta_1",
    "theta_2",
    "theta_3"
]

# Initial guesses for parameters (NOT APPLICABLE IN CURRENT CODE)
const parameters = [
    12.0,   # Delta parameter
    -0.3,   # rho
    -0.1,   # rho_e
    0.35,   # theta_1
    0.15,   # theta_2
    0.25    # theta_3
]
const parameters_to_optimize = [20.0, -0.2, -0.2, 0.3, 0.2, 0.2]

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

function solve_one(args)::Array{Float64, 2}
    parameters, hh = args
    # Solve for a single household
    solved = solve_household(parameters, hh)  
    return solved
end

function solve_households(households::Array{Float64, 3}, parameters::Vector{Float64}, num_workers::Int=0)::Array{Float64, 3}
    """
    Solves a numerical system for each household in parallel using threads.

    Arguments
    ---------
    households : Array{Float64, 3}
        A 3D array representing the households to be solved. Each row corresponds to a household.
    parameters : Vector{Float64}
        A vector of parameters required for solving the system for each household.
    num_workers : Int, optional
        Number of threads to use. If 0, uses available threads divided by number of chains.

    Returns
    -------
    Array{Float64, 3}
        The updated households array with the solution results assigned to the appropriate columns.
    """
    if num_workers <= 0
        num_workers = max(1, Sys.CPU_THREADS ÷ NUM_CHAINS) # Divides by number of chains in adaptive metropolis
        # println("Number of workers: ", num_workers)
    end
    results = Vector{Array{Float64, 2}}(undef, size(households, 1))

    @threads :static for i in 1:size(households, 1)
        results[i] = solve_one((parameters, households[i, :, :]))
    end

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

function simulate_10000_households()
    """
    Simulate 10,000 households, solve them, and save results to a CSV file.
    """
    # Number of households to simulate
    num_households = 5000
    
    # Parameters for solving (same as in tests)
    parameters = [20.0, -0.2, -0.2, 0.3, 0.2, 0.2]  # Delta, Rho_val, Rho_e_val, theta_1, theta_2, theta_3
    
    # Start timing
    start_time = time()
    
    # Create households
    println("Creating $num_households households...")
    households = create_households(num_households)
    
    # Solve households (using all available workers)
    println("Solving households...")
    solved_households = solve_households(households, parameters, 0)
    
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
