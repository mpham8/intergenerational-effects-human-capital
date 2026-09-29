#!/bin/python3
"""
Bijan Taheri (O'Connell Lab)
Summer 2025

This code is designed to implement something similar to Del Boca's method of simulated moments to estimate the model parameters

The code completes the following steps: 
1. Create thousands of households with a level of parent human capital, wage rate trajectories, and government input trajectories
2. Import the latent factor estimates for the 
3. Use the equation for the simulated method of moments to find the moment estimator

8-12 processors
If already in queue: do below command to figure out how long it's going to take
squeue --start -j <jobid>


To test bash script (time): 
sbatch --test-only myscript.sh


salloc -t 60 --cpus-per-task=1 --mem-per-cpu=32gb --partition=unowned


Don't use base environment, create conda env
"""


# Imports
import numpy as np
import itertools
import time
from scipy.optimize import minimize
import matplotlib.pyplot as plt
import concurrent.futures
import os

import solve_system_test_3period as solve_system

# ---------------- CONSTANTS ------------------
# General
NUMBER_OF_HOUSEHOLDS = 1000
HOUSEHOLD_VARIABLES = [
    "Parent Human Capital", 
    "Wage Rate Trajectory", 
    "Government Inputs", 
    "Optimal Leisure", 
    "Optimal Consumption", 
    "Optimal Parental Investment", 
    "Child Human Capital Trajectory", 
]
NUM_PERIODS = 3
NUM_MOMENTS = 18
LATENT_FACTOR_FILEPATH = os.path.dirname(os.path.abspath(__file__)) + "/Attanasio Replication/6-26_estimation_results.xlsx"
PARAM_OUTPUT_FILEPATH = "parameter_estimates_SMM.txt"
METHOD_OPTIMIZATION = 'L-BFGS-B'

# Household-generation
MU_WAGE_RATE_GROWTH = 0.07
ETA_STD_WAGE_RATE_GROWTH = 0.02

# --------------- PARAMETERS ---------------
num_workers = 0  # Number of CPU cores to use, set to zero to use all available cores

# Actual parameters to estimate 

# List of parameters
PARAMETER_NAMES = [
    "Delta", 
    "Rho_val", 
    "Rho_e_val", 
    "theta_1", 
    "theta_2", 
    "theta_3", 
]
# Step sizes for grid search
STEP_SIZES = {
    "Delta": 50, 
    "Rho_val": 2, 
    "Rho_e_val": 2, 
    "theta_1" : 0.5, 
    "theta_2" : 0.5, 
    "theta_3" : 0.5, 

}
# Initial guesses for parameters (NOT APPLICABLE IN CURRENT CODE)
parameters = [
    12,  # Delta parameter
    -0.3, # rho
    -0.1, # rho_e
    0.35, # theta_1
    0.15, # theta_2
    0.25, # theta_3
# TODO: check if this is wrong (shouldn't I have different thetas over time?)
]
parameters_to_optimize = [20, -0.2,-0.2,0.3, 0.2, 0.2]
# Ranges for parameters
# rho (and rho_e): -5 to 0.5
# thetas: 0 to 1
# delta parameter: 0 to 100

times_elapsed = []

# TODO: look into Del Boca
# TODO: try different solvers for minimization
# TODO: look into JAX
# ----------------- FUNCTIONS ---------------------
def load_latent_factors(filepath): 
    # Random parameters, since we don't have any data
    # TODO: replace
    means_leisure = [2, 1, 2, 3]
    stds_leisure = [0.5, 0.2, 0.5, 0.5]
    means_parental_investment = [5, 4, 3, 2]
    stds_parental_investment = [1, 0.5, 0.3, 0.2]
    means_child_hc = [2, 3, 4, 5]
    stds_child_hc = [0.5, 1, 1.5, 2]

    return list(itertools.chain(means_leisure, means_parental_investment, means_child_hc, stds_leisure, stds_parental_investment, stds_child_hc))



def create_households(num_households: int) -> np.ndarray: 
    # 7 columns, 3 for inputs (parent HC, wage rate, and govt), and 4 for solver (consumption, leisure, investment, child HC)
    
    # Households dimensions: number, period, household variable
    households = np.zeros((num_households, NUM_PERIODS, 7))

    # Parents' human capital
    # TODO: substitute this with draws from an observed proxy for parents' human capital
    parents_hc = np.random.normal(1, 0.2, (num_households, 1))
    households[:, :, 0] = parents_hc * np.ones((1, NUM_PERIODS))
    
    # Wage rate
    wage_rates = np.ones((num_households, ))
    households[:, 0, 1] = wage_rates
    for i in range(1, NUM_PERIODS): 
        wage_rates = (1+MU_WAGE_RATE_GROWTH)*wage_rates + np.random.normal(0, ETA_STD_WAGE_RATE_GROWTH, (num_households, ))
        households[:, i, 1] = wage_rates


    # Government inputs
    # TODO: create 
    government_input_means = [0.0005, 0.019, 0.017]
    government_input_stds = [0.0001, 0.004, 0.003]
    for i in range(1, NUM_PERIODS): 
        households[:, i, 2] = np.random.normal(government_input_means[i], government_input_stds[i], (num_households, ))
    
    households[households < 0] = 0
    return households



def solve_one(args):
    parameters, hh = args
    # Solve for a single household
    solved = solve_system.solve_household(parameters, hh)
    return solved

def solve_households(households: np.ndarray, parameters: list, num_workers=0) -> np.ndarray:
    """
    Solves a numerical system for each household in parallel using multiple CPU cores.

    Parameters
    ----------
    households : np.ndarray
        A NumPy array representing the households to be solved. Each row corresponds to a household.
    parameters : list
        A list of parameters required for solving the system for each household.
    num_workers : int, optional
        Number of CPU cores to use. If None, uses all available cores.

    Returns
    -------
    np.ndarray
        The updated households array with the solution results assigned to the appropriate columns.
    """
    if num_workers <= 0:
        num_workers = os.cpu_count()

    args_iter = ((parameters, hh) for hh in households)
    with concurrent.futures.ProcessPoolExecutor(max_workers=num_workers) as executor:
        results = list(executor.map(solve_one, args_iter, chunksize=20))

    for i, solved in enumerate(results):
        households[i, :, 3:] = solved
    return households


def moments(households): 
    moments = np.zeros(NUM_MOMENTS)
    # The columns for each respective moment
    moment_leisure = 4
    moment_expenditure = 5
    moment_child_hc = 6
    for i in range(NUM_PERIODS): 
        moments[i] = np.mean(households[:, i, moment_leisure])
        moments[i+NUM_PERIODS] = np.mean(households[:, i, moment_expenditure]) 
        moments[i+2*NUM_PERIODS] = np.mean(households[:, i, moment_child_hc])
        moments[i+3*NUM_PERIODS] = np.std(households[:, i, moment_leisure])
        moments[i+4*NUM_PERIODS] = np.std(households[:, i, moment_expenditure]) 
        moments[i+5*NUM_PERIODS] = np.std(households[:, i, moment_child_hc])
    
    return np.array(moments)



# ----------------- TEST FUNCTION ---------------------

def simulate_10000_households():
    """Simulate 10,000 households, solve them, and save results to a CSV file."""
    # Number of households to simulate
    num_households = 5000
    
    # Parameters for solving (same as in tests)
    parameters = [20, -0.2, -0.2, 0.3, 0.2, 0.2]  # Delta, Rho_val, Rho_e_val, theta_1, theta_2, theta_3
    
    # Start timing
    start_time = time.time()
    
    # Create households
    print(f"Creating {num_households} households...")
    households = create_households(num_households)
    
    # Solve households
    print("Solving households...")
    solved_households = solve_households(households, parameters, num_workers=0)
    
    # Calculate elapsed time
    elapsed_time = time.time() - start_time
    print(f"Total execution time: {elapsed_time:.3f} seconds")
    
    # Prepare data for CSV
    # Reshape to (num_households * NUM_PERIODS, 9) for [Household ID, Period, 7 variables]
    num_periods = solved_households.shape[1]
    output_data = []
    for hh_id in range(num_households):
        for period in range(num_periods):
            row = [hh_id, period] + list(solved_households[hh_id, period, :])
            output_data.append(row)
    output_array = np.array(output_data)
    
    # Save to CSV
    output_file = "python_households_results.csv"
    header = "Household_ID,Period," + ",".join(HOUSEHOLD_VARIABLES)
    np.savetxt(
        output_file,
        output_array,
        delimiter=",",
        header=header,
        fmt=["%d", "%d"] + ["%.6f"] * 7,
        comments=""
    )
    print(f"Results saved to {output_file}")

# Run the simulation
if __name__ == "__main__":
    simulate_10000_households()
print("Available CPUs (os.cpu_count):", os.cpu_count())