"""
Bijan Taheri (O'Connell Lab)
June 2025

This code is designed to implement something similar to Del Boca's method of simulated moments to estimate the model parameters

The code completes the following steps: 
1. Create thousands of households with a level of parent human capital, wage rate trajectories, and government input trajectories
2. Import the latent factor estimates for the 
3. Use the equation for the simulated method of moments to find the moment estimator




Questions: 
- 
- 
"""

# Imports
import numpy as np
import itertools
import time
from scipy.optimize import minimize
from scipy.optimize import Bounds
import solve_system
import matplotlib.pyplot as plt
import concurrent.futures
import os

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
NUM_PERIODS = 4
NUM_MOMENTS = 24
LATENT_FACTOR_FILEPATH = ""
PARAM_OUTPUT_FILEPATH = "parameter_estimates_SMM.txt"
METHOD_OPTIMIZATION = 'Nelder-Mead'
NUM_PROCESSES = os.cpu_count()

# Household-generation
MU_WAGE_RATE_GROWTH = 0.07
ETA_STD_WAGE_RATE_GROWTH = 0.02

# --------------- PARAMETERS ---------------


# Actual parameters to estimate 

# List of parameters
PARAMETER_NAMES = [
    "Delta", 
    "theta_1", 
    "theta_2", 
    "theta_3", 
    "theta_4", 
    "Rho_val", 
    "Rho_e_val", 
]
# Initial guesses for parameters
parameters = [
    0.8,  # Delta parameter
    0.6, # rho
    0.7, # rho_e
    0.4, # theta_1
    0.2, # theta_2
    0.1, # theta_3

]
times_elapsed = []
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
    # 6 columns, 3 for inputs (parent HC, wage rate, and govt), and 4 for solver (consumption, leisure, investment, child HC)
    
    # Households dimensions: number, period, household variable
    households = np.zeros((num_households, NUM_PERIODS, 7))

    # Parents' human capital
    parents_hc = np.random.normal(1, 0.2, (num_households, 1))
    households[:, :, 0] = parents_hc * np.ones((1, NUM_PERIODS))
    
    # Wage rate
    wage_rates = np.ones((num_households, ))
    households[:, 0, 1] = wage_rates
    for i in range(NUM_PERIODS): 
        wage_rates = (1+MU_WAGE_RATE_GROWTH)*wage_rates + np.random.normal(0, ETA_STD_WAGE_RATE_GROWTH, (num_households, ))
        households[:, i, 1] = wage_rates


    # Government inputs
    government_input_means = [0.0005, 0.019, 0.017, 0.013]
    government_input_stds = [0.0001, 0.004, 0.003, 0.002]
    for i in range(NUM_PERIODS): 
        households[:, i, 2] = np.random.normal(government_input_means[i], government_input_stds[i], (num_households, ))
    
    households[households < 0] = 0
    return households





def solve_one(args):
    parameters, hh = args
    # Solve for a single household
    solved = solve_system.solve_household(parameters, hh)
    return solved

def solve_households(households: np.ndarray, parameters: list) -> np.ndarray:
    """
    Solves a numerical system for each household in parallel using multiple CPU cores.

    Parameters
    ----------
    households : np.ndarray
        A NumPy array representing the households to be solved. Each row corresponds to a household.
    parameters : list
        A list of parameters required for solving the system for each household.

    Returns
    -------
    np.ndarray
        The updated households array with the solution results assigned to the appropriate columns.

    Notes
    -----
    This function uses `concurrent.futures.ProcessPoolExecutor` to parallelize the computation across available CPU cores.
    Each household is processed independently in a separate process.
    """

    args_iter = ((parameters, hh) for hh in households)
    with concurrent.futures.ProcessPoolExecutor() as executor:
        # Map each household to a process, using chunksize for efficiency
        results = list(executor.map(solve_one, args_iter, chunksize=20))

    # Assign results back to households array
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

def compute_weighting_matrix(moment_list: np.ndarray) -> np.ndarray:
    moment_list = np.atleast_2d(moment_list)
    S = np.cov(moment_list.T)
    return np.linalg.pinv(S)

def simulate_moments(param_vec: list) -> np.ndarray:
    # Creating the households
    households = create_households(NUMBER_OF_HOUSEHOLDS)
    households = solve_households(households, param_vec)
    return moments(households)

def objective(param_vec: list, empirical: np.ndarray, weighting: np.ndarray) -> float:
    ticker = time.perf_counter()
    simulated = simulate_moments(param_vec)
    diff = empirical - simulated
    loss = diff.T @ weighting @ diff
    print(f"Current parameters: {param_vec}")
    print(f"Current loss: {loss}")
    print(f"Time elapsed: {time.perf_counter() - ticker}")
    times_elapsed.append(time.perf_counter() - ticker)
    print(f"Average time for simulation: {np.average(times_elapsed)}")
    return loss

def two_step_smm(empirical: np.ndarray, initial_guess: list):

    # Constraints:
    # 1. 0 < rho (parameter[1]) < 1
    # 2. 0 < rho_e (parameter[2]) < 1
    # 3. theta_1 + theta_2 + theta_3 + theta_4 = 1 (parameters[3:7]) (built-in to system solver now)

    # Bounds for all parameters (None means no bound)
    lower_bounds = [-1e10, -1e10, -1e10, 0.0001, 0.0001, 0.0001]
    upper_bounds = [1e10, 1, 1, 1, 1, 1]
    bounds = Bounds(lower_bounds, upper_bounds)



    

    W1 = np.eye(len(empirical))
    res1 = minimize(
        objective,
        initial_guess,
        args=(empirical, W1),
        method=METHOD_OPTIMIZATION,
        bounds=bounds,
        options={'disp': True}
    )
    if res1.success:
        print("Successfully completed first step of SMM.")
        print("Full first optimizer message: ")
        print(res1)
        print("\nInitial guesses for parameters:")
        print(res1.x)
        theta_1 = res1.x
        sims = np.array([simulate_moments(theta_1) for _ in range(100)])
        print("Computing more optimal weighting matrix...")
        W2 = compute_weighting_matrix(sims)
        print("Starting second step of SMM")
        res2 = minimize(
            objective,
            theta_1,
            args=(empirical, W2),
            method=METHOD_OPTIMIZATION,
            bounds=bounds,
            options={'disp': True}
        )
        return res2.x, res2.fun
    else: 
        print("First step of SMM failed. Sadness")
        print(res1)
        print("We're going again")
        two_step_smm(empirical, np.random.uniform(0, 1, len(initial_guess)))


def bootstrap_confidence_intervals(empirical, initial_guess, B=10):
    bootstrap_estimates = []
    for _ in range(B):
        idx = np.random.choice(NUMBER_OF_HOUSEHOLDS, NUMBER_OF_HOUSEHOLDS, replace=True)
        households = create_households(NUMBER_OF_HOUSEHOLDS)[idx]
        households = solve_households(households, parameters)
        boot_empirical = moments(households)
        est, _ = two_step_smm(boot_empirical, initial_guess)
        bootstrap_estimates.append(est)
    estimates = np.array(bootstrap_estimates)
    lower = np.percentile(estimates, 2.5, axis=0)
    upper = np.percentile(estimates, 97.5, axis=0)
    return lower, upper


def convergence_test(empirical, parameters):
    household_sizes = [500, 1000, 2000, 5000, 10000]
    estimates = []
    global NUMBER_OF_HOUSEHOLDS
    for size in household_sizes:
        NUMBER_OF_HOUSEHOLDS = size
        theta, _ = two_step_smm(empirical, parameters)
        estimates.append(theta)
    estimates = np.array(estimates)
    for i, name in enumerate(PARAMETER_NAMES):
        plt.plot(household_sizes, estimates[:, i], label=name)
    plt.xlabel("Number of Households")
    plt.ylabel("Estimated Parameter")
    plt.title("Convergence of Parameter Estimates")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()



def main(): 
    tic = time.perf_counter()
    # Creating the households
    households = create_households(NUMBER_OF_HOUSEHOLDS)
    # Testing that they were created properly
    for i in range(2): 
        print(f"Household {i}: {households[i]}")
    
    # Gathering mean characteristics
    mean_characteristics = np.mean(households, axis=0)
    print(mean_characteristics)
    for i in range(households.shape[2]): 
        print(f"Here are the averages for {HOUSEHOLD_VARIABLES[i]} across periods: \n{mean_characteristics[:, i]}")


    # Running the simulation
    # empirical = load_latent_factors(LATENT_FACTOR_FILEPATH) # TODO: get the actual file and uncomment
    empirical = simulate_moments(
    [1.2,  # Delta parameter
    0.1, # rho
    0.1, # rho_e
    0.3, # theta_1
    0.4, # theta_2
    0.2])
    param_estimates, obj_val = two_step_smm(empirical, parameters)
    print("Estimated Parameters:", param_estimates)
    print("Objective Function Value:", obj_val)

    with open(PARAM_OUTPUT_FILEPATH, "w") as f:
        f.write("Parameter estimates from Simulated Method of Moments")
        for i in range(len(PARAMETER_NAMES)): 
            f.write(f"{PARAMETER_NAMES[i]}: {param_estimates[i]}")
    
    toc = time.perf_counter()
    print(f"Total time for two step simulation (in seconds): {toc - tic}")

    bootstrap_confidence_intervals(empirical, param_estimates)
    convergence_test(empirical, parameters)


    


if __name__ == "__main__": 
    main()