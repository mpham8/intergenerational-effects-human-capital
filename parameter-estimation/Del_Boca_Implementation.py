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
import solve_system

# ---------------- CONSTANTS ------------------
# General
NUMBER_OF_HOUSEHOLDS = 100
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
    0.8, # rho
    0.9, # rho_e
    0.6, # theta_1
    0.35, # theta_2
    0.25, # theta_3
    0.15, # theta_4
    # TODO: finish

]


# ----------------- FUNCTIONS ---------------------
def load_latent_factors(filepath): 
    # Random parameters, since we don't have any data
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
    parents_hc[parents_hc < 0] = 0
    print(households[:, 0].shape)
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


    return households





def solve_households(households: np.ndarray, parameters: list) -> np.ndarray: 
    # TODO: actually implement
    for i in range(households.shape[0]):
        households[i, :, 3:] = solve_system.solve_household(parameters, households[i]) # TODO: REPLACE
    return households


def empirical_moments(households): 
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
    households = create_households(NUMBER_OF_HOUSEHOLDS)
    households = solve_households(households, param_vec)
    return empirical_moments(households)

def objective(param_vec: list, empirical: np.ndarray, weighting: np.ndarray) -> float:
    simulated = simulate_moments(param_vec)
    diff = empirical - simulated
    loss = diff.T @ weighting @ diff
    print(f"Current loss: {loss}")
    return loss

def two_step_smm(empirical: np.ndarray, initial_guess: list):
    W1 = np.eye(len(empirical))
    res1 = minimize(objective, initial_guess, args=(empirical, W1), method='Nelder-Mead')
    theta_1 = res1.x
    sims = np.array([simulate_moments(theta_1) for _ in range(100)])
    W2 = compute_weighting_matrix(sims)
    res2 = minimize(objective, theta_1, args=(empirical, W2), method='Nelder-Mead')
    return res2.x, res2.fun



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
    empirical = empirical_moments(households)
    param_estimates, obj_val = two_step_smm(empirical, parameters)
    print("Estimated Parameters:", param_estimates)
    print("Objective Function Value:", obj_val)

    with open(PARAM_OUTPUT_FILEPATH, "w") as f:
        f.write("Parameter estimates from Simulated Method of Moments")
        for i in range(len(PARAMETER_NAMES)): 
            f.write(f"{PARAMETER_NAMES[i]}: {param_estimates[i]}")
    
    toc = time.perf_counter()
    print(f"Total time for program (in seconds): {toc - tic}")

    


if __name__ == "__main__": 
    main()