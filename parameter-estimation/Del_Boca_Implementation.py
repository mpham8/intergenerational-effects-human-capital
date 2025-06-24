"""
Bijan Taheri (O'Connell Lab)
June 2025

This code is designed to implement something similar to Del Boca's method of simulated moments to estimate the model parameters

The code completes the following steps: 
1. Create thousands of households with a level of parent human capital, wage rate trajectories, and government input trajectories
2. Imports the latent factor estimates for the 
3. Use the equation for the simulated method of moments to find the moment estimator




Questions: 
- 
- 
"""

# Imports
import numpy as np
from scipy.optimize import minimize
import solve_system

# ---------------- CONSTANTS ------------------

# General
NUMBER_OF_HOUSEHOLDS = 10000
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


# Household-generation
MU_WAGE_RATE_GROWTH = 0.02
ETA_STD_WAGE_RATE_GROWTH = 0.05

# Solving the system
BETA = 0.935



# Initial guesses for parameters
parameters = [
    0.6, # theta_1
    0.35, # theta_2
    0.25, # theta_3
    0.15, # theta_4
    0.8, # rho
    # TODO: finish

]


# ----------------- FUNCTIONS ---------------------

def create_households(num_households: int) -> np.ndarray: 
    # 6 columns, 3 for inputs (parent HC, wage rate, and govt), and 4 for solver (leisure, consumption, investment, child HC)
    # TODO: actually implement
    
    # Households dimensions: number, period, household variable
    households = np.zeros((num_households, NUM_PERIODS, 7))

    # Parents' human capital
    parents_hc = np.random.normal(1, 0.2, (num_households, 1))
    print(households[:, 0].shape)
    households[:, :, 0] = parents_hc * np.ones((1, NUM_PERIODS))
    
    # Wage rate
    wage_rates = np.ones((num_households, ))
    households[:, 0, 1] = wage_rates
    for i in range(NUM_PERIODS): 
        wage_rates = (1+MU_WAGE_RATE_GROWTH)*wage_rates + np.random.normal(0, ETA_STD_WAGE_RATE_GROWTH, (num_households, ))
        households[:, i, 1] = wage_rates


    # Government inputs
    government_inputs = np.random.normal(1, 0.2, (num_households, NUM_PERIODS))
    households[:, :, 2] = government_inputs
    return households





def solve_households(households: np.ndarray) -> np.ndarray: 
    # TODO: actually implement
    for i in range(households.shape[0]):
        households[i, :, 3:] = solve_system.solve_household(1, 1, 1) # TODO: REPLACE
    return households






def main(): 
    households = create_households(NUMBER_OF_HOUSEHOLDS)
    for i in range(2): 
        print(f"Household {i}: {households[i]}")
        
    mean_characteristics = np.mean(households, axis=0)
    print(mean_characteristics)
    for i in range(households.shape[2]): 
        print(f"Here are the averages for {HOUSEHOLD_VARIABLES[i]} across periods: \n{mean_characteristics[:, i]}")
    


if __name__ == "__main__": 
    main()