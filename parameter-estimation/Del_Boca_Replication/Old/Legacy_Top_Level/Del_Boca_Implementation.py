#!/bin/bash
"""
Bijan Taheri (O'Connell Lab)
Summer 2025

This code is designed to implement something similar to Del Boca's method of simulated moments to estimate the model parameters

The code completes the following steps: 
1. Import (or for now, create) the latent factor moments
2. Create thousands of households with a level of parent human capital, wage rate trajectories, and government input trajectories
3. Use the equation for the simulated method of moments to find the best parameters

Currently, there is code to work with both two-step SMM and grid search SMM: 
- Two-step SMM should work IF SMM minimzer can cover whole parameter space
- Otherwise, we'll need to use a grid search SMM (or adaptive Metropolis, since grid search SMM is infeasible with our current solver)



NOTES FOR USING FIREBIRD: 
To test when submitted bash script will run: 
squeue --start -j <jobid>

To test bash script BEFORE submission (time): 
sbatch --test-only myscript.sh

To allocate a node to play around with Firebird a bit
salloc -t 60 --cpus-per-task=1 --mem-per-cpu=32gb --partition=unowned

Don't use base environment, create conda env





Dependencies: 
- numpy
- scipy
- matplotlib (for convergence estimates)
"""
# TODO: figure out exactly why solve_system.py is failing at some points (i.e., failing to find a solution)
# TODO: see if we can edit Nelder-Mead simplex to cover entire parameter space
# TODO: look into Del Boca code and see how they make SMM so efficient (is it just different equations, or something else?)
# TODO: try different solvers for minimization

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
import re
import subprocess

# ---------------- CONSTANTS ------------------
# General

# Number of households to create/use in SMM
NUMBER_OF_HOUSEHOLDS = 1000
# Names of each household column / what they refer to
HOUSEHOLD_VARIABLES = [
    "Parent Human Capital", 
    "Wage Rate Trajectory", 
    "Government Inputs", 
    "Optimal Leisure", 
    "Optimal Consumption", 
    "Optimal Parental Investment", 
    "Child Human Capital Trajectory", 
]
# Number of periods in the model
NUM_PERIODS = 4
# Number of moments used in the SMM
# 6 moments per period: mean and std of leisure, expenditure, and child human capital
# Four periods total => 24 moments total
NUM_MOMENTS = 24
# Filepath to the latent factor estimates
LATENT_FACTOR_FILEPATH = os.path.dirname(os.path.abspath(__file__)) + "/Attanasio Replication/6-26_estimation_results.xlsx"
# Filepath to the output file for parameter estimates
PARAM_OUTPUT_FILEPATH = "parameter_estimates_SMM.txt"
# Optimization method for the SMM
# Del Boca uses Nelder-Mead, but we're trying out different solvers
# See https://docs.scipy.org/doc/scipy/reference/generated/scipy.optimize.minimize.html for more information
METHOD_OPTIMIZATION = 'L-BFGS-B'

# Household-generation
# These refer to variables in the model (see Michael's parameter estimation write-up for more information)
# Also, Michael has a document named "parameters" that may be useful
MU_WAGE_RATE_GROWTH = 0.07
ETA_STD_WAGE_RATE_GROWTH = 0.02

# --------------- PARAMETERS ---------------
num_workers = 0  # Number of max CPU cores to use, set to zero to use all available cores

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
# Initial guesses for parameters (applicable if not using grid search)
parameters = [
    12,  # Delta parameter
    -0.3, # rho
    -0.1, # rho_e
    0.35, # theta_1
    0.15, # theta_2
    0.25, # theta_3
    # TODO: change these parameters to reflect new equations (Michael has yet to add them to the code)
]

# The parameters that create the 'empirical' moments
# Right now, since we don't have latent factor moments, we are creating households, solving them for the following parameters, and then using the moments from those households as the empirical moments
parameters_to_optimize = [20, -0.2,-0.2,0.3, 0.2, 0.2]

# Ranges for parameters (applicable to grid search)
# rho (and rho_e): -5 to 0.5
# thetas: 0 to 1
# delta parameter: 0 to 100

times_elapsed = []



# ----------------- FUNCTIONS ---------------------

# A function to load the latent factors from a file
def load_latent_factors(filepath): 
    # TODO: write this function
    print("Warning: no code exists for loading latent factors.")
    return


# A function to create the households, based on Michael's specification
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
    # TODO: change this in line with what Michael decides (since government inputs should be correlated with wage draws)
    government_input_means = [0.0005, 0.019, 0.017, 0.013]
    government_input_stds = [0.0001, 0.004, 0.003, 0.002]
    for i in range(NUM_PERIODS): 
        households[:, i, 2] = np.random.normal(government_input_means[i], government_input_stds[i], (num_households, ))
    
    households[households < 0] = 0
    return households




# A helper function to solve a single household in parallel
def solve_one(args):
    parameters, hh = args
    # Solve for a single household
    solved = solve_system.solve_household(parameters, hh)
    return solved

# The function to solve all households in parallel
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

# A function to compute the moments from the households
def moments(households): 
    moments = np.zeros(NUM_MOMENTS)
    # The columns for each respective moment
    moment_leisure = 4
    moment_expenditure = 5
    moment_child_hc = 6
    # Looping through all periods, adding the moments in a particular order
    for i in range(NUM_PERIODS): 
        moments[i] = np.nanmean(households[:, i, moment_leisure])
        moments[i+NUM_PERIODS] = np.nanmean(households[:, i, moment_expenditure]) 
        moments[i+2*NUM_PERIODS] = np.nanmean(households[:, i, moment_child_hc])
        moments[i+3*NUM_PERIODS] = np.nanstd(households[:, i, moment_leisure])
        moments[i+4*NUM_PERIODS] = np.nanstd(households[:, i, moment_expenditure]) 
        moments[i+5*NUM_PERIODS] = np.nanstd(households[:, i, moment_child_hc])
    
    return np.array(moments)

# Helper function to check if a matrix is positive semi-definite (PSD)
# (I'm getting errors for which a potential cause would be the covariance matrix not being PSD)
def isPSD(A, tol=1e-8):
  E = np.linalg.eigvalsh(A)
  return np.all(E > -tol)

# A function to compute the weighting matrix for the SMM
# NOTE: this is changed in the adaptive metropolis (check to see if that works)
def compute_weighting_matrix(moment_list: np.ndarray) -> np.ndarray:
    moment_list = np.atleast_2d(moment_list)
    S = np.cov(moment_list.T)
    final = np.linalg.pinv(S)
    if not isPSD(final):
        print("Warning: Covariance matrix is not positive semi-definite. This may cause errors")
    
    return final

# A function to simulate moments based on the parameters and households
def simulate_moments(param_vec: list, households: np.ndarray, num_workers=0) -> np.ndarray:
    # Solving the households and returning the moments
    households = solve_households(households, param_vec, num_workers)
    return moments(households)

# Objective function for the optimization
def objective(param_vec: list, empirical: np.ndarray, weighting: np.ndarray, households: np.ndarray, num_workers=0) -> float:# 
    ticker = time.perf_counter()
    simulated = simulate_moments(param_vec, households, num_workers)
    # Error is the percent difference from simulated, not absolute difference (to ensure weighting isn't affected by units)
    diff = (empirical - simulated) / (np.abs(empirical))
    # Computing the loss based on SMM protocol
    loss = diff.T @ weighting @ diff

    # Various print statements for debugging
    if loss < 0: 
        print(f"Warning: Loss is negative with a value of {loss}. This may cause issues with optimization.")
    print(f"Current parameters: {param_vec}")
    print(f"Current loss: {loss}")
    print(f"Time elapsed: {time.perf_counter() - ticker}")
    times_elapsed.append(time.perf_counter() - ticker)
    print(f"Average time for simulation: {np.average(times_elapsed)}")
    return loss

# A function to perform the two-step SMM
def two_step_smm(empirical: np.ndarray, initial_guess, households: np.ndarray, tolerances=[0.01, 0.001], num_workers=0):
    # Hard constraints:
    # 1. rho (parameter[1]) < 1
    # 2. rho_e (parameter[2]) < 1
    # 3. theta_1 + theta_2 + theta_3 + theta_4 = 1 (parameters[3:7]) (built-in to system solver now)


    # Soft bounds: 
    # 0.1 < Delta < 100
    # -5 < rho < 0.5
    # -5 < rho_e < 0.5
    # 0.05 < theta_1, theta_2, theta_3 < 1

    # Bounds for all parameters (None means no bound)
    lower_bounds = [0.1, -4, -4, 0.05, 0.05, 0.05]
    upper_bounds = [100, 1, 1, 0.85, 0.85, 0.85]
    bounds = Bounds(lower_bounds, upper_bounds)

    # print(f"Running two-step SMM with guess {np.ndarray(initial_guess)}")

    # Using the identity weighting matrix for first stage
    W1 = np.eye(len(empirical))

    # Solving for the first-stage SMM
    res1 = minimize(
        objective,
        initial_guess,
        args=(empirical, W1, households, num_workers),
        method=METHOD_OPTIMIZATION,
        bounds=bounds,
        options={'disp': True}, 
        tol=tolerances[0]
    )
    # If successful, move onto second
    if res1.success:
        print("Successfully completed first step of SMM.")
        print("Full first optimizer message: ")
        print(res1)
        print("\nInitial guesses for parameters:")
        print(res1.x)
        theta_1 = res1.x # To avoid confusion: in this case, theta represents the vector of parameters, and theta_1 is the first-stage parameters (it's common notation in the field)

        # Calculating second-stage weighting matrix
        sims = np.ndarray((20, NUM_MOMENTS))
        for i in range(20): 
            households = create_households(NUMBER_OF_HOUSEHOLDS)
            sims[i] = simulate_moments(theta_1, households, num_workers)
        print("Computing more optimal weighting matrix...")
        W2 = compute_weighting_matrix(sims)

        # Starting the second step of SMM
        print("Starting second step of SMM")
        res2 = minimize(
            objective,
            theta_1,
            args=(empirical, W2, households, num_workers),
            method=METHOD_OPTIMIZATION,
            bounds=bounds,
            options={'disp': True},
            tol=tolerances[1]
        )
        if res2.success: 
            return res2.x, res2.fun
        else: 
            print(f"Second step of two-step SMM failed for guess {initial_guess}")
            return None, None
    else: 
        print(f"First step of two-step SMM failed for guess {initial_guess}")
        return None, None


# A function to bootstrap confidence intervals for the parameter estimates
# TODO: make sure this is right (HAS NOT BEEN CHECKED)
def bootstrap_confidence_intervals(empirical, initial_guess, B=10, step_sizes=STEP_SIZES, tolerances=[0.01, 0.001], grid_search=True):
    bootstrap_estimates = []
    for _ in range(B):
        idx = np.random.choice(NUMBER_OF_HOUSEHOLDS, NUMBER_OF_HOUSEHOLDS, replace=True)
        households_empirical = create_households(NUMBER_OF_HOUSEHOLDS)[idx]
        households_empirical = solve_households(households_empirical, initial_guess)
        boot_empirical = moments(households_empirical)
        households_simulated = create_households(NUMBER_OF_HOUSEHOLDS)
        if grid_search: 
            est, _ = grid_search_smm(boot_empirical, households_simulated, step_sizes, tolerances)
        else: 
            est, _ = two_step_smm(boot_empirical, initial_guess, households_simulated, tolerances)
        bootstrap_estimates.append(est)
    estimates = np.array(bootstrap_estimates)
    lower = np.percentile(estimates, 2.5, axis=0)
    upper = np.percentile(estimates, 97.5, axis=0)
    return lower, upper

# A function to test the convergence of the parameter estimates as the number of households increases
# TODO: test (HAS NOT BEEN CHECKED)
def convergence_test(empirical, parameters):
    household_sizes = [500, 1000, 2000, 5000, 10000]
    estimates = []
    for size in household_sizes:
        households = create_households(size)
        theta, _ = two_step_smm(empirical, parameters, households)
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
    plt.savefig("Convergence_test.png")
    plt.clf()
    plt.cla()
    plt.close()


# Helper function (to run grid search SMM)
def run_smm(args):
    return two_step_smm(*args)

# A function to perform a grid search over initial guesses for parameters and find the global minimum.
def grid_search_smm(empirical: np.ndarray, households: np.ndarray, step_sizes: dict, tolerances: list):
    """
    Performs a grid search over initial guesses for parameters and finds the global minimum.
    step_sizes: dict with keys matching PARAMETER_NAMES and values as step sizes.
    Returns best_params, best_obj_val
    """
    
    delta_range = np.arange(1.0, 101+step_sizes["Delta"], step_sizes["Delta"])
    rho_range = np.arange(-5.0, 0.5+step_sizes["Rho_val"], step_sizes["Rho_val"])
    rho_e_range = np.arange(-5.0, 0.5+step_sizes["Rho_e_val"], step_sizes["Rho_e_val"])
    theta_1_range = np.arange(0.1, 1.0+step_sizes["theta_1"], step_sizes["theta_1"])
    theta_2_range = np.arange(0.1, 1.0+step_sizes["theta_2"], step_sizes["theta_2"])
    theta_3_range = np.arange(0.1, 1.0+step_sizes["theta_3"], step_sizes["theta_3"])

    # theta_4 is determined by 1 - (theta_1 + theta_2 + theta_3)
    
    # Get valid initial guesses
    initial_guesses = []
    for rho in rho_range:
        if rho == 0: 
            continue # TODO: specify a small value for rho? See if that works?
        for rho_e in rho_e_range:
            if rho_e == 0: 
                continue
            for theta_1 in theta_1_range:
                for theta_2 in theta_2_range:
                    for theta_3 in theta_3_range:
                        theta_4 = 1 - (theta_1 + theta_2 + theta_3)
                        if not 0 <= theta_4 <= 1:
                            continue
                        for delta in delta_range:
                            initial_guesses.append([delta, rho, rho_e, theta_1, theta_2, theta_3])
    
    print(f"Number of initial guesses: {len(initial_guesses)}")
    args_iter = [(empirical, initial_guess, households, tolerances) for initial_guess in initial_guesses]
    with concurrent.futures.ProcessPoolExecutor() as executor:
        results = list(executor.map(run_smm, args_iter))
    
    print("Multiprocessing complete. Best solution found")
    params_list, obj_vals = zip(*results)
    # Filter out invalid results (None, None)
    valid_results = [res for res in results if res != (None, None)]
    if not valid_results:
        raise ValueError("All two_step_smm calls failed. No valid results to process.")
    
    params_list, obj_vals = zip(*valid_results)
    obj_vals = np.array(obj_vals)
    best_idx = np.argmin(obj_vals)
    best_params = np.array(params_list[best_idx])
    best_obj_val = obj_vals[best_idx]

    return best_params, best_obj_val

# A function to test the performance of the two-step SMM with varying numbers of processes
def performance_test(empirical, households, num_processes): 
    performance_times = []
    for i in range(5, num_processes): 
        
        # Printing the number of CPUs I'm using in the analysis
        print(f"Number of CPUs used: {i}")
        num_workers = i
        # Running the simulation

        # If 
        # empirical = load_latent_factors(LATENT_FACTOR_FILEPATH) # TODO: get the actual file and uncomment
        tic = time.perf_counter()
        param_estimates, obj_val = two_step_smm(empirical, parameters, households, num_workers=num_workers)
        print("Estimated Parameters:", param_estimates)
        print("Objective Function Value:", obj_val)

        toc = time.perf_counter()
        print(f"Total time for two step simulation (in seconds): {toc - tic}")
        performance_times.append(toc - tic)
    
    print("Performance times for different numbers of processes:")
    for i, time_taken in enumerate(performance_times, start=1):
        print(f"Processes: {i}, Time taken: {time_taken:.2f} seconds")
    
    plt.plot(range(5, num_processes), performance_times, marker='o')
    plt.xlabel("Number of Processes")
    plt.ylabel("Time Taken (seconds)")
    plt.title("Performance of Two-Step SMM with Varying Processes")
    plt.savefig("Performance_Test_Two_Step_SMM.png")
    return performance_times, param_estimates


# TODO: write a function that creates a plot of confidence intervals for each parameter in Del Boca, all in one graph
# ------- HELPER FUNCTIONS --------------

# COPIED FROM INTERNET
# A function to determine the number of available CPUs on the system.
def available_cpu_count():
    """ Number of available virtual or physical CPUs on this system, i.e.
    user/real as output by time(1) when called with an optimally scaling
    userspace-only program"""

    # cpuset
    # cpuset may restrict the number of *available* processors
    try:
        m = re.search(r'(?m)^Cpus_allowed:\s*(.*)$',
                      open('/proc/self/status').read())
        if m:
            res = bin(int(m.group(1).replace(',', ''), 16)).count('1')
            if res > 0:
                return res
    except IOError:
        pass

    # Python 2.6+
    try:
        import multiprocessing
        return multiprocessing.cpu_count()
    except (ImportError, NotImplementedError):
        pass

    # https://github.com/giampaolo/psutil
    try:
        import psutil
        return psutil.cpu_count()   # psutil.NUM_CPUS on old versions
    except (ImportError, AttributeError):
        pass

    # POSIX
    try:
        res = int(os.sysconf('SC_NPROCESSORS_ONLN'))

        if res > 0:
            return res
    except (AttributeError, ValueError):
        pass

    # Windows
    try:
        res = int(os.environ['NUMBER_OF_PROCESSORS'])

        if res > 0:
            return res
    except (KeyError, ValueError):
        pass

    

    # BSD
    try:
        sysctl = subprocess.Popen(['sysctl', '-n', 'hw.ncpu'],
                                  stdout=subprocess.PIPE)
        scStdout = sysctl.communicate()[0]
        res = int(scStdout)

        if res > 0:
            return res
    except (OSError, ValueError):
        pass

    # Linux
    try:
        res = open('/proc/cpuinfo').read().count('processor\t:')

        if res > 0:
            return res
    except IOError:
        pass

    # Solaris
    try:
        pseudoDevices = os.listdir('/devices/pseudo/')
        res = 0
        for pd in pseudoDevices:
            if re.match(r'^cpuid@[0-9]+$', pd):
                res += 1

        if res > 0:
            return res
    except OSError:
        pass

    # Other UNIXes (heuristic)
    try:
        try:
            dmesg = open('/var/run/dmesg.boot').read()
        except IOError:
            dmesgProcess = subprocess.Popen(['dmesg'], stdout=subprocess.PIPE)
            dmesg = dmesgProcess.communicate()[0]

        res = 0
        while '\ncpu' + str(res) + ':' in dmesg:
            res += 1

        if res > 0:
            return res
    except OSError:
        pass

    return 0


# A function to compute the condition number of the Jacobian of the moment conditions.
# This may be useful for diagnosing potential issues with the parameter estimation process.
# Also, it should be useful for determining the right optimizer? But I haven't figured that out yet
def condition_number(param_vec: list, epsilon=1e-5) -> float:
    """Compute condition number of the Jacobian of moment conditions."""
    base_moments = simulate_moments(param_vec)
    k = len(param_vec)
    n = len(base_moments)
    J = np.zeros((n, k))
    
    for j in range(k):
        perturbed = param_vec.copy()
        perturbed[j] += epsilon
        diff = simulate_moments(perturbed) - base_moments
        J[:, j] = diff / epsilon
    
    cond = np.linalg.cond(J)
    print(f"Condition number of Jacobian: {cond:.2e}")
    return cond



# ------------------ MAIN FUNCTION ------------------
def main(): 
    tic = time.perf_counter()
    NUM_PROCESSES = available_cpu_count()
    print(f"Detected {NUM_PROCESSES} CPUs available for parallel processing.")

    # Calculating the condition number of the Jacobian of the moment conditions
    # condition_number(parameters)
    
    # TODO: replace following code with commented line below it
    
    households_empirical = create_households(NUMBER_OF_HOUSEHOLDS)
    empirical = np.average([simulate_moments(parameters_to_optimize, households_empirical) for _ in range(2)], axis=0) # Placeholder
    print("Here are our empirical moments: ")
    print(empirical)

    # empirical = load_latent_factors(LATENT_FACTOR_FILEPATH)


    with open(PARAM_OUTPUT_FILEPATH, "w") as f:
        f.write("Parameters to optimize:\n")
        for i in range(len(PARAMETER_NAMES)):
            f.write(f"{PARAMETER_NAMES[i]}: {parameters_to_optimize[i]}\n")

    # Performing the two-step SMM
    print("Starting two-step SMM...")
    households_simulated = create_households(NUMBER_OF_HOUSEHOLDS)
    param_test, func = two_step_smm(empirical, initial_guess=parameters, households=households_simulated, tolerances=[0.1, 0.1])
    print(param_test)
    print(func)

    # Writing the parameter estimates to a file
    with open(PARAM_OUTPUT_FILEPATH, "w") as f:
        f.write("\n\n")
        f.write("Parameter estimates from Simulated Method of Moments\n")
        for i in range(len(PARAMETER_NAMES)): 
            f.write(f"{PARAMETER_NAMES[i]}: {param_test[i]}\n")
    
    toc = time.perf_counter()
    print(f"Total time for two step simulation (in seconds): {toc - tic}")


    # Performance testing
    performance_test(empirical, households_simulated, num_processes=16)

    # Confidence intervals
    lower, upper = bootstrap_confidence_intervals(empirical, parameters, B=10, tolerances=[0.1, 0.1], grid_search=False)
    with open(PARAM_OUTPUT_FILEPATH, "a") as f:
        f.write("Bootstrapped confidence intervals for Simulated Method of Moments")
        for i in range(len(PARAMETER_NAMES)): 
            f.write(f"{PARAMETER_NAMES[i]}: ({lower[i]}, {upper[i]})")

    # Convergence test
    convergence_test(empirical, parameters, households_simulated)

    
    # Starting a grid search for the best parameters
    print("Starting grid search for best parameters...")
    tic = time.perf_counter()
    param_estimates, obj_val = grid_search_smm(empirical, households_simulated, STEP_SIZES, tolerances=[0.05, 0.005])
    print("Estimated Parameters:", param_estimates)
    print("Objective Function Value:", obj_val)
    toc = time.perf_counter()
    print(f"Total time for grid search simulation (in seconds): {toc - tic}")



    


if __name__ == "__main__": 
    main()