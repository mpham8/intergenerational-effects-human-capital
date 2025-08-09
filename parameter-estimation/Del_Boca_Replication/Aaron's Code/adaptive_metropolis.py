
"""
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
"""
import numpy as np
import pandas as pd
import pickle
import os
import time
from typing import List, Tuple, Dict, Callable, Any
import matplotlib.pyplot as plt
from scipy import stats

# Import from implementation file
from Del_Boca_Implementation import create_households, solve_households, moments



# Constants

# Model
NUM_PERIODS = 3
NUM_MOMENTS = 18

# MCMC
INITIAL_PARAMS = [10, -0.3, -0.1, 0.4, 0.15, 0.1] # close, but not equal to 'empirical' params
INITIAL_HOUSEHOLDS = 100 # number of households starting off
MAX_HOUSEHOLDS = 1000 # maximum number of households reached
HOUSEHOLD_INCREASE_THRESHOLD = 0.03 # when the number of households are increased
ADAPTATION_START_TIME = 1000 # time after which adaptive covariance starts
EPSILON = 0.05 # used in calculating proposal covariance matrix. Should be small with respect to moments
PROPOSAL_STRATEGY = 'adaptive' # choose between 'adaptive' (purely adaptive), 'mixed' (custom distributions + some adaptivity), and 'custom' (purely custom)
# NOTE: I am unsure if the 'mixed' function is statistically rigorous. 'Adaptive' and 'custom' should be though
STAGE1_ITERATIONS = 1000 # weighting matrix = identity
STAGE2_ITERATIONS = 500 # weighting matrix = inverse of covariance matrix of hat(theta_1)
SAVE_PREFIX = "first_test_run_adaptive" # also ending of estimator diagnostics graph filename
RANDOM_SEED = 42 # random seed to base the simulation off of. Select "None" to not have a set seed

# Proposal distributions
# For now, they are all truncated normal dists
# NOTE: not used under "adaptive" setting
PROPOSAL_DISTS = {
    'Delta' : {
        'type' : 'truncated_normal', 
        'scale' : 30, # scale = std
        'range' : (1, 100)
    }, 
    'rho_val' : {
        'type' : 'truncated_normal', 
        'scale' : 0.2,
        'range' : (-4, 1),
    }, 
    'rho_e_val' : {
        'type' : 'truncated_normal', 
        'scale' : 0.2, 
        'range' : (-4, 1)
    }, 
    'theta_1' : {
        'type' : 'truncated_normal', 
        'scale' : 0.05,
        'range' : (0, 1)
    }, 
    'theta_2' : {
        'type' : 'truncated_normal', 
        'scale' : 0.05,
        'range' : (0, 1)
    }, 
    'theta_3' : {
        'type' : 'truncated_normal', 
        'scale' : 0.05,
        'range' : (0, 1)
    }, 

}





# =============================================================
# DEFINING THE ADAPTIVE METROPOLIS CLASS
# =============================================================
class SMMAdaptiveMetropolis:
    """
    Simulated Method of Moments estimator using Adaptive Metropolis-Hastings.
    Implements two-step SMM with adaptive household count and comprehensive diagnostics.
    """
    
    def __init__(self, 
                 parameter_names: List[str] = None,
                 parameter_bounds: List[Tuple[float, float]] = None,
                 initial_params: List[float] = None,
                 empirical_moments: np.ndarray = None,
                 initial_households: int = 1000,
                 max_households: int = 10000,
                 household_increase_threshold: float = 0.05,
                 adaptation_start_time: int = 1000, 
                 epsilon: float = 0.01, 
                 custom_proposal_dists=None, 
                 proposal_strategy="adaptive",
                 random_seed: int = None):
        """
        Initialize the SMM Adaptive Metropolis estimator.
        
        Parameters:
        -----------
        parameter_names : List[str]
            Names of parameters being estimated
        parameter_bounds : List[Tuple[float, float]]
            Lower and upper bounds for each parameter
        initial_params : List[float]
            Starting values for parameters
        empirical_moments : np.ndarray
            Observed moments to match (if None, will generate placeholder)
        initial_households : int
            Starting number of households for simulation
        max_households : int
            Maximum number of households
        household_increase_threshold : float
            Threshold for parameter std decrease to increase households
        adaptation_start : int
            As described in Haario et al. (2001), the t_0 at which covariance adaptation for proposals begins
        epsilon: float
            Epsilon parameter in Haario et al. (2001). Should be small relative to parameters, ensuring our covariance matrix does not become singular
        random_seed : int
            Random seed for reproducibility
        """
        
        # Set default parameter names, bounds, and values
        if parameter_names is None:
            self.parameter_names = ["Delta", "rho_val", "rho_e_val", "theta_1", "theta_2", "theta_3"]
        else:
            self.parameter_names = parameter_names
            
        if parameter_bounds is None:
            self.parameter_bounds = [
                # Below, it's set up to change when new parameters are changed
                (1, 100),      # Delta
                (-4, 1),     # rho_1 (right now rho_val)
                (-4, 1),     # rho_2 (right now rho_e_val)
                # (-4, 1).   # rho_3
                (0, 1),        # omega_1^e (right now theta_1)
                (0, 1),        # omega_1^g (right now theta_2)
                (0, 1),        # omega_1^h (right now theta_3)
                # (0, 1),        # omega_2^e
                # (0, 1),        # omega_2^g
                # (0, 1),        # omega_2^h
                # (0, 1),        # omega_3^e
                # (0, 1),        # omega_3^g
                # (0, 1),        # omega_3^h
            ]
        else:
            self.parameter_bounds = parameter_bounds
            
        if initial_params is None:
            self.initial_params = [19.5, -0.25, -0.18, 0.28, 0.22, 0.18]
        else:
            self.initial_params = initial_params
            
        
        # Simulation parameters
        self.num_initial_households = initial_households
        self.num_max_households = max_households
        self.household_increase_threshold = household_increase_threshold
        self.num_current_households = initial_households
        
        self.simulated_households = create_households(self.num_initial_households)

        # Random state (in case we want to replicate analysis)
        self.random_seed = random_seed
        if random_seed != None: 
            np.random.seed(random_seed)
        
        # Load or generate empirical moments
        # NOTE: once we have empirical moments, these lines need to be changed
        if empirical_moments is not None:
            self.empirical_moments = empirical_moments
        else:
            self.empirical_moments = self._generate_placeholder_empirical_moments()
            

        # Load custom parameter distributions
        self.custom_proposal_dists = custom_proposal_dists or {}
        self.proposal_strategy = proposal_strategy
        
        if self.custom_proposal_dists:
            validate_custom_proposals(self.custom_proposal_dists, self.parameter_names)

        # Initialize chain storage
        self.chain = []
        self.accepted_chain = []
        self.smm_values = []
        self.acceptance_history = []
        self.household_count_history = []
        
        # Adaptation parameters
        self.adaptation_start = adaptation_start_time
        self.increase_household_spacer = 0
        self.epsilon = epsilon
        self.current_iteration = 0
        self.n_accepted = 0
        self.covariance_matrix = None
        self.mean_params = None
        
        # SMM components
        self.weighting_matrix = np.eye(NUM_MOMENTS)  # Start with identity
        self.moment_variances = None
        self.is_second_stage = False
        
        # Diagnostics
        self.parameter_std_history = []
        self.computation_times = []
        
        print(f"Initialized SMM Adaptive Metropolis with {len(self.parameter_names)} parameters")
        print(f"Starting with {self.num_current_households} households")
        
    def _generate_placeholder_empirical_moments(self) -> np.ndarray:
        """Generate placeholder empirical moments using simulation."""
        print("Generating placeholder empirical moments...")
        # Parameters on which the empirical moments are based (i.e., any test runs should approximate something close to these parameters)
        parameters_to_optimize = [20, -0.2, -0.2, 0.3, 0.2, 0.2]
        
        # Averaging over multiple simulations of parameters
        empirical_moments_list = []
        for _ in range(10): # Increase the number of loops for greater accuracy of "empirical" parameters
            households_empirical = create_households(self.num_initial_households)
            solved_households = solve_households(households_empirical.copy(), parameters_to_optimize)
            empirical_moments_list.append(moments(solved_households))
        empirical = np.average(empirical_moments_list, axis=0)
        
        print("Generated empirical moments successfully")
        return empirical
        
    def load_empirical_moments(self, filepath: str) -> np.ndarray:
        """
        Placeholder function to load empirical moments from file.
        Replace this with your actual data loading logic.
        """
        # TODO: Implement actual moment loading
        # For now, return the generated moments
        print(f"TODO: Implement loading from {filepath}")
        return self.empirical_moments
        
    def _check_parameter_constraints(self, params: List[float]) -> bool:
        """Check if parameters satisfy bounds and theta sum constraint."""
        # Check individual bounds
        for i, (param, (lower, upper)) in enumerate(zip(params, self.parameter_bounds)):
            if param < lower or param > upper:
                return False
                
        # Check theta sum constraint (theta_4 = 1 - sum(theta_1, theta_2, theta_3) >= 0)
        theta_sum = params[3] + params[4] + params[5]  # theta_1 + theta_2 + theta_3
        if theta_sum > 1.0:
            return False
        
        # TODO: when adding more parameters, add more constraints (sums for each period)
        return True
        
    def _simulate_moments(self, params: List[float]) -> np.ndarray:
        """Simulate moments for given parameters."""
        households_sim = create_households(self.num_current_households)
        solved_households = solve_households(households_sim, params)
        return moments(solved_households)
        
    def _compute_smm_objective(self, params: List[float]) -> float:
        """Compute SMM objective function with standardized moments."""
        
        # Calculate simulated moments, using the same households every time
        # NOTE: using the same households every time is based on this article: https://opensourceecon.github.io/CompMethods/struct_est/SMM.html
        solved_households = solve_households(self.simulated_households, params)
        simulated_moments = moments(solved_households)
        
            
        # Moment differences
        # NOTE: moments should always be POSITIVE. If moments are not positive, this line may not work
        moment_diff = (simulated_moments - self.empirical_moments) / self.empirical_moments
        
        # Standard SMM objective
        smm_obj = moment_diff.T @ self.weighting_matrix @ moment_diff
        
        return smm_obj
        
    def _update_adaptive_covariance(self):
        """Update covariance matrix for adaptive proposals."""
        if len(self.accepted_chain) < self.adaptation_start:
            return
            
        # Convert to numpy array
        chain_array = np.array(self.accepted_chain[-self.adaptation_start:])
        
        # Compute empirical covariance
        # TODO: if this is a time-intensive step, use Haario et al. (2001)'s recursive formula for it
        self.mean_params = np.mean(chain_array, axis=0)
        cov = np.cov(chain_array, rowvar=False)
        
        # Adaptive Metropolis scaling
        # See Haario et al. (2001) and "Efficient Metropolis Jumping Rules" by Gelman et al. (1996) for more explanation
        d = len(self.parameter_names)
        scaling_parameter = (2.38**2) / d
        
        # Regularization for numerical stability
        # NOTE: self.covariance_matrix is equivalent to C_t in Haario et al. (2001)
        self.covariance_matrix = scaling_parameter * cov + scaling_parameter * self.epsilon * np.eye(d)

        #Very positive definiteness
        if not is_positive_definite(self.covariance_matrix):
            print("Warning: Updated covariance matrix is not positive definite, adding extra regularization")
            self.covariance_matrix += self.epsilon * np.eye(d)
    
    # TODO: check this function in more detail and make sure it's in line with Haario et al. (2001)
    def _should_increase_households(self) -> bool:
        """Determine if household count should be increased."""
        if (self.num_current_households >= self.num_max_households or 
            len(self.parameter_std_history) < 5):
            return False
            
        # If it's too soon since we last updated the households, return false
        if self.increase_household_spacer < 2: # i.e., less than 2 more parameter proposals have been accepted
            return False
        # Check if parameter standard deviations have stabilized
        recent_stds = self.parameter_std_history[-5:]
        std_of_stds = np.std(recent_stds, axis=0)
        mean_std_change = np.mean(std_of_stds)
        
        return mean_std_change < self.household_increase_threshold
        
    def _increase_household_count(self):
        """Increase the number of households for simulation."""
        # Calculating the number of new households
        old_count = self.num_current_households
        self.num_current_households = min(int(self.num_current_households * 1.5), self.num_max_households)
        # Creating a new set of households
        # NOTE: we should probably just add to the old households, but this is easier logic-wise
        self.simulated_households = create_households(self.num_current_households)
        self.increase_household_spacer = 0
        print(f"Increased household count from {old_count} to {self.num_current_households}")
        
    def _compute_diagnostics(self) -> Dict[str, Any]:
        """Compute chain diagnostics."""
        if len(self.accepted_chain) < 100:
            return {}
            
        chain_array = np.array(self.accepted_chain)
        diagnostics = {}
        
        # Acceptance rate
        diagnostics['acceptance_rate'] = self.n_accepted / max(1, self.current_iteration)
        
        # Effective sample size (simple autocorrelation-based estimate)
        ess = []
        for i in range(chain_array.shape[1]):
            param_chain = chain_array[:, i]
            autocorr = np.correlate(param_chain - np.mean(param_chain), 
                                  param_chain - np.mean(param_chain), mode='full')
            autocorr = autocorr[autocorr.size // 2:]
            autocorr = autocorr / autocorr[0]
            
            # Find first negative autocorrelation or use length
            tau = 1
            for j in range(1, min(len(autocorr), len(param_chain)//4)):
                if autocorr[j] <= 0:
                    break
                tau += 2 * autocorr[j]
            
            ess.append(len(param_chain) / (2 * tau))
            
        diagnostics['effective_sample_size'] = ess
        
        # R-hat (simplified version - would need multiple chains for full implementation)
        diagnostics['rhat_available'] = False
        
        return diagnostics
    
    def _compute_optimal_weighting_matrix(self):
        """Compute optimal weighting matrix with robust PSD enforcement."""
        weighting_matrix, diagnostics, success = compute_optimal_weighting_matrix_robust(
            accepted_chain=self.accepted_chain,
            simulate_moments_func=self._simulate_moments,
            num_moments=NUM_MOMENTS,
            min_samples_multiplier=2,
            max_param_vectors=500,
            subsample_target=200
        )
    
        if success:
            self.weighting_matrix = weighting_matrix
            # Save diagnostics if desired
            save_weighting_matrix_diagnostics(
                diagnostics,
                np.eye(NUM_MOMENTS),  # placeholder for original_cov
                np.eye(NUM_MOMENTS),  # placeholder for final_cov  
                weighting_matrix
            )
        else:
            print("Using identity weighting matrix")
            self.weighting_matrix = np.eye(NUM_MOMENTS)
    
    def _propose_parameters(self, current_params): 
        """Proposing parameters in our adaptive Metropolis algorithm"""
        if self.proposal_strategy == "custom":
            proposal = propose_custom_parameters(
                current_params, self.parameter_names, self.custom_proposal_dists
            )
        elif self.proposal_strategy == "mixed":
            proposal = propose_mixed_parameters(
                current_params, self.parameter_names, self.custom_proposal_dists,
                self.covariance_matrix, 
                use_adaptive=(self.covariance_matrix is not None and 
                            len(self.accepted_chain) > self.adaptation_start)
            )
        else:  # "adaptive"
            proposal = propose_adaptive_parameters(
                current_params, self.parameter_names, self.custom_proposal_dists,
                self.covariance_matrix,
                use_adaptive=(self.covariance_matrix is not None and 
                            len(self.accepted_chain) > self.adaptation_start)
            )
        return proposal

    def run_mcmc(self, 
                 n_iterations: int,
                 save_every: int = 100,
                 save_prefix: str = "smm_mcmc") -> Dict[str, Any]:
        """
        Run the MCMC chain.
        
        Parameters:
        -----------
        n_iterations : int
            Number of MCMC iterations
        save_every : int
            Save checkpoint every N iterations
        save_prefix : str
            Prefix for save files
            
        Returns:
        --------
        Dict with results and diagnostics
        """
        
        print(f"Starting MCMC chain for {n_iterations} iterations...")
        print(f"Stage: {'Second (with optimal weights)' if self.is_second_stage else 'First (identity weights)'}")
        
        # Initialize current parameters
        if len(self.chain) == 0:
            current_params = self.initial_params.copy()
            current_smm = self._compute_smm_objective(current_params)
            self.chain.append(current_params)
            self.accepted_chain.append(current_params)
            self.smm_values.append(current_smm)
        else:
            current_params = self.chain[-1].copy()
            current_smm = self.smm_values[-1]
            
        start_time = time.time()
        
        for iteration in range(n_iterations):
            iter_start_time = time.time()
            self.current_iteration += 1
            
            # Propose new parameters
            proposal = self._propose_parameters(current_params)
                
            # Check constraints
            if not self._check_parameter_constraints(proposal):
                # Reject proposal
                self.chain.append(current_params)
                self.smm_values.append(current_smm)
                self.acceptance_history.append(False)
            else:
                # Evaluate SMM objective
                proposal_smm = self._compute_smm_objective(proposal)
                
                # Metropolis acceptance step (convert SMM to pseudo-likelihood)
                log_alpha = -0.5 * (proposal_smm - current_smm)
                alpha = min(1.0, np.exp(log_alpha))
                
                if np.random.random() < alpha:
                    # Accept proposal
                    current_params = proposal.copy()
                    current_smm = proposal_smm
                    self.accepted_chain.append(current_params)
                    self.n_accepted += 1
                    self.acceptance_history.append(True)
                    self.increase_household_spacer += 1
                else:
                    # Reject proposal
                    self.acceptance_history.append(False)
                    
                self.chain.append(current_params)
                self.smm_values.append(current_smm)
                
            # Update adaptation
            if self.current_iteration > self.adaptation_start:
                self._update_adaptive_covariance()
                
            # Track household count
            self.household_count_history.append(self.num_current_households)
            
            # Track parameter standard deviations
            if len(self.accepted_chain) > 50:
                recent_chain = np.array(self.accepted_chain[-50:])
                param_stds = np.std(recent_chain, axis=0)
                self.parameter_std_history.append(param_stds)
                
                # Check if we should increase household count
                if self._should_increase_households():
                    self._increase_household_count()
                    
            # Track computation time
            iter_time = time.time() - iter_start_time
            self.computation_times.append(iter_time)
            
            # Progress reporting
            if iteration % 100 == 0:
                acceptance_rate = estimator.n_accepted / estimator.current_iteration
                current_best_smm = min(estimator.smm_values)
                avg_time = np.mean(estimator.computation_times[-100:]) if estimator.computation_times else 0
                print(f"--Iteration {estimator.current_iteration}: "
                    f"Accept Rate: {acceptance_rate:.3f}, "
                    f"Best SMM: {current_best_smm:.6f}, "
                    f"Households: {estimator.num_current_households}, "
                    f"Avg Time: {avg_time:.3f}s, "
                    f"Accepted chain length: {len(estimator.accepted_chain)}")
                      
            # Save checkpoint
            if (iteration + 1) % save_every == 0:
                self.save_checkpoint(save_prefix)
                
        total_time = time.time() - start_time
        print(f"MCMC completed in {total_time:.2f} seconds")
        
        # Final diagnostics
        diagnostics = self._compute_diagnostics()
        
        return {
            'chain': np.array(self.chain),
            'accepted_chain': np.array(self.accepted_chain),
            'smm_values': np.array(self.smm_values),
            'diagnostics': diagnostics,
            'total_time': total_time
        }
        
    def run_two_step_smm(self,
                        stage1_iterations: int = 20000,
                        stage2_iterations: int = 10000,
                        save_every: int = 100,
                        save_prefix: str = "two_step_smm") -> Dict[str, Any]:
        """
        Run two-step SMM procedure.
        
        Stage 1: Identity weighting matrix
        Stage 2: Optimal weighting matrix computed from Stage 1
        """
        
        print("="*60)
        print("STARTING TWO-STEP SMM PROCEDURE")
        print("="*60)
        
        # Stage 1: Identity weighting matrix
        print("\nSTAGE 1: Running with identity weighting matrix...")
        self.is_second_stage = False
        self.weighting_matrix = np.eye(NUM_MOMENTS)
        
        stage1_results = self.run_mcmc(
            n_iterations=stage1_iterations,
            save_every=save_every,
            save_prefix=f"{save_prefix}_stage1"
        )
        
        # Compute optimal weighting matrix
        print("\nComputing optimal weighting matrix...")
        self._compute_optimal_weighting_matrix()
        
        # Stage 2: Optimal weighting matrix
        print("\nSTAGE 2: Running with optimal weighting matrix...")
        self.is_second_stage = True
        
        # Reset some adaptation parameters for stage 2
        stage2_start_params = self.get_parameter_estimates()['mean']
        self.initial_params = stage2_start_params
        
        stage2_results = self.run_mcmc(
            n_iterations=stage2_iterations,
            save_prefix=f"{save_prefix}_stage2"
        )
        
        print("\n" + "="*60)
        print("TWO-STEP SMM COMPLETED")
        print("="*60)
        
        return {
            'stage1_results': stage1_results,
            'stage2_results': stage2_results,
            'final_estimates': self.get_parameter_estimates()
        }
            
    def get_parameter_estimates(self) -> Dict[str, Any]:
        """Get parameter estimates and standard errors."""
            
        chain_array = np.array(self.accepted_chain)
        
        # Remove burnin (first 20% of samples)
        burnin = max(100, len(chain_array) // 5)
        post_burnin = chain_array[burnin:]

        if len(post_burnin) < 100: 
            print("Too few results to get accurate estimates.")
            return {}
        
        estimates = {}
        for i, name in enumerate(self.parameter_names):
            estimates[name] = {
                'mean': np.mean(post_burnin[:, i]),
                'std': np.std(post_burnin[:, i]),
                'quantile_025': np.percentile(post_burnin[:, i], 2.5),
                'quantile_975': np.percentile(post_burnin[:, i], 97.5)
            }
            
        # Overall summary
        estimates['mean'] = np.mean(post_burnin, axis=0)
        estimates['std'] = np.std(post_burnin, axis=0)
        estimates['n_samples'] = len(post_burnin)
        
        return estimates
        
    def save_checkpoint(self, prefix: str = "smm_checkpoint"):
        """Save current state for resuming later."""
        
        # Save heavy data to pickle
        pickle_data = {
            'chain': self.chain,
            'accepted_chain': self.accepted_chain,
            'smm_values': self.smm_values,
            'covariance_matrix': self.covariance_matrix,
            'weighting_matrix': self.weighting_matrix,
            'empirical_moments': self.empirical_moments,
            'parameter_std_history': self.parameter_std_history,
            'computation_times': self.computation_times
        }
        
        with open(f"{prefix}_data.pkl", 'wb') as f:
            pickle.dump(pickle_data, f)
            
        # Save important scalars and diagnostics to Excel
        diagnostics = self._compute_diagnostics()
        
        # Create summary DataFrame
        summary_data = {
            'Metric': ['Current Iteration', 'Accepted Samples', 'Current Households', 
                      'Acceptance Rate', 'Is Second Stage', 'Best SMM Value',
                      'Average Computation Time', 'Random Seed'],
            'Value': [self.current_iteration, self.n_accepted, self.num_current_households,
                     self.n_accepted / max(1, self.current_iteration), self.is_second_stage,
                     min(self.smm_values) if self.smm_values else np.nan,
                     np.mean(self.computation_times[-100:]) if len(self.computation_times) > 0 else np.nan,
                     self.random_seed]
        }
        
        summary_df = pd.DataFrame(summary_data)
        
        # Parameter estimates
        estimates = self.get_parameter_estimates()
        if estimates:
            param_data = []
            for name in self.parameter_names:
                if name in estimates:
                    param_data.append({
                        'Parameter': name,
                        'Mean': estimates[name]['mean'],
                        'Std': estimates[name]['std'],
                        'Q2.5': estimates[name]['quantile_025'],
                        'Q97.5': estimates[name]['quantile_975']
                    })
            param_df = pd.DataFrame(param_data)
        else:
            param_df = pd.DataFrame()
            
        # Save to Excel
        with pd.ExcelWriter(f"{prefix}_summary.xlsx") as writer:
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
            if not param_df.empty:
                param_df.to_excel(writer, sheet_name='Parameter_Estimates', index=False)
                
        print(f"Checkpoint saved: {prefix}_data.pkl and {prefix}_summary.xlsx")
        
    def load_checkpoint(self, prefix: str = "smm_checkpoint"):
        """Load previous state to resume estimation."""
        
        # Load pickle data
        try:
            with open(f"{prefix}_data.pkl", 'rb') as f:
                pickle_data = pickle.load(f)
                
            self.chain = pickle_data['chain']
            self.accepted_chain = pickle_data['accepted_chain']
            self.smm_values = pickle_data['smm_values']
            self.covariance_matrix = pickle_data['covariance_matrix']
            self.weighting_matrix = pickle_data['weighting_matrix']
            self.empirical_moments = pickle_data['empirical_moments']
            self.parameter_std_history = pickle_data['parameter_std_history']
            self.computation_times = pickle_data['computation_times']
            
            # Reconstruct other variables
            self.current_iteration = len(self.chain)
            self.n_accepted = len(self.accepted_chain)

            # Verify loaded covariance matrix is positive definite
            if self.covariance_matrix is not None and not is_positive_definite(self.covariance_matrix):
                print("Warning: Loaded covariance matrix is not positive definite, reinitializing")
                d = len(self.parameter_names)
                scaling_parameter = (2.38**2) / d
                initial_variances = []
                for i, param_name in enumerate(self.parameter_names):
                    if param_name in self.custom_proposal_dists and 'scale' in self.custom_proposal_dists[param_name]:
                        variance = self.custom_proposal_dists[param_name]['scale']**2
                    else:
                        lower, upper = self.parameter_bounds[i]
                        variance = ((upper - lower) * 0.01)**2
                    initial_variances.append(variance)
                self.covariance_matrix = scaling_parameter * np.diag(initial_variances)
            
            print(f"Checkpoint loaded: {prefix}_data.pkl")
            print(f"Resumed at iteration {self.current_iteration} with {self.n_accepted} accepted samples")
            
        except FileNotFoundError:
            print(f"Checkpoint file {prefix}_data.pkl not found")
            
    def plot_diagnostics(self, save_path: str = None):
        """Plot chain diagnostics."""
        if len(self.accepted_chain) < 10:
            print("Not enough samples for plotting")
            return
            
        chain_array = np.array(self.accepted_chain)
        n_params = len(self.parameter_names)
        
        fig, axes = plt.subplots(n_params, 2, figsize=(12, 3*n_params))
        if n_params == 1:
            axes = axes.reshape(1, -1)
            
        for i, name in enumerate(self.parameter_names):
            # Trace plot
            axes[i, 0].plot(chain_array[:, i])
            axes[i, 0].set_title(f'{name} - Trace Plot')
            axes[i, 0].set_xlabel('Iteration')
            axes[i, 0].set_ylabel('Value')
            
            # Histogram
            axes[i, 1].hist(chain_array[:, i], bins=50, alpha=0.7)
            axes[i, 1].set_title(f'{name} - Posterior Distribution')
            axes[i, 1].set_xlabel('Value')
            axes[i, 1].set_ylabel('Frequency')
            
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            print(f"Diagnostics plot saved: {save_path}")
        else:
            plt.show()



# HELPER FUNCTIONS
# =============================================================================
# =============================================================================

def is_positive_definite(matrix: np.ndarray) -> bool:
    """Check if a matrix is positive definite."""
    if matrix is None:
        return False
    try:
        eigenvals = np.linalg.eigvals(matrix)
        return np.all(eigenvals > 0)
    except np.linalg.LinAlgError:
        return False

# =============================================================================
# WEIGHTING MATRIX DIAGNOSTICS FUNCTIONS
# =============================================================================

def compute_optimal_weighting_matrix_robust(accepted_chain: List[List[float]], 
                                          simulate_moments_func: Callable,
                                          num_moments: int = 24,
                                          min_samples_multiplier: int = 2,
                                          max_param_vectors: int = 500,
                                          subsample_target: int = 200) -> tuple:
    """
    Compute optimal weighting matrix with robust PSD enforcement.
    
    Parameters:
    -----------
    accepted_chain : List[List[float]]
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
    
    if len(accepted_chain) < min_samples:
        print(f"Warning: Need at least {min_samples} samples, have {len(accepted_chain)}")
        print("Using identity weighting matrix")
        return np.eye(num_moments), {}, False
        
    print("Computing moment variance-covariance matrix...")
    
    # Use recent accepted parameters
    n_params_to_use = min(max_param_vectors, len(accepted_chain))
    recent_params = accepted_chain[-n_params_to_use:]
    
    # Subsample to target number
    subsample_rate = max(1, len(recent_params) // subsample_target)
    moment_sims = []
    
    print(f"Using {len(recent_params)} parameter vectors with subsample rate {subsample_rate}")
    
    for i in range(0, len(recent_params), subsample_rate):
        if i % 20 == 0:
            print(f"  Computing moments for parameter vector {i//subsample_rate + 1}/{len(recent_params)//subsample_rate}")
        params = recent_params[i]
        sim_moments = simulate_moments_func(params)
        moment_sims.append(sim_moments)
        
    moment_sims = np.array(moment_sims)
    print(f"Computed {len(moment_sims)} moment vectors")
    
    # Compute variance-covariance matrix
    moment_cov = np.cov(moment_sims, rowvar=False)
    
    # Enforce PSD and compute diagnostics
    weighting_matrix, diagnostics = enforce_positive_semidefinite(moment_cov, num_moments)
    
    return weighting_matrix, diagnostics, True


def enforce_positive_semidefinite(moment_cov: np.ndarray, 
                                num_moments: int,
                                min_eigenval: float = 1e-8,
                                diagonal_reg: float = 1e-6) -> tuple:
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
    
    print("Checking positive semi-definiteness...")
    eigenvals, eigenvecs = np.linalg.eigh(moment_cov)
    
    print(f"Eigenvalue range: [{np.min(eigenvals):.2e}, {np.max(eigenvals):.2e}]")
    
    # Count problematic eigenvalues
    negative_eigs = np.sum(eigenvals < 0)
    near_zero_eigs = np.sum(np.abs(eigenvals) < 1e-12)
    
    if negative_eigs > 0:
        print(f"Warning: {negative_eigs} negative eigenvalues detected")
    if near_zero_eigs > 0:
        print(f"Warning: {near_zero_eigs} near-zero eigenvalues detected (< 1e-12)")
        
    # Eigenvalue regularization
    regularized_eigenvals = np.maximum(eigenvals, min_eigenval)
    
    # Reconstruct PSD matrix
    moment_cov_psd = eigenvecs @ np.diag(regularized_eigenvals) @ eigenvecs.T
    
    # Additional diagonal regularization
    diagonal_reg_matrix = diagonal_reg * np.eye(num_moments)
    moment_cov_final = moment_cov_psd + diagonal_reg_matrix
    
    # Verify final matrix is PSD
    final_eigenvals = np.linalg.eigvals(moment_cov_final)
    min_final_eig = np.min(final_eigenvals)
    
    if min_final_eig > 0:
        print(f"Matrix is positive definite (min eigenvalue: {min_final_eig:.2e})")
    else:
        print(f"Warning: Matrix still not PSD (min eigenvalue: {min_final_eig:.2e})")
        
    # Compute condition number
    condition_number = np.max(final_eigenvals) / np.min(final_eigenvals)
    print(f"Condition number: {condition_number:.2e}")
    
    if condition_number > 1e12:
        print("Warning: Matrix is ill-conditioned, results may be unreliable")
        
    # Compute weighting matrix
    try:
        weighting_matrix = np.linalg.inv(moment_cov_final)
        
        # Verify weighting matrix is also PSD
        w_eigenvals = np.linalg.eigvals(weighting_matrix)
        if np.min(w_eigenvals) > 0:
            print("Optimal weighting matrix computed successfully and is positive definite")
        else:
            print("Warning: Weighting matrix is not positive definite")
            
    except np.linalg.LinAlgError as e:
        print(f"Error inverting moment covariance: {e}")
        print("Using identity weighting matrix")
        weighting_matrix = np.eye(num_moments)
        
    # Compile diagnostics
    diagnostics = {
        'original_eigenvalues': eigenvals,
        'regularized_eigenvalues': regularized_eigenvals,
        'final_eigenvalues': final_eigenvals,
        'condition_number_original': np.max(eigenvals) / np.max(np.abs(eigenvals[eigenvals != 0])) if np.any(eigenvals != 0) else np.inf,
        'condition_number_final': condition_number,
        'negative_eigenvalues': negative_eigs,
        'near_zero_eigenvalues': near_zero_eigs,
        'min_eigenvalue_original': np.min(eigenvals),
        'min_eigenvalue_final': min_final_eig
    }
    
    return weighting_matrix, diagnostics


def save_weighting_matrix_diagnostics(diagnostics: Dict[str, Any],
                                    original_cov: np.ndarray,
                                    final_cov: np.ndarray,
                                    weighting_matrix: np.ndarray,
                                    filename: str = 'weighting_matrix_diagnostics.pkl'):
    """Save detailed weighting matrix diagnostics."""
    
    diagnostic_data = {
        'diagnostics': diagnostics,
        'original_covariance': original_cov,
        'final_covariance': final_cov,
        'weighting_matrix': weighting_matrix
    }
    
    with open(filename, 'wb') as f:
        pickle.dump(diagnostic_data, f)
        
    print(f"Weighting matrix diagnostics saved to '{filename}'")


# =============================================================================
# CUSTOM PROPOSAL FUNCTIONS
# =============================================================================

def validate_custom_proposals(custom_proposal_dists: Dict[str, Dict], 
                            parameter_names: List[str]):
    """Validate custom proposal distribution specifications."""
    
    for param_name, dist_spec in custom_proposal_dists.items():
        if param_name not in parameter_names:
            raise ValueError(f"Custom proposal specified for unknown parameter: {param_name}")
        
        required_keys = ['type']
        if not all(key in dist_spec for key in required_keys):
            raise ValueError(f"Custom proposal for {param_name} missing required keys: {required_keys}")
        
        # Validate specific distribution types
        dist_type = dist_spec['type']
        if dist_type == 'normal':
            if not all(key in dist_spec for key in ['mean', 'std']):
                raise ValueError(f"Normal proposal for {param_name} requires 'std' parameter")
        elif dist_type == 'lognormal':
            if 'sigma' not in dist_spec:
                raise ValueError(f"Lognormal proposal for {param_name} requires 'sigma' parameter")
        elif dist_type == 'beta':
            if not all(key in dist_spec for key in ['alpha', 'beta', 'loc', 'scale']):
                raise ValueError(f"Beta proposal for {param_name} requires 'alpha', 'beta', 'loc', 'scale'")
        elif dist_type == 'truncated_normal':
            if not all(key in dist_spec for key in ['scale', 'range']):
                raise ValueError(f"Truncated normal proposal for {param_name} requires 'scale', 'range'")
        elif dist_type == 'custom_function':
            if 'sampler' not in dist_spec:
                raise ValueError(f"Custom function proposal for {param_name} requires 'sampler' function")
        else: 
            raise ValueError(f"Unknown proposal distribution type: {dist_type}")


def propose_single_parameter(param_idx: int, 
                           current_value: float,
                           parameter_names: List[str],
                           custom_proposal_dists: Dict[str, Dict],
                           adaptive_cov_diagonal: np.ndarray = None,
                           default_scale: float = 0.1) -> float:
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
    if param_name in custom_proposal_dists:
        dist_spec = custom_proposal_dists[param_name]
        dist_type = dist_spec['type']
        
        if dist_type == 'normal':
            std = dist_spec['std']
            proposal = np.random.normal(current_value, std)
            
        elif dist_type == 'lognormal':
            sigma = dist_spec['sigma']
            # Sample in log space, then transform
            log_current = np.log(max(current_value, 1e-10))  # Avoid log(0)
            log_proposal = np.random.normal(log_current, sigma)
            proposal = np.exp(log_proposal)
            
        elif dist_type == 'beta':
            alpha, beta = dist_spec['alpha'], dist_spec['beta']
            loc, scale = dist_spec['loc'], dist_spec['scale']
            # Sample from beta and transform to desired range
            beta_sample = np.random.beta(alpha, beta)
            proposal = loc + scale * beta_sample
            
        elif dist_type == 'truncated_normal':
            scale = dist_spec['scale']
            lower, upper = dist_spec['range']
            proposal = stats.truncnorm.rvs(
                (lower - current_value) / scale,
                (upper - current_value) / scale,
                loc=current_value, scale=scale
            )

        elif dist_type == 'uniform': 
            lower, upper = dist_spec['range']
            proposal = np.random.uniform(lower, upper)
            
            
        elif dist_type == 'custom_function':
            sampler = dist_spec['sampler']
            proposal = sampler(current_value, **dist_spec.get('kwargs', {}))
    
        else:
            # Fallback to normal
            proposal = np.random.normal(current_value, default_scale)
            
    else:
        # Use adaptive covariance if available
        if adaptive_cov_diagonal is not None:
            var = adaptive_cov_diagonal[param_idx]
            proposal = np.random.normal(current_value, np.sqrt(max(var, 1e-8)))
        else:
            # Default normal proposal
            proposal = np.random.normal(current_value, default_scale)
            
    return proposal


def propose_custom_parameters(current_params: List[float],
                            parameter_names: List[str],
                            custom_proposal_dists: Dict[str, Dict]) -> np.ndarray:
    """Propose new parameters using only custom distributions."""
    
    proposal = np.zeros(len(current_params))
    
    for i, current_val in enumerate(current_params):
        proposal[i] = propose_single_parameter(
            i, current_val, parameter_names, custom_proposal_dists
        )
        
    return proposal


def propose_mixed_parameters(current_params: List[float],
                           parameter_names: List[str],
                           custom_proposal_dists: Dict[str, Dict],
                           covariance_matrix: np.ndarray = None,
                           use_adaptive: bool = False) -> np.ndarray:
    """Mix custom proposals with adaptive covariance where available."""
    
    proposal = np.zeros(len(current_params))
    
    if use_adaptive and covariance_matrix is not None:
        # Start with full adaptive proposal
        adaptive_proposal = np.random.multivariate_normal(current_params, covariance_matrix)
        
        # Override with custom proposals where specified
        for i, param_name in enumerate(parameter_names):
            if param_name in custom_proposal_dists:
                proposal[i] = propose_single_parameter(
                    i, current_params[i], parameter_names, custom_proposal_dists
                )
            else:
                proposal[i] = adaptive_proposal[i]
    else:
        # Use individual parameter proposals
        adaptive_cov_diagonal = None
        if covariance_matrix is not None:
            adaptive_cov_diagonal = np.diag(covariance_matrix)
            
        for i, current_val in enumerate(current_params):
            proposal[i] = propose_single_parameter(
                i, current_val, parameter_names, custom_proposal_dists,
                adaptive_cov_diagonal
            )
            
    return proposal


def propose_adaptive_parameters(current_params: List[float],
                               parameter_names: List[str],
                               custom_proposal_dists: Dict[str, Dict],
                               covariance_matrix: np.ndarray = None,
                               use_adaptive: bool = False) -> np.ndarray:
    """Standard adaptive proposal with custom overrides."""
    
    if use_adaptive and covariance_matrix is not None:
        # Use adaptive covariance as base
        base_proposal = np.random.multivariate_normal(current_params, covariance_matrix)
        
        # Override specific parameters with custom proposals
        for i, param_name in enumerate(parameter_names):
            if param_name in custom_proposal_dists:
                base_proposal[i] = propose_single_parameter(
                    i, current_params[i], parameter_names, custom_proposal_dists
                )
                
        return base_proposal
    else:
        # Early phase: use individual proposals
        proposal = np.zeros(len(current_params))
        adaptive_cov_diagonal = None
        if covariance_matrix is not None:
            adaptive_cov_diagonal = np.diag(covariance_matrix)
            
        for i, current_val in enumerate(current_params):
            proposal[i] = propose_single_parameter(
                i, current_val, parameter_names, custom_proposal_dists,
                adaptive_cov_diagonal
            )
            
        return proposal


# =============================================================================
# =============================================================================
# =============================================================================
# MAIN FUNCTION
# =============================================================================
# =============================================================================
# =============================================================================


if __name__ == "__main__":
    
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
    
    # Run two-step SMM
    results = estimator.run_two_step_smm(
        stage1_iterations=STAGE1_ITERATIONS,  # Reduced for example
        stage2_iterations=STAGE2_ITERATIONS,   # Reduced for example
        save_prefix=SAVE_PREFIX, 
    )
    
    # Get final estimates
    final_estimates = estimator.get_parameter_estimates()
    print("\nFinal Parameter Estimates:")
    print("="*50)
    for name in estimator.parameter_names:
        if name in final_estimates:
            est = final_estimates[name]
            print(f"{name:>12}: {est['mean']:8.4f} ± {est['std']:6.4f} "
                  f"[{est['quantile_025']:7.4f}, {est['quantile_975']:7.4f}]")
    
    # Plot diagnostics
    estimator.plot_diagnostics(f"parameter-estimation/Del_Boca_Replication/estimator_diagnostics_{SAVE_PREFIX}.png")
    
    print(f"\nEstimation completed!")
    print(f"Total accepted samples: {len(estimator.accepted_chain)}")
    print(f"Final acceptance rate: {estimator.n_accepted / estimator.current_iteration:.3f}")