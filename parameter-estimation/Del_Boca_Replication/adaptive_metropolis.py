import numpy as np
import pandas as pd
import pickle
import os
import time
from typing import List, Tuple, Optional, Dict, Any
import matplotlib.pyplot as plt
from scipy import stats
import warnings

# Import from your implementation file
from Del_Boca_Implementation import create_households, solve_households, moments

# Constants
NUM_PERIODS = 4
NUM_MOMENTS = 24

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
                 random_seed: int = 42):
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
        random_seed : int
            Random seed for reproducibility
        """
        
        # Set default parameter names and bounds
        if parameter_names is None:
            self.parameter_names = ["Delta", "rho_val", "rho_e_val", "theta_1", "theta_2", "theta_3"]
        else:
            self.parameter_names = parameter_names
            
        if parameter_bounds is None:
            self.parameter_bounds = [
                (0, 100),      # Delta
                (-5, 0.5),     # rho_val
                (-5, 0.5),     # rho_e_val
                (0, 1),        # theta_1
                (0, 1),        # theta_2
                (0, 1)         # theta_3
            ]
        else:
            self.parameter_bounds = parameter_bounds
            
        if initial_params is None:
            self.initial_params = [19.5, -0.25, -0.18, 0.28, 0.22, 0.18]
        else:
            self.initial_params = initial_params
            
        # Simulation parameters
        self.initial_households = initial_households
        self.max_households = max_households
        self.household_increase_threshold = household_increase_threshold
        self.current_households = initial_households
        
        # Random state
        self.random_seed = random_seed
        np.random.seed(random_seed)
        
        # Load or generate empirical moments
        if empirical_moments is not None:
            self.empirical_moments = empirical_moments
        else:
            self.empirical_moments = self._generate_placeholder_empirical_moments()
            
        # Initialize chain storage
        self.chain = []
        self.accepted_chain = []
        self.smm_values = []
        self.acceptance_history = []
        self.household_count_history = []
        
        # Adaptation parameters
        self.adaptation_start = 1000
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
        print(f"Starting with {self.current_households} households")
        
    def _generate_placeholder_empirical_moments(self) -> np.ndarray:
        """Generate placeholder empirical moments using simulation."""
        print("Generating placeholder empirical moments...")
        parameters_to_optimize = [20, -0.2, -0.2, 0.3, 0.2, 0.2]
        
        empirical_moments_list = []
        for _ in range(2):
            households_empirical = create_households(self.initial_households)
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
            
        return True
        
    def _simulate_moments(self, params: List[float]) -> np.ndarray:
        """Simulate moments for given parameters."""
        households_sim = create_households(self.current_households)
        solved_households = solve_households(households_sim, params)
        return moments(solved_households)
        
    def _compute_smm_objective(self, params: List[float], n_simulations: int = 1) -> float:
        """Compute SMM objective function with standardized moments."""
        simulated_moments_list = []
        
        for _ in range(n_simulations):
            sim_moments = self._simulate_moments(params)
            simulated_moments_list.append(sim_moments)
            
        # Average across simulations
        avg_simulated_moments = np.mean(simulated_moments_list, axis=0)
        
        # Compute moment variances if not available (for standardization)
        if self.moment_variances is None:
            # Use empirical moments std as rough estimate, or compute from multiple sims
            self.moment_variances = np.std(simulated_moments_list, axis=0)
            self.moment_variances[self.moment_variances == 0] = 1.0  # Avoid division by zero
            
        # Standardized moment differences
        moment_diff = (avg_simulated_moments - self.empirical_moments) / self.moment_variances
        
        # SMM objective
        smm_obj = moment_diff.T @ self.weighting_matrix @ moment_diff
        
        return smm_obj
        
    def _update_adaptive_covariance(self):
        """Update covariance matrix for adaptive proposals."""
        if len(self.accepted_chain) < self.adaptation_start:
            return
            
        # Convert to numpy array
        chain_array = np.array(self.accepted_chain[-self.adaptation_start:])
        
        # Compute empirical covariance
        self.mean_params = np.mean(chain_array, axis=0)
        cov = np.cov(chain_array, rowvar=False)
        
        # Adaptive Metropolis scaling
        d = len(self.parameter_names)
        scaling = (2.38**2) / d
        epsilon = 0.01
        
        # Regularization for numerical stability
        self.covariance_matrix = scaling * cov + scaling * epsilon * np.eye(d)
        
    def _should_increase_households(self) -> bool:
        """Determine if household count should be increased."""
        if (self.current_households >= self.max_households or 
            len(self.parameter_std_history) < 5):
            return False
            
        # Check if parameter standard deviations have stabilized
        recent_stds = self.parameter_std_history[-5:]
        std_of_stds = np.std(recent_stds, axis=0)
        mean_std_change = np.mean(std_of_stds)
        
        return mean_std_change < self.household_increase_threshold
        
    def _increase_household_count(self):
        """Increase the number of households for simulation."""
        old_count = self.current_households
        self.current_households = min(int(self.current_households * 1.5), self.max_households)
        print(f"Increased household count from {old_count} to {self.current_households}")
        
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
        
    def run_mcmc(self, 
                 n_iterations: int,
                 n_simulations_per_eval: int = 1,  
                 save_every: int = 100,
                 save_prefix: str = "smm_mcmc") -> Dict[str, Any]:
        """
        Run the MCMC chain.
        
        Parameters:
        -----------
        n_iterations : int
            Number of MCMC iterations
        n_simulations_per_eval : int
            Number of simulations per SMM evaluation
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
            current_smm = self._compute_smm_objective(current_params, n_simulations_per_eval)
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
            if (self.covariance_matrix is not None and 
                len(self.accepted_chain) > self.adaptation_start):
                # Use adaptive covariance
                proposal = np.random.multivariate_normal(current_params, self.covariance_matrix)
            else:
                # Use scaled identity matrix
                d = len(self.parameter_names)
                scale = 0.1  # Initial proposal scale
                proposal = current_params + np.random.normal(0, scale, d)
                
            # Check constraints
            if not self._check_parameter_constraints(proposal):
                # Reject proposal
                self.chain.append(current_params)
                self.smm_values.append(current_smm)
                self.acceptance_history.append(False)
            else:
                # Evaluate SMM objective
                proposal_smm = self._compute_smm_objective(proposal, n_simulations_per_eval)
                
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
                else:
                    # Reject proposal
                    self.acceptance_history.append(False)
                    
                self.chain.append(current_params)
                self.smm_values.append(current_smm)
                
            # Update adaptation
            if self.current_iteration > self.adaptation_start:
                self._update_adaptive_covariance()
                
            # Track household count
            self.household_count_history.append(self.current_households)
            
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
            if (iteration + 1) % 100 == 0:
                acceptance_rate = self.n_accepted / self.current_iteration
                current_best_smm = min(self.smm_values)
                avg_time = np.mean(self.computation_times[-100:])
                
                print(f"Iteration {self.current_iteration}: "
                      f"Accept Rate: {acceptance_rate:.3f}, "
                      f"Best SMM: {current_best_smm:.6f}, "
                      f"Households: {self.current_households}, "
                      f"Avg Time: {avg_time:.3f}s")
                      
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
                        n_simulations_per_eval: int = 1,
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
            n_simulations_per_eval=n_simulations_per_eval,
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
            n_simulations_per_eval=n_simulations_per_eval,
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
        
    def _compute_optimal_weighting_matrix(self):
        """Compute optimal weighting matrix from Stage 1 results."""
        if len(self.accepted_chain) < 100:
            print("Warning: Too few accepted samples for optimal weighting matrix")
            return
            
        print("Computing moment variance-covariance matrix...")
        
        # Use recent accepted parameters to compute moment variance
        recent_params = self.accepted_chain[-min(500, len(self.accepted_chain)):]
        
        moment_sims = []
        for params in recent_params[::5]:  # Use every 5th to save computation
            sim_moments = self._simulate_moments(params)
            moment_sims.append(sim_moments)
            
        moment_sims = np.array(moment_sims)
        
        # Compute variance-covariance matrix
        moment_cov = np.cov(moment_sims, rowvar=False)
        
        # Regularize for numerical stability
        regularization = 1e-6 * np.eye(NUM_MOMENTS)
        moment_cov += regularization
        
        # Optimal weighting matrix is inverse of covariance
        try:
            self.weighting_matrix = np.linalg.inv(moment_cov)
            print("Optimal weighting matrix computed successfully")
        except np.linalg.LinAlgError:
            print("Warning: Could not invert moment covariance, using identity")
            self.weighting_matrix = np.eye(NUM_MOMENTS)
            
    def get_parameter_estimates(self) -> Dict[str, Any]:
        """Get parameter estimates and standard errors."""
        if len(self.accepted_chain) < 10:
            return {}
            
        chain_array = np.array(self.accepted_chain)
        
        # Remove burnin (first 20% of samples)
        burnin = max(100, len(chain_array) // 5)
        post_burnin = chain_array[burnin:]
        
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
            'Value': [self.current_iteration, self.n_accepted, self.current_households,
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


# Example usage
if __name__ == "__main__":
    
    # Initialize estimator
    estimator = SMMAdaptiveMetropolis(
        initial_params=[19.5, -0.25, -0.18, 0.28, 0.22, 0.18],
        random_seed=42, 
        initial_households=100, 
        max_households=1000,
        household_increase_threshold=0.05
    )
    
    # Run two-step SMM
    results = estimator.run_two_step_smm(
        stage1_iterations=500,  # Reduced for example
        stage2_iterations=200,   # Reduced for example
        n_simulations_per_eval=1,
        save_prefix="example_run", 
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
    estimator.plot_diagnostics("parameter-estimation/Del_Boca_Replication/estimator_diagnostics.png")
    
    print(f"\nEstimation completed!")
    print(f"Total accepted samples: {len(estimator.accepted_chain)}")
    print(f"Final acceptance rate: {estimator.n_accepted / estimator.current_iteration:.3f}")