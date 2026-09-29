"""
Bijan Taheri (O'Connell Lab)
Summer 2024

Metropolis-Hastings with Robbins-Monro adaptation for CES parameter estimation (NOW DEFUNCT)

NOTE: the following code was written by Claude
"""


# Imports
import numpy as np
import scipy.stats as stats
from scipy.linalg import cholesky, LinAlgError
import matplotlib.pyplot as plt
import time
from Del_Boca_Implementation import create_households, solve_households, moments, simulate_moments


# Constants
NUM_HOUSEHOLDS = 100  # Number of households to simulate
parameters_to_optimize = [0.5, -0.1, 0.1, 0.3, 0.3, 0.25]  # Parameters for "empirical" moments
PARAMETER_NAMES = [
    "Delta", 
    "Rho_val", 
    "Rho_e_val", 
    "theta_1", 
    "theta_2", 
    "theta_3", 
]

class MH_RM_CES_Estimator:
    """
    Metropolis-Hastings with Robbins-Monro adaptation for CES parameter estimation
    
    Designed for Del Boca et al. (2014) style models with CES production functions
    """
    
    def __init__(self, n_params=6, n_households=10000, n_periods=4, n_cores=16):
        print("Initializing MH-RM CES Estimator...")
        self.n_params = n_params  # 6 estimated parameters (theta_4 is derived)
        self.n_households = n_households  
        self.n_periods = n_periods
        self.n_cores = n_cores
        
        # Initialize proposal covariance (start with identity scaled by reasonable values)
        self.proposal_cov = 0.01 * np.eye(n_params)  # Small initial variance
        self.proposal_chol = cholesky(self.proposal_cov, lower=True)
        
        # Adaptation parameters
        self.adaptation_rate = 0.01  # Learning rate for covariance updates
        self.target_acceptance = 0.35  # Target acceptance rate
        self.adaptation_window = 100  # Window for computing acceptance rates
        
        # Storage for adaptation
        self.sample_mean = np.zeros(n_params)
        self.sample_count = 0
        self.recent_accepts = []
        
        # Parameter bounds for the 6 estimated parameters
        self.param_bounds = {
            'Delta': (0.01, 0.99),          # Parent valuation of child human capital
            'rho_val': (-10, 1),       # True substitution parameter (ρ)
            'rho_e_val': (-10, 1),     # Perceived substitution parameter (ρ_e)
            'theta_1': (0.01, 0.96),        # CES share parameter 1
            'theta_2': (0.01, 0.96),        # CES share parameter 2  
            'theta_3': (0.01, 0.96)         # CES share parameter 3
        }
    
    def get_full_parameters(self, estimated_params):
        """
        Convert 6 estimated parameters to full 7-parameter vector
        theta_4 = 1 - theta_1 - theta_2 - theta_3 (CES constraint)
        """
        Delta, rho_val, rho_e_val, theta_1, theta_2, theta_3 = estimated_params
        theta_4 = 1.0 - theta_1 - theta_2 - theta_3
        
        return np.array([Delta, rho_val, rho_e_val, theta_1, theta_2, theta_3, theta_4])
        
    def check_ces_constraint(self, estimated_params):
        """
        Check if theta parameters satisfy CES constraints:
        1. All theta_i > 0
        2. sum(theta_i) = 1  
        3. Rho_val and rho_e_val are less than 1
        """
        _, rho_val, rho_e_val, theta_1, theta_2, theta_3 = estimated_params
        theta_4 = 1.0 - theta_1 - theta_2 - theta_3
        
        # Check all thetas are positive
        if theta_1 <= 0 or theta_2 <= 0 or theta_3 <= 0 or theta_4 <= 0:
            return False
            
        # Check that they sum to approximately 1 (within numerical precision)
        theta_sum = theta_1 + theta_2 + theta_3 + theta_4
        if abs(theta_sum - 1.0) > 1e-10:
            return False
        
        # Check that rho_val and rho_e_val are less than 1
        if rho_val >= 1 or rho_e_val >= 1:
            return False
        
        return True
    
    def log_prior(self, estimated_params):
        """
        Log prior density for the 6 estimated parameters
        Incorporates CES constraint that theta_4 = 1 - theta_1 - theta_2 - theta_3
        """
        if not self._check_bounds(estimated_params):
            return -np.inf
            
        # Check CES constraint
        if not self.check_ces_constraint(estimated_params):
            return -np.inf
            
        Delta, rho_val, rho_e_val, theta_1, theta_2, theta_3 = estimated_params
        theta_4 = 1.0 - theta_1 - theta_2 - theta_3
        
        log_p = 0.0
        
        # Delta: Parent valuation parameter - Beta(2,2) scaled to (0,1)
        log_p += stats.beta.logpdf(Delta, a=2, b=2)
        
        # rho_val: True substitution parameter
        # Normal prior centered at 0 (Cobb-Douglas), allowing for both substitutes and complements
        log_p += stats.norm.logpdf(rho_val, loc=0, scale=2)
        
        # rho_e_val: Perceived substitution parameter  
        # Similar to rho_val but might have different variance if you expect more/less misperception
        log_p += stats.norm.logpdf(rho_e_val, loc=0, scale=2)
        
        # Theta parameters: Dirichlet prior to enforce sum-to-one constraint
        # Using symmetric Dirichlet with concentration parameter > 1 for mild regularization
        theta_vector = np.array([theta_1, theta_2, theta_3, theta_4])
        alpha_dirichlet = np.ones(4) * 2.0  # Symmetric with α = 2 for each component
        
        # Dirichlet log-pdf
        from scipy.special import gammaln
        log_p += (gammaln(np.sum(alpha_dirichlet)) - np.sum(gammaln(alpha_dirichlet)) + 
                 np.sum((alpha_dirichlet - 1) * np.log(theta_vector)))
        
        return log_p
        
    
    def _check_bounds(self, estimated_params):
        """Check if the 6 estimated parameters are within bounds"""
        bounds_list = list(self.param_bounds.values())
        for i, (low, high) in enumerate(bounds_list):
            if not (low < estimated_params[i] < high):
                return False
        return True
    
    def simulate_household_trajectories(self, estimated_params):
        """
        Simulate optimal trajectories for all households using your Del Boca implementation
        This uses the imported functions: create_households, solve_households, moments
        """
        print("Simulating household trajectories...")
        
        try:
            # Create households using your implementation
            households = create_households(self.n_households)
            
            
            
            # Solve households using your implementation  
            solved_households = solve_households(households, estimated_params, num_workers=self.n_cores)
            
            # Calculate moments using your implementation
            simulated_moments = moments(solved_households)
            
            return simulated_moments
            
        except Exception as e:
            print(f"Error in household simulation: {e}")
            # Return a large penalty if simulation fails
            return np.full(24, 1e10)  # Assuming 24 moments based on your moments function
    
    def log_likelihood(self, estimated_params, data_moments):
        """
        Simulation-based log-likelihood using method of simulated moments
        """
        if not self._check_bounds(estimated_params):
            return -np.inf
            
        if not self.check_ces_constraint(estimated_params):
            return -np.inf
            
        try:
            # Get simulated moments from your model (using full 7-parameter vector internally)
            sim_moments = self.simulate_household_trajectories(estimated_params)
            
            # Calculate moment differences (this is your key matching step)
            moment_diff = sim_moments - data_moments
            
            # Weighted quadratic form (adjust weighting matrix as needed)
            # In practice, you'd use optimal weighting matrix from first-stage estimation
            weight_matrix = np.eye(len(moment_diff))  # Identity for now
            
            log_lik = -0.5 * moment_diff.T @ weight_matrix @ moment_diff
            
            return log_lik
            
        except Exception as e:
            print(f"Error in likelihood computation: {e}")
            return -np.inf
    
    def log_posterior(self, estimated_params, data_moments):
        """Combined log posterior for the 6 estimated parameters"""
        return self.log_prior(estimated_params) + self.log_likelihood(estimated_params, data_moments)
    
    def propose_parameters(self, current_params):
        print("Proposing new parameters...")
        """Generate proposal using current covariance matrix"""
        try:
            # Generate proposal using Cholesky decomposition for efficiency
            z = np.random.standard_normal(self.n_params)
            proposal = current_params + self.proposal_chol @ z
            return proposal
        except LinAlgError:
            # Fallback if covariance becomes singular
            print("Warning: Proposal covariance singular, using diagonal")
            proposal = current_params + 0.01 * np.random.standard_normal(self.n_params)
            return proposal
    
    def update_proposal_covariance(self, new_sample, accepted):
        """
        Robbins-Monro update of proposal covariance matrix
        """
        print("Updating proposal covariance...")
        self.sample_count += 1
        self.recent_accepts.append(accepted)
        
        # Keep only recent acceptances for rate calculation
        if len(self.recent_accepts) > self.adaptation_window:
            self.recent_accepts.pop(0)
        
        # Update sample mean
        old_mean = self.sample_mean.copy()
        self.sample_mean += (new_sample - self.sample_mean) / self.sample_count
        
        # Update covariance using Robbins-Monro
        if self.sample_count > 1:
            # Compute outer product update
            mean_diff = new_sample - old_mean
            outer_prod = np.outer(mean_diff, mean_diff)
            
            # Robbins-Monro update with decreasing learning rate
            learning_rate = self.adaptation_rate / (1 + self.sample_count * 0.001)
            self.proposal_cov += learning_rate * (outer_prod - self.proposal_cov)
            
            # Ensure positive definiteness by adding small diagonal term
            self.proposal_cov += 1e-8 * np.eye(self.n_params)
            
            # Scale based on acceptance rate
            if len(self.recent_accepts) >= 20:  # Need enough samples
                acc_rate = np.mean(self.recent_accepts)
                if acc_rate < 0.2:  # Too low acceptance
                    self.proposal_cov *= 0.95
                elif acc_rate > 0.5:  # Too high acceptance  
                    self.proposal_cov *= 1.05
            
            # Update Cholesky decomposition
            try:
                self.proposal_chol = cholesky(self.proposal_cov, lower=True)
            except LinAlgError:
                # If singular, regularize
                self.proposal_cov += 0.001 * np.eye(self.n_params)
                self.proposal_chol = cholesky(self.proposal_cov, lower=True)
    
    def run_chain(self, data_moments, n_iterations=10000, initial_params=None):
        """
        Run single MCMC chain with Robbins-Monro adaptation
        """
        # Initialize
        if initial_params is None:
            # Start from reasonable parameter values
            # [Delta, rho_val, rho_e_val, theta_1, theta_2, theta_3]
            current_params = np.array([0.5, -0.1, 0.1, 0.3, 0.3, 0.25])  # theta_4 = 0.15
        else:
            current_params = initial_params.copy()
        
        current_log_post = self.log_posterior(current_params, data_moments)
        
        # Storage
        samples = np.zeros((n_iterations, self.n_params))
        log_posts = np.zeros(n_iterations)
        accepted_count = 0
        
        print("Starting MH-RM chain...")
        
        for i in range(n_iterations):
            # Propose new parameters
            proposal = self.propose_parameters(current_params)
            proposal_log_post = self.log_posterior(proposal, data_moments)
            
            # Metropolis-Hastings acceptance
            if proposal_log_post > current_log_post:
                # Accept
                current_params = proposal
                current_log_post = proposal_log_post
                accepted = True
                accepted_count += 1
            else:
                # Accept with probability
                log_alpha = proposal_log_post - current_log_post
                if np.log(np.random.random()) < log_alpha:
                    current_params = proposal
                    current_log_post = proposal_log_post
                    accepted = True
                    accepted_count += 1
                else:
                    accepted = False
            
            # Store sample
            samples[i] = current_params
            log_posts[i] = current_log_post
            
            # Update proposal covariance (Robbins-Monro step)
            self.update_proposal_covariance(current_params, accepted)
            
            # Progress reporting
            if (i + 1) % 10 == 0:
                acc_rate = accepted_count / (i + 1)
                print(f"Iteration {i+1}: Acceptance rate = {acc_rate:.3f}")
                
        final_acc_rate = accepted_count / n_iterations
        print(f"Final acceptance rate: {final_acc_rate:.3f}")
        
        return {
            'samples': samples,
            'log_posteriors': log_posts,
            'acceptance_rate': final_acc_rate,
            'final_covariance': self.proposal_cov
        }
    # TODO: Save past draws into a file

def run_parallel_chains(data_moments, n_chains=4, n_iterations=10000):
    """
    Run multiple chains in parallel for convergence diagnostics
    """
    print(f"Running {n_chains} parallel MH-RM chains...")
    
    # Different starting values for each chain
    # [Delta, rho_val, rho_e_val, theta_1, theta_2, theta_3] 
    starting_points = [
        np.array([0.4, -0.5, -0.1, 0.25, 0.35, 0.25]),   # theta_4 = 0.15
        np.array([0.6, 0.5, 0.2, 0.30, 0.30, 0.20]),    # theta_4 = 0.20  
        np.array([0.3, -2, -0.3, 0.40, 0.25, 0.20]),   # theta_4 = 0.15
        np.array([0.7, 0.1, 0.8, 0.20, 0.40, 0.25])     # theta_4 = 0.15
    ]
    
    results = []
    for i in range(n_chains):
        print(f"\nStarting chain {i+1}/{n_chains}")
        estimator = MH_RM_CES_Estimator()
        result = estimator.run_chain(
            data_moments, 
            n_iterations=n_iterations,
            initial_params=starting_points[i] if i < len(starting_points) else None
        )
        results.append(result)
    
    return results

# Example usage
if __name__ == "__main__":
    # You'll need to replace this with your actual empirical moments from real data
    # The moments function returns 24 moments (6 types × 4 periods):
    # - Mean leisure for each period (4 moments)  
    # - Mean expenditure for each period (4 moments)
    # - Mean child HC for each period (4 moments)
    # - Std leisure for each period (4 moments)
    # - Std expenditure for each period (4 moments)  
    # - Std child HC for each period (4 moments)
    
    print("Creating 'empirical' household data for simulation...")
    households_empirical = create_households(1000)
    empirical = np.average([simulate_moments(parameters_to_optimize, households_empirical) for _ in range(2)], axis=0) # Placeholder
    print("Starting MH-RM estimation for CES model with misperceptions...")
    print(f"Estimating 6 parameters: Delta, rho_val, rho_e_val, theta_1, theta_2, theta_3")
    print(f"Using {NUM_HOUSEHOLDS} households and {4} periods per simulation")
    
    # Run estimation
    results = run_parallel_chains(
        data_moments=empirical,
        n_chains=4, 
        n_iterations=1000  # Adjust based on convergence - you may need more
    )
    
    # Basic convergence diagnostics
    print("\n=== CONVERGENCE DIAGNOSTICS ===")
    for i, result in enumerate(results):
        print(f"Chain {i+1}: Acceptance rate = {result['acceptance_rate']:.3f}")
    
    # Combine chains for inference (after discarding burn-in)
    burn_in = 1000
    all_samples = np.vstack([r['samples'][burn_in:] for r in results])
    
    print(f"\nPosterior means (based on {all_samples.shape[0]} samples):")

    with open("ces_estimates.txt", "w") as f:
        f.write("Posterior means and standard deviations (with 95% CI):\n")
        for i, name in enumerate(PARAMETER_NAMES):
            mean_est = np.mean(all_samples[:, i])
            std_est = np.std(all_samples[:, i])
            ci_low = np.percentile(all_samples[:, i], 2.5)
            ci_high = np.percentile(all_samples[:, i], 97.5)
            print(f"{name}: {mean_est:.4f} ± {std_est:.4f} [95% CI: {ci_low:.4f}, {ci_high:.4f}]")
            f.write(f"{name}: {mean_est:.4f} ± {std_est:.4f} [95% CI: {ci_low:.4f}, {ci_high:.4f}]\n")
    
        # Also report implied theta_4 values
        theta_4_samples = 1.0 - all_samples[:, 3] - all_samples[:, 4] - all_samples[:, 5]
        theta_4_mean = np.mean(theta_4_samples)
        theta_4_std = np.std(theta_4_samples)
        theta_4_ci_low = np.percentile(theta_4_samples, 2.5)
        theta_4_ci_high = np.percentile(theta_4_samples, 97.5)
        print(f"theta_4 (share4): {theta_4_mean:.4f} ± {theta_4_std:.4f} [95% CI: {theta_4_ci_low:.4f}, {theta_4_ci_high:.4f}]")
        f.write(f"theta_4: {theta_4_mean:.4f} ± {theta_4_std:.4f} [95% CI: {theta_4_ci_low:.4f}, {theta_4_ci_high:.4f}]\n")