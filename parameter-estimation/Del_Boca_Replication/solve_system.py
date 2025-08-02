import numpy as np
from scipy.optimize import least_squares
import numpy as np
import pandas as pd
from itertools import product


# Empirically known parameters
beta    = 0.96 # discount factor
gamma_  = 2 # Disutility of Leisure
eta     = 2.0 # Risk aversion
phi     = 0.5 # Public-Private Share (Jang & Yum, 2021)
psi     = 0.3 # Public-Private Elasticity of Sub. ψ ≤ 1 (Jang & Yum, 2021)
xi = 2 # I made this up, placeholder removing in future iterations with simpler specification
alpha   = 3 # I made this up, placeholder removing in future iterations with simpler specification
e_bar = 0.1 # Average share of income allocated to parental education investment 
g_bar = 0.01375 # Average US public education expenditure as a share of GDP
eps1, eps2, eps3, eps4 = 1, 1, 1, 1 # Parental productivity
G1, G2, G3, G4         = 1, 1, 1, 1 # Public education productivity

def solve_household(parameters, household): 
    c1, e1, l1, h2, _ = solve_bounded_system_t1(parameters, household)
    c2, e2, l2, h3 = solve_bounded_system_t2(parameters, household, h2)
    c3, e3, l3, h4 = solve_bounded_system_t3(parameters, household, h3)
    c4, e4, l4, h5 = solve_bounded_system_t4(parameters, household, h4)
    return np.array([
        [c1, e1, l1, h2],
        [c2, e2, l2, h3],
        [c3, e3, l3, h4],
        [c4, e4, l4, h5],
    ])



def solve_bounded_system_t1(parameters, household):
    delta   = parameters[0]
    rho = parameters[1]
    rho_e = parameters[2]
    theta1, theta2, theta3 = parameters[3:]
    theta4 = 1 - theta1 - theta2 - theta3

    h = household[0, 0] # zeroth period, zeroth column
    w1, w2, w3, w4 = household[:, 1]
    g1, g2, g3, g4 = household[:, 2]


    # If h1^rho is a constant
    h1_rho = 1.0

    def c_t_star(lt, wt):
        return ((1.0 - lt) * wt * h) / gamma_

    def e_t_star(lt, wt):
        # Ensure lt is at least 1/(gamma_+1) to avoid negative e_t values
        lt_safe = max(lt, 1.0/(gamma_ + 1.0))
        return (wt * h / gamma_)*((gamma_ + 1)*lt_safe - 1.0)

    def h5c(l1, l2, l3, l4):
        e1_val = max(e_t_star(l1, w1), 1e-10)  # Ensure positive values
        e2_val = max(e_t_star(l2, w2), 1e-10)
        e3_val = max(e_t_star(l3, w3), 1e-10)
        e4_val = max(e_t_star(l4, w4), 1e-10)

        # Handle potential numerical issues with negative psi
        term4 = phi*(eps4*e4_val/e_bar)**psi + (1-phi)*(G4*g4/g_bar)**psi
        term3 = phi*(eps3*e3_val/e_bar)**psi + (1-phi)*(G3*g3/g_bar)**psi
        term2 = phi*(eps2*e2_val/e_bar)**psi + (1-phi)*(G2*g2/g_bar)**psi
        term1 = phi*(eps1*e1_val/e_bar)**psi + (1-phi)*(G1*g1/g_bar)**psi

        # Ensure terms are positive before raising to power
        term4_pow = max(term4, 1e-10)**(rho/psi) if term4 > 0 else 1e-10
        term3_pow = max(term3, 1e-10)**(rho/psi) if term3 > 0 else 1e-10
        term2_pow = max(term2, 1e-10)**(rho/psi) if term2 > 0 else 1e-10
        term1_pow = max(term1, 1e-10)**(rho/psi) if term1 > 0 else 1e-10

        bracket = (
            theta4*term4_pow
            + (1-theta4)*theta3*term3_pow
            + (1-theta4)*(1-theta3)*theta2*term2_pow
            + (1-theta4)*(1-theta3)*(1-theta2)*theta1*term1_pow
            + (1-theta4)*(1-theta3)*(1-theta2)*(1-theta1)*h1_rho
        )
        return max(bracket, 1e-10)**(1.0/rho)
    
    def h_2(e1star):
        I_t = (phi*(eps1*e1star/e_bar)**psi + (1-phi)*(G1*g1/g_bar)**psi) ** (rho_e/psi)
        return (theta1 * (I_t) + (1-theta1)*(h1_rho)) ** (1/rho_e)

    def dh5c_de1(l1, l2, l3, l4):
        e1 = max(e_t_star(l1, w1), 1e-10)
        inside = max(phi*(eps1*e1/e_bar)**psi + (1.0-phi)*(G1*g1/g_bar)**psi, 1e-10)
        front = (1-theta4)*(1-theta3)*(1-theta2)*theta1*phi*((eps1/e_bar)**psi)*(e1**(psi - 1.0))
        h5c_val = h5c(l1,l2,l3,l4)
        return front * inside**((rho/psi) - 1.0) * (h5c_val**(1.0 - rho))

    def dh5c_de2(l1, l2, l3, l4):
        e2 = max(e_t_star(l2, w2), 1e-10)
        inside = max(phi*(eps2*e2/e_bar)**psi + (1.0-phi)*(G2*g2/g_bar)**psi, 1e-10)
        front = (1-theta4)*(1-theta3)*theta2*phi*((eps2/e_bar)**psi)*(e2**(psi - 1.0))
        h5c_val = h5c(l1,l2,l3,l4)
        return front * inside**((rho/psi) - 1.0) * (h5c_val**(1.0 - rho))

    def dh5c_de3(l1, l2, l3, l4):
        e3 = max(e_t_star(l3, w3), 1e-10)
        inside = max(phi*(eps3*e3/e_bar)**psi + (1.0-phi)*(G3*g3/g_bar)**psi, 1e-10)
        front = (1-theta4)*theta3*phi*((eps3/e_bar)**psi)*(e3**(psi - 1.0))
        h5c_val = h5c(l1,l2,l3,l4)
        return front * inside**((rho/psi) - 1.0) * (h5c_val**(1.0 - rho))

    def dh5c_de4(l1, l2, l3, l4):
        e4 = max(e_t_star(l4, w4), 1e-10)
        inside = max(phi*(eps4*e4/e_bar)**psi + (1.0-phi)*(G4*g4/g_bar)**psi, 1e-10)
        front = theta4*phi*((eps4/e_bar)**psi)*(e4**(psi - 1.0))
        h5c_val = h5c(l1,l2,l3,l4)
        return front * inside**((rho/psi) - 1.0) * (h5c_val**(1.0 - rho))

    def system(ells):
        l1,l2,l3,l4 = ells
        A = (beta**4)*(alpha*np.log(h+xi) + delta)

        f1 = A*dh5c_de1(l1,l2,l3,l4) \
             - ( c_t_star(l1,w1)*((1.0-l1)**gamma_) )**(-eta)*((1.0-l1)**gamma_)
        f2 = A*dh5c_de2(l1,l2,l3,l4) \
             - beta*( c_t_star(l2,w2)*((1.0-l2)**gamma_) )**(-eta)*((1.0-l2)**gamma_)
        f3 = A*dh5c_de3(l1,l2,l3,l4) \
             - (beta**2)*( c_t_star(l3,w3)*((1.0-l3)**gamma_) )**(-eta)*((1.0-l3)**gamma_)
        f4 = A*dh5c_de4(l1,l2,l3,l4) \
             - (beta**3)*( c_t_star(l4,w4)*((1.0-l4)**gamma_) )**(-eta)*((1.0-l4)**gamma_)
        return [f1,f2,f3,f4]

    # We'll minimize the sum of squares of these residuals with bounds
    def system_resid(ells):
        try:
            return np.array(system(ells)).flatten()  # Flatten the array to ensure 1D
        except (ValueError, RuntimeWarning, RuntimeError):
            # Return a large value if computation fails
            return np.array([1e10, 1e10, 1e10, 1e10])

    # Ensure l_t >= 1/(gamma_+1) to guarantee e_t^* >= 0
    min_l = 1.0/(gamma_ + 1.0)
    bounds_lower = [min_l, min_l, min_l, min_l]
    bounds_upper = [0.9999, 0.9999, 0.9999, 0.9999]  # slightly below 1

    # Try multiple initial guesses to avoid local minima
    best_sol = None
    best_norm = float('inf')
    
    # Add more diverse initial guesses
    initial_guesses = [
        [0.5, 0.5, 0.5, 0.5],
        [0.4, 0.4, 0.4, 0.4],
        [0.6, 0.6, 0.6, 0.6],
        [0.7, 0.7, 0.7, 0.7],
        [min_l + 0.01, min_l + 0.01, min_l + 0.01, min_l + 0.01],  # Slightly above min_l
        [0.9, 0.9, 0.9, 0.9],
        [0.5, 0.6, 0.7, 0.8],
        [0.8, 0.7, 0.6, 0.5],
        [0.4, 0.5, 0.6, 0.7],
        [0.7, 0.6, 0.5, 0.4],
        [0.45, 0.45, 0.45, 0.45],
        [0.55, 0.55, 0.55, 0.55],
        [0.65, 0.65, 0.65, 0.65]
    ]
    
    for guess in initial_guesses:
        try:
            sol = least_squares(
                system_resid,
                guess,
                bounds=(bounds_lower, bounds_upper),
                xtol=1e-12,
                ftol=1e-12,
                method='trf',  # Trust Region Reflective algorithm
                max_nfev=20000
            )
            
            norm = np.linalg.norm(system_resid(sol.x))
            if norm < best_norm:
                best_norm = norm
                best_sol = sol
                
            # Print progress for each guess
            # print(f"Guess {guess}: norm = {norm}, solution = {sol.x}")
        except Exception as e:
            print(f"Failed with guess {guess}: {str(e)}")
            continue
    
    sol = best_sol
    
    # If all solutions are at the lower bound, try a different approach
    if best_sol is not None and np.allclose(best_sol.x, bounds_lower):
        # print("\nTrying a different approach with relaxed bounds...")
        # Try with slightly relaxed lower bounds
        relaxed_bounds_lower = [min_l - 0.05, min_l - 0.05, min_l - 0.05, min_l - 0.05]
        
        for guess in initial_guesses:
            try:
                sol = least_squares(
                    system_resid,
                    guess,
                    bounds=(relaxed_bounds_lower, bounds_upper),
                    xtol=1e-12,
                    ftol=1e-12,
                    method='trf',
                    max_nfev=20000
                )
                
                norm = np.linalg.norm(system_resid(sol.x))
                if norm < best_norm:
                    best_norm = norm
                    best_sol = sol
                    
                # print(f"Relaxed bounds - Guess {guess}: norm = {norm}, solution = {sol.x}")
            except Exception as e:
                # print(f"Failed with relaxed bounds and guess {guess}: {str(e)}")
                continue
        
        sol = best_sol
    if sol == None: 
        print("No solution found with any approach. Returning NaN values.")
        return np.nan, np.nan, np.nan, np.nan, np.nan
    if sol.success:
        # print("\nSuccess!")
        # print("Solution l1,l2,l3,l4 =", sol.x)
        # print("Residuals =", system_resid(sol.x))
        # print("Norm of residuals =", np.linalg.norm(system_resid(sol.x)))
        
        # # Add more diagnostics
        # print("\nDiagnostics:")
        # print("Number of function evaluations:", sol.nfev)
        # print("Termination reason:", sol.message)
        
        # Check if solution is at bounds
        at_lower = np.isclose(sol.x, bounds_lower)
        at_upper = np.isclose(sol.x, bounds_upper)
        # if any(at_lower) or any(at_upper):
        #     print("\nWarning: Solution is at bounds:")
        #     for i, (val, lower, upper) in enumerate(zip(sol.x, bounds_lower, bounds_upper)):
        #         if np.isclose(val, lower):
        #             print(f"l{i+1} is at lower bound ({lower})")
        #         if np.isclose(val, upper):
        #             print(f"l{i+1} is at upper bound ({upper})")
                    
        # Calculate and print e_t, c_t values and final h5c
        # print("\nCalculated e_t values:")
        # print(f"e1 = {e_t_star(sol.x[0], w1)}")
        # print(f"e2 = {e_t_star(sol.x[1], w2)}")
        # print(f"e3 = {e_t_star(sol.x[2], w3)}")
        # print(f"e4 = {e_t_star(sol.x[3], w4)}")
        
        # print("\nCalculated c_t values:")
        # print(f"c1 = {c_t_star(sol.x[0], w1)}")
        # print(f"c2 = {c_t_star(sol.x[1], w2)}")
        # print(f"c3 = {c_t_star(sol.x[2], w3)}")
        # print(f"c4 = {c_t_star(sol.x[3], w4)}")
        
        predh5final = h5c(sol.x[0], sol.x[1], sol.x[2], sol.x[3])
        # print(f"\nFinal h5c value = {predh5final}")
    else:
        print("No solution found with bounding approach. Message:", sol.message)

    e1star = e_t_star(sol.x[0], w1)
    c1star = c_t_star(sol.x[0], w1)

    return c1star, e1star, sol.x[0], h_2(e1star), predh5final





def solve_bounded_system_t2(parameters, household, h2):
    delta   = parameters[0]
    rho = parameters[1]
    rho_e = parameters[2]
    theta1, theta2, theta3 = parameters[3:]
    theta4 = 1 - theta1 - theta2 - theta3

    h = household[0, 0] # zeroth period, zeroth column
    w1, w2, w3, w4 = household[:, 1]
    g1, g2, g3, g4 = household[:, 2]


    # If h1^rho is a constant
    h1_rho = 1.0


    if h2 == np.nan: 
        # This means that solve_bounded_system_t1 failed to find a solution, so we'll return NaN values
        return np.nan, np.nan, np.nan, np.nan

    def c_t_star(lt, wt):
        return ((1.0 - lt) * wt * h) / gamma_

    def e_t_star(lt, wt):
        # Ensure lt is at least 1/(gamma_+1) to avoid negative e_t values
        lt_safe = max(lt, 1.0/(gamma_ + 1.0))
        return (wt * h / gamma_)*((gamma_ + 1)*lt_safe - 1.0)

    # def h5c(l2, l3, l4):
    #     # e1_val = max(e_t_star(l1, w1), 1e-10)  # Ensure positive values
    #     e2_val = max(e_t_star(l2, w2), 1e-10)
    #     e3_val = max(e_t_star(l3, w3), 1e-10)
    #     e4_val = max(e_t_star(l4, w4), 1e-10)

    #     # Handle potential numerical issues with negative psi
    #     term4 = phi*(eps4*e4_val/e_bar)**psi + (1-phi)*(G4*g4/g_bar)**psi
    #     term3 = phi*(eps3*e3_val/e_bar)**psi + (1-phi)*(G3*g3/g_bar)**psi
    #     term2 = phi*(eps2*e2_val/e_bar)**psi + (1-phi)*(G2*g2/g_bar)**psi
    #     # term1 = phi*(eps1*e1_val/e_bar)**psi + (1-phi)*(G1*g1/g_bar)**psi

    #     # Ensure terms are positive before raising to power
    #     term4_pow = max(term4, 1e-10)**(rho/psi) if term4 > 0 else 1e-10
    #     term3_pow = max(term3, 1e-10)**(rho/psi) if term3 > 0 else 1e-10
    #     term2_pow = max(term2, 1e-10)**(rho/psi) if term2 > 0 else 1e-10
    #     # term1_pow = max(term1, 1e-10)**(rho/psi) if term1 > 0 else 1e-10

    #     bracket = (
    #         theta4*term4_pow
    #         + (1-theta4)*theta3*term3_pow
    #         + (1-theta4)*(1-theta3)*theta2*term2_pow
    #         + (1-theta4)*(1-theta3)*(1-theta2)*theta1*term1_pow
    #         + (1-theta4)*(1-theta3)*(1-theta2)*(1-theta1)*h1_rho
    #     )
    #     return max(bracket, 1e-10)**(1.0/rho)
    
    def a2(l2, l3, l4):
        # e1_val = max(e_t_star(l1, w1), 1e-10)  # Ensure positive values
        e2_val = max(e_t_star(l2, w2), 1e-10)
        e3_val = max(e_t_star(l3, w3), 1e-10)
        e4_val = max(e_t_star(l4, w4), 1e-10)

        # Handle potential numerical issues with negative psi
        term4 = phi*(eps4*e4_val/e_bar)**psi + (1-phi)*(G4*g4/g_bar)**psi
        term3 = phi*(eps3*e3_val/e_bar)**psi + (1-phi)*(G3*g3/g_bar)**psi
        term2 = phi*(eps2*e2_val/e_bar)**psi + (1-phi)*(G2*g2/g_bar)**psi
        # term1 = phi*(eps1*e1_val/e_bar)**psi + (1-phi)*(G1*g1/g_bar)**psi

        # Ensure terms are positive before raising to power
        term4_pow = max(term4, 1e-10)**(rho/psi) if term4 > 0 else 1e-10
        term3_pow = max(term3, 1e-10)**(rho/psi) if term3 > 0 else 1e-10
        term2_pow = max(term2, 1e-10)**(rho/psi) if term2 > 0 else 1e-10
        # term1_pow = max(term1, 1e-10)**(rho/psi) if term1 > 0 else 1e-10

        bracket = (
            theta4*term4_pow
            + (1-theta4)*theta3*term3_pow
            + (1-theta4)*(1-theta3)*theta2*term2_pow
            + (1-theta4)*(1-theta3)*(1-theta2)*(h2**rho)
            # + (1-theta4)*(1-theta3)*(1-theta2)*(1-theta1)*h1_rho
        )
        return max(bracket, 1e-10)
    
    def h_3(e2star):
        I_t = (phi*(eps2*e2star/e_bar)**psi + (1-phi)*(G2*g2/g_bar)**psi) ** (1/psi)
        return (theta2 * (I_t ** rho_e) + (1-theta2)*(h2**rho_e)) ** (1/rho_e)

    # def dh5c_de1(l1, l2, l3, l4):
    #     e1 = max(e_t_star(l1, w1), 1e-10)
    #     inside = max(phi*(eps1*e1/e_bar)**psi + (1.0-phi)*(G1*g1/g_bar)**psi, 1e-10)
    #     front = (1-theta4)*(1-theta3)*(1-theta2)*theta1*phi*((eps1/e_bar)**psi)*(e1**(psi - 1.0))
    #     h5c_val = h5c(l1,l2,l3,l4)
    #     return front * inside**((rho/psi) - 1.0) * (h5c_val**(1.0 - rho))

    def dh5c_de2(l2, l3, l4):
        e2 = max(e_t_star(l2, w2), 1e-10)
        inside = max(phi*(eps2*e2/e_bar)**psi + (1.0-phi)*(G2*g2/g_bar)**psi, 1e-10)
        front = (1-theta4)*(1-theta3)*theta2*phi*((eps2/e_bar)**psi)*(e2**(psi - 1.0))
        a2_val = a2(l2,l3,l4)

        return front * inside**((rho/psi) - 1.0) * (a2_val**(1/rho - 1))

    def dh5c_de3(l2, l3, l4):
        e3 = max(e_t_star(l3, w3), 1e-10)
        inside = max(phi*(eps3*e3/e_bar)**psi + (1.0-phi)*(G3*g3/g_bar)**psi, 1e-10)
        front = (1-theta4)*theta3*phi*((eps3/e_bar)**psi)*(e3**(psi - 1.0))
        a2_val = a2(l2,l3,l4)
        return front * inside**((rho/psi) - 1.0) * (a2_val**(1/rho - 1))

    def dh5c_de4(l2, l3, l4):
        e4 = max(e_t_star(l4, w4), 1e-10)
        inside = max(phi*(eps4*e4/e_bar)**psi + (1.0-phi)*(G4*g4/g_bar)**psi, 1e-10)
        front = theta4*phi*((eps4/e_bar)**psi)*(e4**(psi - 1.0))
        a2_val = a2(l2,l3,l4)
        return front * inside**((rho/psi) - 1.0) * (a2_val**(1/rho - 1))

    def system(ells):
        l2,l3,l4 = ells
        A = (beta**4)*(alpha*np.log(h+xi) + delta)

        # f1 = A*dh5c_de1(l1,l2,l3,l4) \
        #      - ( c_t_star(l1,w1)*((1.0-l1)**gamma_) )**(-eta)*((1.0-l1)**gamma_)
        f2 = A*dh5c_de2(l2,l3,l4) \
             - beta*( c_t_star(l2,w2)*((1.0-l2)**gamma_) )**(-eta)*((1.0-l2)**gamma_)
        f3 = A*dh5c_de3(l2,l3,l4) \
             - (beta**2)*( c_t_star(l3,w3)*((1.0-l3)**gamma_) )**(-eta)*((1.0-l3)**gamma_)
        f4 = A*dh5c_de4(l2,l3,l4) \
             - (beta**3)*( c_t_star(l4,w4)*((1.0-l4)**gamma_) )**(-eta)*((1.0-l4)**gamma_)
        return [f2,f3,f4]

    # We'll minimize the sum of squares of these residuals with bounds
    def system_resid(ells):
        try:
            return np.array(system(ells))
        except (ValueError, RuntimeWarning, RuntimeError):
            # Return a large value if computation fails
            return np.array([1e10, 1e10, 1e10])

    # Ensure l_t >= 1/(gamma_+1) to guarantee e_t^* >= 0
    min_l = 1.0/(gamma_ + 1.0)
    bounds_lower = [min_l, min_l, min_l]
    bounds_upper = [0.9999, 0.9999, 0.9999]  # slightly below 1

    # Try multiple initial guesses to avoid local minima
    best_sol = None
    best_norm = float('inf')
    
    # Add more diverse initial guesses
    initial_guesses = [
        [0.5, 0.5, 0.5],
        [0.4, 0.4, 0.4],
        [0.6, 0.6, 0.6],
        [0.7, 0.7, 0.7],
        [min_l + 0.01, min_l + 0.01, min_l + 0.01],  # Slightly above min_l
        [0.9, 0.9, 0.9],
        [0.6, 0.7, 0.8],
        [0.7, 0.6, 0.5],
        [0.5, 0.6, 0.7],
        [0.6, 0.5, 0.4],
        [0.45, 0.45, 0.45],
        [0.55, 0.55, 0.55],
        [0.65, 0.65, 0.65]
    ]
    
    for guess in initial_guesses:
        try:
            sol = least_squares(
                system_resid,
                guess,
                bounds=(bounds_lower, bounds_upper),
                xtol=1e-12,
                ftol=1e-12,
                method='trf',  # Trust Region Reflective algorithm
                max_nfev=20000
            )
            
            norm = np.linalg.norm(system_resid(sol.x))
            if norm < best_norm:
                best_norm = norm
                best_sol = sol
                
            # Print progress for each guess
            # print(f"Guess {guess}: norm = {norm}, solution = {sol.x}")
        except Exception as e:
            print(f"Failed with guess {guess}: {str(e)}")
            continue
    
    sol = best_sol
    
    # If all solutions are at the lower bound, try a different approach
    if best_sol is not None and np.allclose(best_sol.x, bounds_lower):
        # print("\nTrying a different approach with relaxed bounds...")
        # Try with slightly relaxed lower bounds
        relaxed_bounds_lower = [min_l - 0.05, min_l - 0.05, min_l - 0.05]
        
        for guess in initial_guesses:
            try:
                sol = least_squares(
                    system_resid,
                    guess,
                    bounds=(relaxed_bounds_lower, bounds_upper),
                    xtol=1e-12,
                    ftol=1e-12,
                    method='trf',
                    max_nfev=20000
                )
                
                norm = np.linalg.norm(system_resid(sol.x))
                if norm < best_norm:
                    best_norm = norm
                    best_sol = sol
                    
                # print(f"Relaxed bounds - Guess {guess}: norm = {norm}, solution = {sol.x}")
            except Exception as e:
                print(f"Failed with relaxed bounds and guess {guess}: {str(e)}")
                continue
        
        sol = best_sol
    if sol == None: 
        print("No solution found with any approach. Returning NaN values.")
        return np.nan, np.nan, np.nan, np.nan
    if sol.success:
        # print("\nSuccess!")
        # print("Solution l2,l3,l4 =", sol.x)
        # print("Residuals =", system_resid(sol.x))
        # print("Norm of residuals =", np.linalg.norm(system_resid(sol.x)))
        
        # # Add more diagnostics
        # print("\nDiagnostics:")
        # print("Number of function evaluations:", sol.nfev)
        # print("Termination reason:", sol.message)
        
        # Check if solution is at bounds
        at_lower = np.isclose(sol.x, bounds_lower)
        at_upper = np.isclose(sol.x, bounds_upper)
        # if any(at_lower) or any(at_upper):
        #     print("\nWarning: Solution is at bounds:")
        #     for i, (val, lower, upper) in enumerate(zip(sol.x, bounds_lower, bounds_upper)):
        #         if np.isclose(val, lower):
        #             print(f"l{i+1} is at lower bound ({lower})")
        #         if np.isclose(val, upper):
        #             print(f"l{i+1} is at upper bound ({upper})")
                    
        # Calculate and print e_t, c_t values and final h5c
        # print("\nCalculated e_t values:")
        # print(f"e2 = {e_t_star(sol.x[0], w2)}")
        # print(f"e3 = {e_t_star(sol.x[1], w3)}")
        # print(f"e4 = {e_t_star(sol.x[2], w4)}")
        # print(f"e4 = {e_t_star(sol.x[3], w4)}")
        
        # print("\nCalculated c_t values:")
        # print(f"c2 = {c_t_star(sol.x[0], w2)}")
        # print(f"c3 = {c_t_star(sol.x[1], w3)}")
        # print(f"c4 = {c_t_star(sol.x[2], w4)}")
        # print(f"c4 = {c_t_star(sol.x[3], w4)}")
        
        # print(f"\nFinal h5c value = {h5c(sol.x[0], sol.x[1], sol.x[2])}")
    else:
        print("No solution found with bounding approach. Message:", sol.message)

    e2star = e_t_star(sol.x[0], w2)
    c2star = c_t_star(sol.x[0], w2)

    return c2star, e2star, sol.x[0], h_3(e2star)


def solve_bounded_system_t3(parameters, household, h3):
    # Parameters to optimize
    delta   = parameters[0]
    rho = parameters[1]
    rho_e = parameters[2]
    theta1, theta2, theta3 = parameters[3:]
    theta4 = 1 - theta1 - theta2 - theta3

    # Household-level parameters
    h = household[0, 0] # zeroth period, zeroth column
    w1, w2, w3, w4 = household[:, 1]
    g1, g2, g3, g4         = household[:, 2]


    # If h1^rho is a constant
    h1_rho = 1.0

    if h3 == np.nan:
        # This means that solve_bounded_system_t2 failed to find a solution, so we'll return NaN values
        return np.nan, np.nan, np.nan, np.nan

    def c_t_star(lt, wt):
        return ((1.0 - lt) * wt * h) / gamma_

    def e_t_star(lt, wt):
        # Ensure lt is at least 1/(gamma_+1) to avoid negative e_t values
        lt_safe = max(lt, 1.0/(gamma_ + 1.0))
        return (wt * h / gamma_)*((gamma_ + 1)*lt_safe - 1.0)

    # def h5c(l2, l3, l4):
    #     # e1_val = max(e_t_star(l1, w1), 1e-10)  # Ensure positive values
    #     e2_val = max(e_t_star(l2, w2), 1e-10)
    #     e3_val = max(e_t_star(l3, w3), 1e-10)
    #     e4_val = max(e_t_star(l4, w4), 1e-10)

    #     # Handle potential numerical issues with negative psi
    #     term4 = phi*(eps4*e4_val/e_bar)**psi + (1-phi)*(G4*g4/g_bar)**psi
    #     term3 = phi*(eps3*e3_val/e_bar)**psi + (1-phi)*(G3*g3/g_bar)**psi
    #     term2 = phi*(eps2*e2_val/e_bar)**psi + (1-phi)*(G2*g2/g_bar)**psi
    #     # term1 = phi*(eps1*e1_val/e_bar)**psi + (1-phi)*(G1*g1/g_bar)**psi

    #     # Ensure terms are positive before raising to power
    #     term4_pow = max(term4, 1e-10)**(rho/psi) if term4 > 0 else 1e-10
    #     term3_pow = max(term3, 1e-10)**(rho/psi) if term3 > 0 else 1e-10
    #     term2_pow = max(term2, 1e-10)**(rho/psi) if term2 > 0 else 1e-10
    #     # term1_pow = max(term1, 1e-10)**(rho/psi) if term1 > 0 else 1e-10

    #     bracket = (
    #         theta4*term4_pow
    #         + (1-theta4)*theta3*term3_pow
    #         + (1-theta4)*(1-theta3)*theta2*term2_pow
    #         + (1-theta4)*(1-theta3)*(1-theta2)*theta1*term1_pow
    #         + (1-theta4)*(1-theta3)*(1-theta2)*(1-theta1)*h1_rho
    #     )
    #     return max(bracket, 1e-10)**(1.0/rho)
    
    def a2(l3, l4):
        # e1_val = max(e_t_star(l1, w1), 1e-10)  # Ensure positive values
        # e2_val = max(e_t_star(l2, w2), 1e-10)
        e3_val = max(e_t_star(l3, w3), 1e-10)
        e4_val = max(e_t_star(l4, w4), 1e-10)

        # Handle potential numerical issues with negative psi
        term4 = phi*(eps4*e4_val/e_bar)**psi + (1-phi)*(G4*g4/g_bar)**psi
        term3 = phi*(eps3*e3_val/e_bar)**psi + (1-phi)*(G3*g3/g_bar)**psi
        # term2 = phi*(eps2*e2_val/e_bar)**psi + (1-phi)*(G2*g2/g_bar)**psi
        # term1 = phi*(eps1*e1_val/e_bar)**psi + (1-phi)*(G1*g1/g_bar)**psi

        # Ensure terms are positive before raising to power
        term4_pow = max(term4, 1e-10)**(rho/psi) if term4 > 0 else 1e-10
        term3_pow = max(term3, 1e-10)**(rho/psi) if term3 > 0 else 1e-10
        # term2_pow = max(term2, 1e-10)**(rho/psi) if term2 > 0 else 1e-10
        # term1_pow = max(term1, 1e-10)**(rho/psi) if term1 > 0 else 1e-10

        bracket = (
            theta4*term4_pow
            + (1-theta4)*theta3*term3_pow
            + (1-theta4)*(1-theta3)*(h3**rho)
            # + (1-theta4)*(1-theta3)*(1-theta2)*(h2**rho)
            # + (1-theta4)*(1-theta3)*(1-theta2)*(1-theta1)*h1_rho
        )
        return max(bracket, 1e-10)
    
    def h_4(e3star):
        I_t = (phi*(eps3*e3star/e_bar)**psi + (1-phi)*(G3*g3/g_bar)**psi)**(1/psi)
        return (theta3 * (I_t ** rho_e) + (1-theta3)*(h3**rho_e)) ** (1/rho_e)

    # def dh5c_de1(l1, l2, l3, l4):
    #     e1 = max(e_t_star(l1, w1), 1e-10)
    #     inside = max(phi*(eps1*e1/e_bar)**psi + (1.0-phi)*(G1*g1/g_bar)**psi, 1e-10)
    #     front = (1-theta4)*(1-theta3)*(1-theta2)*theta1*phi*((eps1/e_bar)**psi)*(e1**(psi - 1.0))
    #     h5c_val = h5c(l1,l2,l3,l4)
    #     return front * inside**((rho/psi) - 1.0) * (h5c_val**(1.0 - rho))

    # def dh5c_de2(l2, l3, l4):
    #     e2 = max(e_t_star(l2, w2), 1e-10)
    #     inside = max(phi*(eps2*e2/e_bar)**psi + (1.0-phi)*(G2*g2/g_bar)**psi, 1e-10)
    #     front = (1-theta4)*(1-theta3)*theta2*phi*((eps2/e_bar)**psi)*(e2**(psi - 1.0))
    #     a2_val = a2(l2,l3,l4)

    #     return front * inside**((rho/psi) - 1.0) * (a2_val**(1/rho - 1))

    def dh5c_de3(l3, l4):
        e3 = max(e_t_star(l3, w3), 1e-10)
        inside = max(phi*(eps3*e3/e_bar)**psi + (1.0-phi)*(G3*g3/g_bar)**psi, 1e-10)
        front = (1-theta4)*theta3*phi*((eps3/e_bar)**psi)*(e3**(psi - 1.0))
        a2_val = a2(l3,l4)
        return front * inside**((rho/psi) - 1.0) * (a2_val**(1/rho - 1))

    def dh5c_de4(l3, l4):
        e4 = max(e_t_star(l4, w4), 1e-10)
        inside = max(phi*(eps4*e4/e_bar)**psi + (1.0-phi)*(G4*g4/g_bar)**psi, 1e-10)
        front = theta4*phi*((eps4/e_bar)**psi)*(e4**(psi - 1.0))
        a2_val = a2(l3,l4)
        return front * inside**((rho/psi) - 1.0) * (a2_val**(1/rho - 1))

    def system(ells):
        l3,l4 = ells
        A = (beta**4)*(alpha*np.log(h+xi) + delta)

        # f1 = A*dh5c_de1(l1,l2,l3,l4) \
        #      - ( c_t_star(l1,w1)*((1.0-l1)**gamma_) )**(-eta)*((1.0-l1)**gamma_)
        # f2 = A*dh5c_de2(l2,l3,l4) \
        #      - beta*( c_t_star(l2,w2)*((1.0-l2)**gamma_) )**(-eta)*((1.0-l2)**gamma_)
        f3 = A*dh5c_de3(l3,l4) \
             - (beta**2)*( c_t_star(l3,w3)*((1.0-l3)**gamma_) )**(-eta)*((1.0-l3)**gamma_)
        f4 = A*dh5c_de4(l3,l4) \
             - (beta**3)*( c_t_star(l4,w4)*((1.0-l4)**gamma_) )**(-eta)*((1.0-l4)**gamma_)
        return [f3,f4]

    # We'll minimize the sum of squares of these residuals with bounds
    def system_resid(ells):
        try:
            return np.array(system(ells))
        except (ValueError, RuntimeWarning, RuntimeError):
            # Return a large value if computation fails
            return np.array([1e10, 1e10])

    # Ensure l_t >= 1/(gamma_+1) to guarantee e_t^* >= 0
    min_l = 1.0/(gamma_ + 1.0)
    bounds_lower = [min_l, min_l]
    bounds_upper = [0.9999, 0.9999]  # slightly below 1

    # Try multiple initial guesses to avoid local minima
    best_sol = None
    best_norm = float('inf')
    
    # Add more diverse initial guesses
    initial_guesses = [
        [0.5, 0.5],
        [0.4, 0.4],
        [0.6, 0.6],
        [0.7, 0.7],
        [min_l + 0.01, min_l + 0.01],  # Slightly above min_l
        [0.9, 0.9],
        [0.6, 0.7],
        [0.7, 0.6],
        [0.5, 0.6],
        [0.6, 0.5],
        [0.45, 0.45],
        [0.55, 0.55],
        [0.65, 0.65]
    ]
    
    for guess in initial_guesses:
        try:
            sol = least_squares(
                system_resid,
                guess,
                bounds=(bounds_lower, bounds_upper),
                xtol=1e-12,
                ftol=1e-12,
                method='trf',  # Trust Region Reflective algorithm
                max_nfev=20000
            )
            
            norm = np.linalg.norm(system_resid(sol.x))
            if norm < best_norm:
                best_norm = norm
                best_sol = sol
                
            # Print progress for each guess
            # print(f"Guess {guess}: norm = {norm}, solution = {sol.x}")
        except Exception as e:
            print(f"Failed with guess {guess}: {str(e)}")
            continue
    
    sol = best_sol
    
    # If all solutions are at the lower bound, try a different approach
    if best_sol is not None and np.allclose(best_sol.x, bounds_lower):
        # print("\nTrying a different approach with relaxed bounds...")
        # Try with slightly relaxed lower bounds
        relaxed_bounds_lower = [min_l - 0.05, min_l - 0.05]
        
        for guess in initial_guesses:
            try:
                sol = least_squares(
                    system_resid,
                    guess,
                    bounds=(relaxed_bounds_lower, bounds_upper),
                    xtol=1e-12,
                    ftol=1e-12,
                    method='trf',
                    max_nfev=20000
                )
                
                norm = np.linalg.norm(system_resid(sol.x))
                if norm < best_norm:
                    best_norm = norm
                    best_sol = sol
                    
                # print(f"Relaxed bounds - Guess {guess}: norm = {norm}, solution = {sol.x}")
            except Exception as e:
                print(f"Failed with relaxed bounds and guess {guess}: {str(e)}")
                continue
        
        sol = best_sol
    if sol == None:
        print("No solution found with any approach. Returning NaN values.")
        return np.nan, np.nan, np.nan, np.nan
    if sol.success:
        # print("\nSuccess!")
        # print("Solution l3,l4 =", sol.x)
        # print("Residuals =", system_resid(sol.x))
        # print("Norm of residuals =", np.linalg.norm(system_resid(sol.x)))
        
        # # Add more diagnostics
        # print("\nDiagnostics:")
        # print("Number of function evaluations:", sol.nfev)
        # print("Termination reason:", sol.message)
        
        # Check if solution is at bounds
        at_lower = np.isclose(sol.x, bounds_lower)
        at_upper = np.isclose(sol.x, bounds_upper)
        # if any(at_lower) or any(at_upper):
        #     print("\nWarning: Solution is at bounds:")
        #     for i, (val, lower, upper) in enumerate(zip(sol.x, bounds_lower, bounds_upper)):
        #         if np.isclose(val, lower):
        #             print(f"l{i+1} is at lower bound ({lower})")
        #         if np.isclose(val, upper):
        #             print(f"l{i+1} is at upper bound ({upper})")
                    
        # Calculate and print e_t, c_t values and final h5c
        # print("\nCalculated e_t values:")
        # print(f"e3 = {e_t_star(sol.x[0], w3)}")
        # print(f"e4 = {e_t_star(sol.x[1], w4)}")
        # print(f"e4 = {e_t_star(sol.x[2], w4)}")
        # print(f"e4 = {e_t_star(sol.x[3], w4)}")
        
        # print("\nCalculated c_t values:")
        # print(f"c3 = {c_t_star(sol.x[0], w3)}")
        # print(f"c4 = {c_t_star(sol.x[1], w4)}")
        # print(f"c4 = {c_t_star(sol.x[2], w4)}")
        # print(f"c4 = {c_t_star(sol.x[3], w4)}")
        
        # print(f"\nFinal h5c value = {h5c(sol.x[0], sol.x[1], sol.x[2])}")
    else:
        print("No solution found with bounding approach. Message:", sol.message)

    e3star = e_t_star(sol.x[0], w3)
    c3star = c_t_star(sol.x[0], w3)
    return c3star, e3star, sol.x[0], h_4(e3star)


def solve_bounded_system_t4(parameters, household, h4):
    delta   = parameters[0]
    rho = parameters[1]
    rho_e = parameters[2]
    theta1, theta2, theta3 = parameters[3:]
    theta4 = 1 - theta1 - theta2 - theta3

    h = household[0, 0] # zeroth period, zeroth column
    w1, w2, w3, w4 = household[:, 1]
    g1, g2, g3, g4         = household[:, 2]


    # If h1^rho is a constant
    h1_rho = 1.0

    if h4 == np.nan:
        # This means that solve_bounded_system_t3 failed to find a solution, so we'll return NaN values
        return np.nan, np.nan, np.nan, np.nan

    def c_t_star(lt, wt):
        return ((1.0 - lt) * wt * h) / gamma_

    def e_t_star(lt, wt):
        # Ensure lt is at least 1/(gamma_+1) to avoid negative e_t values
        lt_safe = max(lt, 1.0/(gamma_ + 1.0))
        return (wt * h / gamma_)*((gamma_ + 1)*lt_safe - 1.0)

    # def h5c(l2, l3, l4):
    #     # e1_val = max(e_t_star(l1, w1), 1e-10)  # Ensure positive values
    #     e2_val = max(e_t_star(l2, w2), 1e-10)
    #     e3_val = max(e_t_star(l3, w3), 1e-10)
    #     e4_val = max(e_t_star(l4, w4), 1e-10)

    #     # Handle potential numerical issues with negative psi
    #     term4 = phi*(eps4*e4_val/e_bar)**psi + (1-phi)*(G4*g4/g_bar)**psi
    #     term3 = phi*(eps3*e3_val/e_bar)**psi + (1-phi)*(G3*g3/g_bar)**psi
    #     term2 = phi*(eps2*e2_val/e_bar)**psi + (1-phi)*(G2*g2/g_bar)**psi
    #     # term1 = phi*(eps1*e1_val/e_bar)**psi + (1-phi)*(G1*g1/g_bar)**psi

    #     # Ensure terms are positive before raising to power
    #     term4_pow = max(term4, 1e-10)**(rho/psi) if term4 > 0 else 1e-10
    #     term3_pow = max(term3, 1e-10)**(rho/psi) if term3 > 0 else 1e-10
    #     term2_pow = max(term2, 1e-10)**(rho/psi) if term2 > 0 else 1e-10
    #     # term1_pow = max(term1, 1e-10)**(rho/psi) if term1 > 0 else 1e-10

    #     bracket = (
    #         theta4*term4_pow
    #         + (1-theta4)*theta3*term3_pow
    #         + (1-theta4)*(1-theta3)*theta2*term2_pow
    #         + (1-theta4)*(1-theta3)*(1-theta2)*theta1*term1_pow
    #         + (1-theta4)*(1-theta3)*(1-theta2)*(1-theta1)*h1_rho
    #     )
    #     return max(bracket, 1e-10)**(1.0/rho)
    
    def a2(l4):
        # e1_val = max(e_t_star(l1, w1), 1e-10)  # Ensure positive values
        # e2_val = max(e_t_star(l2, w2), 1e-10)
        # e3_val = max(e_t_star(l3, w3), 1e-10)
        e4_val = max(e_t_star(l4, w4), 1e-10)

        # Handle potential numerical issues with negative psi
        term4 = phi*(eps4*e4_val/e_bar)**psi + (1-phi)*(G4*g4/g_bar)**psi
        # term3 = phi*(eps3*e3_val/e_bar)**psi + (1-phi)*(G3*g3/g_bar)**psi
        # term2 = phi*(eps2*e2_val/e_bar)**psi + (1-phi)*(G2*g2/g_bar)**psi
        # term1 = phi*(eps1*e1_val/e_bar)**psi + (1-phi)*(G1*g1/g_bar)**psi

        # Ensure terms are positive before raising to power
        term4_pow = max(term4, 1e-10)**(rho/psi) if term4 > 0 else 1e-10
        # term3_pow = max(term3, 1e-10)**(rho/psi) if term3 > 0 else 1e-10
        # term2_pow = max(term2, 1e-10)**(rho/psi) if term2 > 0 else 1e-10
        # term1_pow = max(term1, 1e-10)**(rho/psi) if term1 > 0 else 1e-10

        bracket = (
            theta4*term4_pow
            + (1-theta4)*(h4**rho)
            # + (1-theta4)*(1-theta3)*(h3**rho)
            # + (1-theta4)*(1-theta3)*(1-theta2)*(h2**rho)
            # + (1-theta4)*(1-theta3)*(1-theta2)*(1-theta1)*h1_rho
        )
        return max(bracket, 1e-10)
    
    def h_5(e4star):
        I_t = (phi*(eps4*e4star/e_bar)**psi + (1-phi)*(G4*g4/g_bar)**psi) ** (1/psi)
        return (theta4 * (I_t ** rho_e) + (1-theta4)*(h4**rho_e)) ** (1/rho_e)

    # def dh5c_de1(l1, l2, l3, l4):
    #     e1 = max(e_t_star(l1, w1), 1e-10)
    #     inside = max(phi*(eps1*e1/e_bar)**psi + (1.0-phi)*(G1*g1/g_bar)**psi, 1e-10)
    #     front = (1-theta4)*(1-theta3)*(1-theta2)*theta1*phi*((eps1/e_bar)**psi)*(e1**(psi - 1.0))
    #     h5c_val = h5c(l1,l2,l3,l4)
    #     return front * inside**((rho/psi) - 1.0) * (h5c_val**(1.0 - rho))

    # def dh5c_de2(l2, l3, l4):
    #     e2 = max(e_t_star(l2, w2), 1e-10)
    #     inside = max(phi*(eps2*e2/e_bar)**psi + (1.0-phi)*(G2*g2/g_bar)**psi, 1e-10)
    #     front = (1-theta4)*(1-theta3)*theta2*phi*((eps2/e_bar)**psi)*(e2**(psi - 1.0))
    #     a2_val = a2(l2,l3,l4)

    #     return front * inside**((rho/psi) - 1.0) * (a2_val**(1/rho - 1))

    # def dh5c_de3(l4):
    #     e3 = max(e_t_star(l3, w3), 1e-10)
    #     inside = max(phi*(eps3*e3/e_bar)**psi + (1.0-phi)*(G3*g3/g_bar)**psi, 1e-10)
    #     front = (1-theta4)*theta3*phi*((eps3/e_bar)**psi)*(e3**(psi - 1.0))
    #     a2_val = a2(l3,l4)
    #     return front * inside**((rho/psi) - 1.0) * (a2_val**(1/rho - 1))

    def dh5c_de4(l4):
        e4 = max(e_t_star(l4, w4), 1e-10)
        inside = max(phi*(eps4*e4/e_bar)**psi + (1.0-phi)*(G4*g4/g_bar)**psi, 1e-10)
        front = theta4*phi*((eps4/e_bar)**psi)*(e4**(psi - 1.0))
        a2_val = a2(l4)
        return front * inside**((rho/psi) - 1.0) * (a2_val**(1/rho - 1))

    def system(ells):
        l4 = ells
        A = (beta**4)*(alpha*np.log(h+xi) + delta)

        # f1 = A*dh5c_de1(l1,l2,l3,l4) \
        #      - ( c_t_star(l1,w1)*((1.0-l1)**gamma_) )**(-eta)*((1.0-l1)**gamma_)
        # f2 = A*dh5c_de2(l2,l3,l4) \
        #      - beta*( c_t_star(l2,w2)*((1.0-l2)**gamma_) )**(-eta)*((1.0-l2)**gamma_)
        # f3 = A*dh5c_de3(l3,l4) \
        #      - (beta**2)*( c_t_star(l3,w3)*((1.0-l3)**gamma_) )**(-eta)*((1.0-l3)**gamma_)
        f4 = A*dh5c_de4(l4) \
             - (beta**3)*( c_t_star(l4,w4)*((1.0-l4)**gamma_) )**(-eta)*((1.0-l4)**gamma_)
        return [f4]

    # We'll minimize the sum of squares of these residuals with bounds
    def system_resid(ells):
        try:
            return np.array(system(ells)).flatten()  # Flatten the array to ensure 1D
        except (ValueError, RuntimeWarning, RuntimeError):
            # Return a large value if computation fails
            return np.array([1e10])  # Return a 1D array with one element

    # Ensure l_t >= 1/(gamma_+1) to guarantee e_t^* >= 0
    min_l = 1.0/(gamma_ + 1.0)
    bounds_lower = [min_l]
    bounds_upper = [0.9999]  # slightly below 1

    # Try multiple initial guesses to avoid local minima
    best_sol = None
    best_norm = float('inf')
    
    # Add more diverse initial guesses
    initial_guesses = [
        [0.5],
        [0.4],
        [0.6],
        [0.7],
        [min_l + 0.01],  # Slightly above min_l
        [0.9],
        [0.45],
        [0.55],
        [0.65],
        [0.75],
        [0.85],
        [0.3], # NOTE: 0.3 is always failing as a guess. Why?
        [0.8]
    ]
    for guess in initial_guesses:
        try:
            sol = least_squares(
                system_resid,
                guess,
                bounds=(bounds_lower, bounds_upper),
                xtol=1e-12,
                ftol=1e-12,
                method='trf',  # Trust Region Reflective algorithm
                max_nfev=20000
            )
            
            norm = np.linalg.norm(system_resid(sol.x))
            if norm < best_norm:
                best_norm = norm
                best_sol = sol
                
            # Print progress for each guess
            # print(f"Guess {guess}: norm = {norm}, solution = {sol.x}")
        except Exception as e:
            # print(f"Failed with guess {guess}: {str(e)}")
            continue
    
    sol = best_sol
    
    # If all solutions are at the lower bound, try a different approach
    if best_sol is not None and np.allclose(best_sol.x, bounds_lower):
        # print("\nTrying a different approach with relaxed bounds...")
        # Try with slightly relaxed lower bounds
        relaxed_bounds_lower = [min_l - 0.05]
        
        for guess in initial_guesses:
            try:
                sol = least_squares(
                    system_resid,
                    guess,
                    bounds=(relaxed_bounds_lower, bounds_upper),
                    xtol=1e-12,
                    ftol=1e-12,
                    method='trf',
                    max_nfev=20000
                )
                
                norm = np.linalg.norm(system_resid(sol.x))
                if norm < best_norm:
                    best_norm = norm
                    best_sol = sol
                    
                # print(f"Relaxed bounds - Guess {guess}: norm = {norm}, solution = {sol.x}")
            except Exception as e:
                print(f"Failed with relaxed bounds and guess {guess}: {str(e)}")
                continue
        
        sol = best_sol
    
    if sol == None:
        print("No solution found with any approach. Returning NaN values.")
        return np.nan, np.nan, np.nan, np.nan
    if sol.success:
        # print("\nSuccess!")
        # print("Solution l4 =", sol.x)
        # print("Residuals =", system_resid(sol.x))
        # print("Norm of residuals =", np.linalg.norm(system_resid(sol.x)))
        
        # # Add more diagnostics
        # print("\nDiagnostics:")
        # print("Number of function evaluations:", sol.nfev)
        # print("Termination reason:", sol.message)
        
        # Check if solution is at bounds
        at_lower = np.isclose(sol.x, bounds_lower)
        at_upper = np.isclose(sol.x, bounds_upper)
        # if any(at_lower) or any(at_upper):
        #     print("\nWarning: Solution is at bounds:")
        #     for i, (val, lower, upper) in enumerate(zip(sol.x, bounds_lower, bounds_upper)):
        #         if np.isclose(val, lower):
        #             print(f"l{i+1} is at lower bound ({lower})")
        #         if np.isclose(val, upper):
        #             print(f"l{i+1} is at upper bound ({upper})")
                    
        # Calculate and print e_t, c_t values and final h5c
        # print("\nCalculated e_t values:")
        # print(f"e3 = {e_t_star(sol.x[0], w4)}")
        # print(f"e4 = {e_t_star(sol.x[1], w4)}")
        # print(f"e4 = {e_t_star(sol.x[2], w4)}")
        # print(f"e4 = {e_t_star(sol.x[3], w4)}")
        
        # print("\nCalculated c_t values:")
        # print(f"c3 = {c_t_star(sol.x[0], w4)}")
        # print(f"c4 = {c_t_star(sol.x[1], w4)}")
        # print(f"c4 = {c_t_star(sol.x[2], w4)}")
        # print(f"c4 = {c_t_star(sol.x[3], w4)}")
        
        # print(f"\nFinal h5c value = {h5c(sol.x[0], sol.x[1], sol.x[2])}")
    else:
        print("No solution found with bounding approach. Message:", sol.message)

    e4star = e_t_star(sol.x[0], w4)
    c4star = c_t_star(sol.x[0], w4)
    return c4star, e4star, sol.x[0], h_5(e4star)

# if __name__ == "__main__":
#     # Run all simulations and save results

#     # Create a function to run the simulation with different parameters
#     def run_simulation(rho_val, rho_e_val):
#         try:
#             h_val = 0.7
#             c1, e1, l1, h2, predh5final = solve_bounded_system_t1(rho_val, rho_e_val, h_val)
#             c2, e2, l2, h3 = solve_bounded_system_t2(rho_val, rho_e_val, h2, h_val)
#             c3, e3, l3, h4 = solve_bounded_system_t3(rho_val, rho_e_val, h3, h_val)
#             c4, e4, l4, h5 = solve_bounded_system_t4(rho_val, rho_e_val, h4, h_val)
            
#             return {
#                 'rho': rho_val,
#                 'rho_e': rho_e_val,
#                 'predh5final': predh5final,
#                 'h5': h5
#             }
#         except Exception as e:
#             print(f"Error with rho={rho_val}, rho_e={rho_e_val}: {str(e)}")
#             return {
#                 'rho': rho_val,
#                 'rho_e': rho_e_val,
#                 'predh5final': np.nan,
#                 'h5': np.nan
#             }
    
#     # Function to run all simulations and save results
#     def run_all_simulations():
#         # Generate parameter values
#         rho_values = [r/10 for r in range(-40, 11) if r != 0]  # -4.0 to 1.0 in 0.1 increments, skip 0
#         rho_e_values = [r/10 for r in range(-40, 11) if r != 0]  # -4.0 to 1.0 in 0.1 increments, skip 0
        
#         results = []
        
#         # Run simulations for all parameter combinations
#         total_combinations = len(rho_values) * len(rho_e_values)
#         completed = 0
        
#         for rho_val, rho_e_val in product(rho_values, rho_e_values):
#             print(f"Running simulation {completed+1}/{total_combinations}: rho={rho_val}, rho_e={rho_e_val}")
#             result = run_simulation(rho_val, rho_e_val)
#             results.append(result)
#             completed += 1
            
#             # Save intermediate results every 10 simulations
#             if completed % 10 == 0:
#                 df = pd.DataFrame(results)
#                 df.to_csv('policy_results_intermediate70.csv', index=False)
#                 print(f"Saved intermediate results ({completed}/{total_combinations})")
        
#         # Create DataFrame and save to CSV
#         df = pd.DataFrame(results)
#         df.to_csv('policy_results70.csv', index=False)
#         print("All simulations completed. Results saved to policy_results.csv")
    
#     run_all_simulations()

    # rho = -0.1
    # rho_e = -2
    # h = 1
    # c1, e1, l1, h2, predh5final = solve_bounded_system_t1(rho, rho_e, h)
    # # print(e1, l1, h2)

    # c2, e2, l2, h3 = solve_bounded_system_t2(rho, rho_e, h2, h)

    # c3, e3, l3, h4 = solve_bounded_system_t3(rho, rho_e, h3, h)

    # c4, e4, l4, h5 = solve_bounded_system_t4(rho, rho_e, h4, h)


    # print("\nPred h5 final after t1")
    # print(predh5final)

    # print("\nPeriod 1 Results:")
    # print(f"c1 = {c1:.3f}, e1 = {e1:.3f}, l1 = {l1:.3f}, h2 = {h2:.3f}")
    
    # print("\nPeriod 2 Results:")
    # print(f"c2 = {c2:.3f}, e2 = {e2:.3f}, l2 = {l2:.3f}, h3 = {h3:.3f}")

    # print("\nPeriod 3 Results:")
    # print(f"c3 = {c3:.3f}, e3 = {e3:.3f}, l3 = {l3:.3f}, h4 = {h4:.3f}")

    # print("\nPeriod 4 Results:")
    # print(f"c4 = {c4:.3f}, e3 = {e4:.3f}, l3 = {l4:.3f}, h5 = {h5:.3f}")




