
import numpy as np
from scipy.optimize import least_squares
import pandas as pd
from itertools import product

# Empirically known parameters
beta    = 0.96  # discount factor
gamma_  = 2     # Disutility of Leisure
eta     = 2.0   # Risk aversion
phi     = 0.5   # Public-Private Share (Jang & Yum, 2021)
psi     = 0.3   # Public-Private Elasticity of Sub. ψ ≤ 1 (Jang & Yum, 2021)
xi      = 2     # Placeholder
alpha   = 3     # Placeholder
e_bar   = 0.1   # Average share of income allocated to parental education investment
g_bar   = 0.01375  # Average US public education expenditure as a share of GDP
eps1, eps2, eps3 = 1.0, 1.0, 1.0
G1, G2, G3 = 1.0, 1.0, 1.0

def create_simple_household():
    NUM_PERIODS = 3
    household = np.zeros((NUM_PERIODS,7))
    h_val = 1.0
    household[:,0] = h_val
    household[:,1] = 1.0
    household[:,2] = 1.0
    return household

def solve_household(parameters, household):
    c1,e1,l1,h2,predh4final = solve_bounded_system_t1(parameters, household)
    c2,e2,l2,h3,l3          = solve_bounded_system_t2(parameters, household, h2)
    c3,e3,l3,h4             = solve_bounded_system_t3(parameters, household, h3, l3_fixed=l3)
    return np.array([[c1,e1,l1,h2],[c2,e2,l2,h3],[c3,e3,l3,h4]])

def solve_bounded_system_t1(parameters, household):
    delta, rho, rho_e, theta1, theta2 = parameters
    theta3 = 1.0 - theta1 - theta2
    h = household[0,0]
    w1,w2,w3 = household[:,1]
    g1,g2,g3 = household[:,2]
    h1_rho = 1.0

    def c_t_star(lt, wt):
        return ((1.0 - lt) * wt * h) / gamma_
    def e_t_star(lt, wt):
        lt_safe = max(lt, 1.0/(gamma_ + 1.0))
        return (wt*h/gamma_) * ((gamma_+1.0)*lt_safe - 1.0)

    def h4c(l1,l2,l3):
        e1 = max(e_t_star(l1,w1),1e-10)
        e2 = max(e_t_star(l2,w2),1e-10)
        e3 = max(e_t_star(l3,w3),1e-10)
        t1 = phi*(eps1*e1/e_bar)**psi + (1.0-phi)*(G1*g1/g_bar)**psi
        t2 = phi*(eps2*e2/e_bar)**psi + (1.0-phi)*(G2*g2/g_bar)**psi
        t3 = phi*(eps3*e3/e_bar)**psi + (1.0-phi)*(G3*g3/g_bar)**psi
        T1 = (max(t1,1e-10))**(rho/psi) if t1>0 else 1e-10
        T2 = (max(t2,1e-10))**(rho/psi) if t2>0 else 1e-10
        T3 = (max(t3,1e-10))**(rho/psi) if t3>0 else 1e-10
        a1 = (1-theta3)*(1-theta2)*theta1*T1 + (1-theta3)*theta2*T2 + theta3*T3 + (1-theta3)*(1-theta2)*(1-theta1)*h1_rho
        return max(a1,1e-10)**(1.0/rho)

    def dh4c_de1(l1,l2,l3):
        e1 = max(e_t_star(l1,w1),1e-10)
        inside = max(phi*(eps1*e1/e_bar)**psi + (1.0-phi)*(G1*g1/g_bar)**psi, 1e-10)
        front = (1-theta3)*(1-theta2)*theta1*phi*((eps1/e_bar)**psi)*(e1**(psi-1.0))
        h4v = h4c(l1,l2,l3)
        return front * inside**((rho/psi)-1.0) * (h4v**(1.0 - rho))

    def dh4c_de2(l1,l2,l3):
        e2 = max(e_t_star(l2,w2),1e-10)
        inside = max(phi*(eps2*e2/e_bar)**psi + (1.0-phi)*(G2*g2/g_bar)**psi, 1e-10)
        front = (1-theta3)*theta2*phi*((eps2/e_bar)**psi)*(e2**(psi-1.0))
        h4v = h4c(l1,l2,l3)
        return front * inside**((rho/psi)-1.0) * (h4v**(1.0 - rho))

    def dh4c_de3(l1,l2,l3):
        e3 = max(e_t_star(l3,w3),1e-10)
        inside = max(phi*(eps3*e3/e_bar)**psi + (1.0-phi)*(G3*g3/g_bar)**psi, 1e-10)
        front = theta3*phi*((eps3/e_bar)**psi)*(e3**(psi-1.0))
        h4v = h4c(l1,l2,l3)
        return front * inside**((rho/psi)-1.0) * (h4v**(1.0 - rho))

    def h_2(e1star):
        I1 = (phi*(eps1*e1star/e_bar)**psi + (1.0-phi)*(G1*g1/g_bar)**psi)**(rho_e/psi)
        return (theta1*(I1) + (1.0-theta1)*(h1_rho))**(1.0/rho_e)

    def system(ells):
        l1,l2,l3 = ells
        A = (beta**3) * (alpha*np.log(h+xi) + delta)
        f1 = A*dh4c_de1(l1,l2,l3) - (beta**3)*(c_t_star(l1,w1)*((1.0-l1)**gamma_))**(-eta) * ((1.0-l1)**gamma_)
        f2 = A*dh4c_de2(l1,l2,l3) - (beta**2)*(c_t_star(l2,w2)*((1.0-l2)**gamma_))**(-eta) * ((1.0-l2)**gamma_)
        f3 = A*dh4c_de3(l1,l2,l3) - (beta**1)*(c_t_star(l3,w3)*((1.0-l3)**gamma_))**(-eta) * ((1.0-l3)**gamma_)
        return [f1,f2,f3]

    def system_resid(ells):
        r = system(ells)
        return np.array([np.nan_to_num(val, nan=1e6, posinf=1e6, neginf=-1e6) for val in r])

    min_l = 0.9/(gamma_ + 1.0)
    max_l = 0.95
    bounds_lower = [min_l, min_l, min_l]
    bounds_upper = [max_l, max_l, max_l]
    initial_guesses = [[0.6,0.6,0.6],[0.5,0.6,0.7],[0.7,0.6,0.5],[0.8,0.6,0.6],[0.6,0.8,0.6]]

    best_sol = None
    best_norm = np.inf
    for guess in initial_guesses:
        try:
            sol = least_squares(system_resid, guess, bounds=(bounds_lower,bounds_upper),
                                xtol=1e-12, ftol=1e-12, method='trf', max_nfev=20000)
            nrm = np.linalg.norm(system_resid(sol.x))
            if nrm < best_norm:
                best_norm = nrm
                best_sol = sol
        except Exception:
            continue

    if best_sol is not None and np.allclose(best_sol.x, bounds_lower):
        relaxed_bounds_lower = [min_l - 0.05]*3
        for guess in initial_guesses:
            try:
                sol = least_squares(system_resid, guess, bounds=(relaxed_bounds_lower,bounds_upper),
                                    xtol=1e-12, ftol=1e-12, method='trf', max_nfev=20000)
                nrm = np.linalg.norm(system_resid(sol.x))
                if nrm < best_norm:
                    best_norm = nrm
                    best_sol = sol
            except Exception:
                continue

    if best_sol is None or not best_sol.success:
        print("No solution found in T1. Returning NaNs.")
        return np.nan, np.nan, np.nan, np.nan, np.nan

    l1_opt, l2_opt, l3_opt = best_sol.x
    e1star = e_t_star(l1_opt, w1)
    c1star = c_t_star(l1_opt, w1)
    h2 = h_2(e1star)
    predh4final = h4c(l1_opt, l2_opt, l3_opt)
    return c1star, e1star, l1_opt, h2, predh4final

def solve_bounded_system_t2(parameters, household, h2):
    delta, rho, rho_e, theta1, theta2 = parameters
    theta3 = 1.0 - theta1 - theta2
    h = household[0,0]
    w1,w2,w3 = household[:,1]
    g1,g2,g3 = household[:,2]

    def c_t_star(lt, wt):
        return ((1.0 - lt) * wt * h) / gamma_
    def e_t_star(lt, wt):
        lt_safe = max(lt, 1.0/(gamma_ + 1.0))
        return (wt*h/gamma_) * ((gamma_+1.0)*lt_safe - 1.0)

    def a2(l2,l3):
        e2 = max(e_t_star(l2,w2),1e-10)
        e3 = max(e_t_star(l3,w3),1e-10)
        t2 = phi*(eps2*e2/e_bar)**psi + (1.0-phi)*(G2*g2/g_bar)**psi
        t3 = phi*(eps3*e3/e_bar)**psi + (1.0-phi)*(G3*g3/g_bar)**psi
        T2 = (max(t2,1e-10))**(rho/psi) if t2>0 else 1e-10
        T3 = (max(t3,1e-10))**(rho/psi) if t3>0 else 1e-10
        return theta3*T3 + (1-theta3)*theta2*T2 + (1-theta3)*(1-theta2)*(h2**rho)

    def dh4c_de2(l2,l3):
        e2 = max(e_t_star(l2,w2),1e-10)
        inside = max(phi*(eps2*e2/e_bar)**psi + (1.0-phi)*(G2*g2/g_bar)**psi, 1e-10)
        front = (1-theta3)*theta2*phi*((eps2/e_bar)**psi)*(e2**(psi-1.0))
        a2v = a2(l2,l3)
        return front * inside**((rho/psi)-1.0) * (a2v**(1.0/rho - 1.0))

    def dh4c_de3(l2,l3):
        e3 = max(e_t_star(l3,w3),1e-10)
        inside = max(phi*(eps3*e3/e_bar)**psi + (1.0-phi)*(G3*g3/g_bar)**psi, 1e-10)
        front = theta3*phi*((eps3/e_bar)**psi)*(e3**(psi-1.0))
        a2v = a2(l2,l3)
        return front * inside**((rho/psi)-1.0) * (a2v**(1.0/rho - 1.0))

    def system(ells):
        l2,l3 = ells
        A = (beta**3) * (alpha*np.log(h+xi) + delta)
        f2 = A*dh4c_de2(l2,l3) - (beta**1)*(c_t_star(l2,w2)*((1.0-l2)**gamma_))**(-eta) * ((1.0-l2)**gamma_)
        f3 = A*dh4c_de3(l2,l3) - (beta**2)*(c_t_star(l3,w3)*((1.0-l3)**gamma_))**(-eta) * ((1.0-l3)**gamma_)
        return [f2,f3]

    def system_resid(ells):
        r = system(ells)
        return np.array([np.nan_to_num(val, nan=1e6, posinf=1e6, neginf=-1e6) for val in r])

    min_l = 0.9/(gamma_ + 1.0); max_l = 0.95
    bounds_lower = [min_l, min_l]; bounds_upper = [max_l, max_l]
    guesses = [[0.5,0.5],[0.6,0.6],[0.7,0.7],[0.8,0.6],[0.6,0.8],[0.9,0.9]]
    best_sol = None; best_norm = np.inf
    for g in guesses:
        try:
            sol = least_squares(system_resid, g, bounds=(bounds_lower,bounds_upper),
                                xtol=1e-12, ftol=1e-12, method='trf', max_nfev=20000)
            nrm = np.linalg.norm(system_resid(sol.x))
            if nrm < best_norm:
                best_sol, best_norm = sol, nrm
        except Exception:
            pass
    if best_sol is None or not best_sol.success:
        return np.nan, np.nan, np.nan, np.nan, np.nan

    l2_opt, l3_opt = best_sol.x
    e2star = e_t_star(l2_opt, w2)
    c2star = c_t_star(l2_opt, w2)
    # h3 evolution (match 4-period pattern: It^(rho_e/psi) then power mix)
    I2 = (phi*(eps2*e2star/e_bar)**psi + (1.0-phi)*(G2*g2/g_bar)**psi)**(rho_e/psi)
    h3  = (theta2*(I2) + (1.0-theta2)*(h2))**(1.0/rho_e)
    return c2star, e2star, l2_opt, h3, l3_opt

def solve_bounded_system_t3(parameters, household, h3, l3_fixed=None):
    delta, rho, rho_e, theta1, theta2 = parameters
    theta3 = 1.0 - theta1 - theta2
    h = household[0,0]
    w1,w2,w3 = household[:,1]
    g1,g2,g3 = household[:,2]

    def c_t_star(lt, wt):
        return ((1.0 - lt) * wt * h) / gamma_
    def e_t_star(lt, wt):
        lt_safe = max(lt, 1.0/(gamma_ + 1.0))
        return (wt*h/gamma_) * ((gamma_+1.0)*lt_safe - 1.0)
    def h_4(e3star):
        It = (phi*(eps3*e3star/e_bar)**psi + (1.0-phi)*(G3*g3/g_bar)**psi)**(rho_e/psi)
        return (theta3*(It) + (1.0-theta3)*(h3))**(1.0/rho_e)

    if l3_fixed is not None:
        l3 = l3_fixed
        e3star = e_t_star(l3, w3)
        c3star = c_t_star(l3, w3)
        h4 = h_4(e3star)
        return c3star, e3star, l3, h4

    def a2(l3):
        e3 = max(e_t_star(l3,w3),1e-10)
        t3 = phi*(eps3*e3/e_bar)**psi + (1.0-phi)*(G3*g3/g_bar)**psi
        T3 = (max(t3,1e-10))**(rho/psi) if t3>0 else 1e-10
        return theta3*T3 + (1-theta3)*(h3**rho)

    def dh4c_de3(l3):
        e3 = max(e_t_star(l3,w3),1e-10)
        inside = max(phi*(eps3*e3/e_bar)**psi + (1.0-phi)*(G3*g3/g_bar)**psi, 1e-10)
        front = theta3*phi*((eps3/e_bar)**psi)*(e3**(psi-1.0))
        a2v = a2(l3)
        return front * inside**((rho/psi)-1.0) * (a2v**(1.0/rho - 1.0))

    def system(ells):
        l3 = ells[0]
        A = (beta**3) * (alpha*np.log(h+xi) + delta)
        f3 = A*dh4c_de3(l3) - (beta**2)*(c_t_star(l3,w3)*((1.0-l3)**gamma_))**(-eta) * ((1.0-l3)**gamma_)
        return [f3]

    def system_resid(ells):
        r = system(ells)
        return np.array([np.nan_to_num(val, nan=1e6, posinf=1e6, neginf=-1e6) for val in r])

    min_l = 0.9/(gamma_ + 1.0); max_l = 0.95
    bounds_lower = [min_l]; bounds_upper = [max_l]
    guesses = [[0.6],[0.7],[0.8],[0.9]]
    best_sol=None; best_norm=np.inf
    for g in guesses:
        try:
            sol = least_squares(system_resid, g, bounds=(bounds_lower,bounds_upper),
                                xtol=1e-12, ftol=1e-12, method='trf', max_nfev=20000)
            nrm = np.linalg.norm(system_resid(sol.x))
            if nrm < best_norm:
                best_sol, best_norm = sol, nrm
        except Exception:
            pass
    if best_sol is None or not best_sol.success:
        return np.nan, np.nan, np.nan, np.nan

    l3 = best_sol.x[0]
    e3star = e_t_star(l3, w3)
    c3star = c_t_star(l3, w3)
    return c3star, e3star, l3, h_4(e3star)

NUM_PERIODS = 3

def run_test_simulation(rho_val, rho_e_val):
    parameters = [20.0, rho_val, rho_e_val, 0.3, 0.2]
    household = create_simple_household()
    c1,e1,l1,h2,predh4final = solve_bounded_system_t1(parameters, household)
    c2,e2,l2,h3,l3 = solve_bounded_system_t2(parameters, household, h2)
    c3,e3,l3,h4    = solve_bounded_system_t3(parameters, household, h3, l3_fixed=l3)
    return {'rho':rho_val,'rho_e':rho_e_val,'predh4final':predh4final,'h4':h4}

def run_all_test_simulations():
    rho_values = np.linspace(-2,0.5,5)
    rho_e_values = np.linspace(-2,0.5,5)
    results = []
    total = len(rho_values)*len(rho_e_values)
    completed=0
    for rho_val in rho_values:
        for rho_e_val in rho_e_values:
            print(f"Running simulation {completed+1}/{total}: rho={rho_val}, rho_e={rho_e_val}")
            results.append(run_test_simulation(rho_val, rho_e_val))
            completed+=1
            if completed % 5 == 0:
                pd.DataFrame(results).to_csv('test_results_3periods_intermediate.csv', index=False)
                print(f"Saved intermediate results ({completed}/{total})")
    pd.DataFrame(results).to_csv('test_results_3periods.csv', index=False)
    print("All test simulations completed. Results saved to test_results_3periods.csv")

if __name__ == "__main__":
    run_all_test_simulations()
