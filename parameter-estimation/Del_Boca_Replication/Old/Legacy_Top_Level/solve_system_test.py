from __future__ import annotations
import numpy as np


# Empirically known parameters
beta    = 0.96 # discount factor
gamma_  = 2 # Disutility of Leisure
eta     = 2.0 # Risk aversion
phi     = 0.5 # Public-Private Share (Jang & Yum, 2021)
psi     = 0.3 # Public-Private Elasticity of Sub. ψ ≤ 1 (Jang & Yum, 2021)
xi = 2 # I made this up, placeholder removing in future iterations with simpler specification
alpha   = 3 # I made this up, placeholder removing in future iterations with simpler specification

# Additional parameters that were missing
psi_bar_ces = psi  # CES elasticity parameter
b_par = 1.0  # Placeholder parameter for education preference
c_par = 1.0  # Placeholder parameter for consumption preference
delta_e = np.array([1.0, 1.0, 1.0, 0.0])  # Education investment weights by period

EPS = np.ones(4)  # parental productivity epsilon_t
G_prod = np.ones(4)  # public productivity G_t (rename to avoid clash)

E_BAR = 0.10     # avg parental share of income for education
G_BAR = 0.01375  # avg public share of GDP for education

# ---------------------------------------------------------------------------
# 2.  Helpers for the closed‑form solution
# ---------------------------------------------------------------------------

def _psi_t(t: int) -> float:
    """Education‑to‑consumption markup (psi_t) in period *t* (1‑based)."""
    return (beta ** (4 - t)) * b_par * c_par * delta_e[t - 1]


def _closed_form_vars(t: int, w_t: float, h_parent: float) -> tuple[float, float, float]:
    """Return (c_t, e_t, l_t) for period *t* via share rules."""
    psi_t = _psi_t(t)
    denom = gamma_ + 1.0 + psi_t  # D_t
    l_t   = (1.0 + psi_t) / denom
    c_t   = (w_t * h_parent) / denom
    e_t   = psi_t * c_t
    return c_t, e_t, l_t

# ---------------------------------------------------------------------------
# 3.  Human‑capital transition helpers (rho / rho_e nests retained)
# ---------------------------------------------------------------------------

def _I(e_t: float, g_t: float, idx: int) -> float:
    """Inside‑period CES aggregator of private (e_t) and public (g_t) inputs."""
    inner = phi * (EPS[idx] * e_t / E_BAR) ** psi_bar_ces + \
            (1 - phi) * (G_prod[idx] * g_t / G_BAR) ** psi_bar_ces
    return inner


def _h_next(e_t: float, h_prev: float, g_t: float,
            rho_e: float, theta: float, idx: int) -> float:
    """Generic one‑step human‑capital update (period idx = 0,1,2)."""
    inner = _I(e_t, g_t, idx) ** (rho_e / psi_bar_ces)
    return (theta * inner + (1 - theta) * (h_prev ** rho_e)) ** (1 / rho_e)

# ---------------------------------------------------------------------------
# 4.  Solver functions with the *original* signatures
# ---------------------------------------------------------------------------

def solve_bounded_system_t1(parameters, household):
    """Return (c1, e1, l1, h2, h5_pred)."""
    # Unpack parameters exactly as before
    _, rho, rho_e, theta1, theta2, theta3 = parameters
    h0 = household[0, 0]
    w = household[:, 1]  # length‑4
    g = household[:, 2]  # length‑4

    # Period‑1 choices
    c1, e1, l1 = _closed_form_vars(1, w[0], h0)
    h2 = _h_next(e1, h0, g[0], rho_e, theta1, idx=0)

    # Forward prediction (periods 2‑4) just to supply h5_pred
    c2, e2, _ = _closed_form_vars(2, w[1], h0)
    h3 = _h_next(e2, h2, g[1], rho_e, theta2, idx=1)

    c3, e3, _ = _closed_form_vars(3, w[2], h0)
    h4 = _h_next(e3, h3, g[2], rho_e, theta3, idx=2)

    h5_pred = h4  # no new inputs in period‑4
    return c1, e1, l1, h2, h5_pred


def solve_bounded_system_t2(parameters, household, h2):
    """Return (c2, e2, l2, h3)."""
    _, _, rho_e, _, theta2, _ = parameters
    h0 = household[0, 0]
    w = household[:, 1]
    g = household[:, 2]

    c2, e2, l2 = _closed_form_vars(2, w[1], h0)
    h3 = _h_next(e2, h2, g[1], rho_e, theta2, idx=1)
    return c2, e2, l2, h3


def solve_bounded_system_t3(parameters, household, h3):
    """Return (c3, e3, l3, h4)."""
    _, _, rho_e, _, _, theta3 = parameters
    h0 = household[0, 0]
    w = household[:, 1]
    g = household[:, 2]

    c3, e3, l3 = _closed_form_vars(3, w[2], h0)
    h4 = _h_next(e3, h3, g[2], rho_e, theta3, idx=2)
    return c3, e3, l3, h4


def solve_bounded_system_t4(parameters, household, h4):
    """Return (c4, e4, l4, h5).  Education is zero in period‑4."""
    h0 = household[0, 0]
    w4 = household[3, 1]

    denom4 = gamma_ + 1.0  # psi_4 = 0
    l4 = 1.0 / denom4
    c4 = (w4 * h0) / denom4
    e4 = 0.0
    h5 = h4
    return c4, e4, l4, h5

# ---------------------------------------------------------------------------
# 5.  Wrapper solve_household (unchanged signature)
# ---------------------------------------------------------------------------

def solve_household(parameters, household):
    c1, e1, l1, h2, _ = solve_bounded_system_t1(parameters, household)
    c2, e2, l2, h3   = solve_bounded_system_t2(parameters, household, h2)
    c3, e3, l3, h4   = solve_bounded_system_t3(parameters, household, h3)
    c4, e4, l4, h5   = solve_bounded_system_t4(parameters, household, h4)

    return np.array([
        [c1, e1, l1, h2],
        [c2, e2, l2, h3],
        [c3, e3, l3, h4],
        [c4, e4, l4, h5],
    ])

# ---------------------------------------------------------------------------
# 6.  Smoke test
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # Fake household: columns 0 (h0), 1 (wage), 2 (govt input)
    NUM_PERIODS = 4
    hh = np.zeros((NUM_PERIODS, 3))
    hh[:, 0] = 0.8  # parent human capital constant across periods
    hh[:, 1] = [1.0, 1.05, 1.10, 1.15]
    hh[:, 2] = [0.02, 0.02, 0.02, 0.02]

    params = [0.0,  -0.1, -2.0, 0.25, 0.25, 0.25]  # delta ignored

    solved = solve_household(params, hh)
    print("Solved array (c,e,l,h_next):\n", solved)
