using LinearAlgebra
using Random

# Empirically known parameters
const beta = 0.96    # discount factor
const gamma_ = 2.0   # Disutility of Leisure
const eta = 2.0      # Risk aversion
const phi = 0.5      # Public-Private Share (Jang & Yum, 2021)
const psi = 0.3      # Public-Private Elasticity of Sub. ψ ≤ 1 (Jang & Yum, 2021)
const xi = 2.0       # Placeholder
const alpha = 3.0    # Placeholder

# Additional parameters
const psi_bar_ces = psi  # CES elasticity parameter
const b_par = 1.0        # Placeholder parameter for education preference
const c_par = 1.0        # Placeholder parameter for consumption preference
const delta_e = [1.0, 1.0, 1.0]  # Education investment weights by period (now 3 periods)

const EPS = ones(3)      # parental productivity epsilon_t (now 3 periods)
const G_prod = ones(3)   # public productivity G_t (now 3 periods)

const E_BAR = 0.10       # avg parental share of income for education
const G_BAR = 0.01375    # avg public share of GDP for education

# ---------------------------------------------------------------------------
# 2. Helpers for the closed-form solution
# ---------------------------------------------------------------------------

function _psi_t(t::Int)::Float64
    """Education-to-consumption markup (psi_t) in period t (1-based)."""
    return (beta ^ (3 - t)) * b_par * c_par * delta_e[t]
end

function _closed_form_vars(t::Int, w_t::Float64, h_parent::Float64)::Tuple{Float64, Float64, Float64}
    """Return (c_t, e_t, l_t) for period t via share rules."""
    psi_t = _psi_t(t)
    denom = gamma_ + 1.0 + psi_t  # D_t
    l_t = (1.0 + psi_t) / denom
    c_t = (w_t * h_parent) / denom
    e_t = psi_t * c_t
    return c_t, e_t, l_t
end

# ---------------------------------------------------------------------------
# 3. Human-capital transition helpers (rho / rho_e nests retained)
# ---------------------------------------------------------------------------

function _I(e_t::Float64, g_t::Float64, idx::Int)::Float64
    """Inside-period CES aggregator of private (e_t) and public (g_t) inputs."""
    inner = phi * (EPS[idx] * e_t / E_BAR) ^ psi_bar_ces +
            (1 - phi) * (G_prod[idx] * g_t / G_BAR) ^ psi_bar_ces
    return inner
end

function _h_next(e_t::Float64, h_prev::Float64, g_t::Float64,
                rho_e::Float64, theta::Float64, idx::Int)::Float64
    """Generic one-step human-capital update (period idx = 0,1)."""
    inner = _I(e_t, g_t, idx) ^ (rho_e / psi_bar_ces)
    return (theta * inner + (1 - theta) * (h_prev ^ rho_e)) ^ (1 / rho_e)
end

# ---------------------------------------------------------------------------
# 4. Solver functions for 3-period model
# ---------------------------------------------------------------------------

function solve_bounded_system_t1(parameters::Vector{Float64}, household::Matrix{Float64})::Tuple{Float64, Float64, Float64, Float64, Float64}
    """Return (c1, e1, l1, h2, h4_pred)."""
    # Unpack parameters exactly as before
    _, rho, rho_e, theta1, theta2, theta3 = parameters
    h0 = household[1, 1]
    w = household[:, 2]  # length-3
    g = household[:, 3]  # length-3

    # Period-1 choices
    c1, e1, l1 = _closed_form_vars(1, w[1], h0)
    h2 = _h_next(e1, h0, g[1], rho_e, theta1, 1)

    # Forward prediction (periods 2-3) just to supply h4_pred
    c2, e2, _ = _closed_form_vars(2, w[2], h0)
    h3 = _h_next(e2, h2, g[2], rho_e, theta2, 2)

    c3, e3, _ = _closed_form_vars(3, w[3], h0)
    h4_pred = _h_next(e3, h3, g[3], rho_e, theta3, 3)

    return c1, e1, l1, h2, h4_pred
end

function solve_bounded_system_t2(parameters::Vector{Float64}, household::Matrix{Float64}, h2::Float64)::Tuple{Float64, Float64, Float64, Float64}
    """Return (c2, e2, l2, h3)."""
    _, _, rho_e, _, theta2, _ = parameters
    h0 = household[1, 1]
    w = household[:, 2]
    g = household[:, 3]

    c2, e2, l2 = _closed_form_vars(2, w[2], h0)
    h3 = _h_next(e2, h2, g[2], rho_e, theta2, 2)
    return c2, e2, l2, h3
end

function solve_bounded_system_t3(parameters::Vector{Float64}, household::Matrix{Float64}, h3::Float64)::Tuple{Float64, Float64, Float64, Float64}
    """Return (c3, e3, l3, h4)."""
    _, _, rho_e, _, _, theta3 = parameters
    h0 = household[1, 1]
    w = household[:, 2]
    g = household[:, 3]

    c3, e3, l3 = _closed_form_vars(3, w[3], h0)
    h4 = _h_next(e3, h3, g[3], rho_e, theta3, 3)
    return c3, e3, l3, h4
end

# ---------------------------------------------------------------------------
# 5. Wrapper solve_household (unchanged signature, but now 3 periods)
# ---------------------------------------------------------------------------

function solve_household(parameters::Vector{Float64}, household::Matrix{Float64})::Matrix{Float64}
    c1, e1, l1, h2, _ = solve_bounded_system_t1(parameters, household)
    c2, e2, l2, h3 = solve_bounded_system_t2(parameters, household, h2)
    c3, e3, l3, h4 = solve_bounded_system_t3(parameters, household, h3)

    return [
        c1 e1 l1 h2
        c2 e2 l2 h3
        c3 e3 l3 h4
    ]
end

# ---------------------------------------------------------------------------
# 6. Smoke test
# ---------------------------------------------------------------------------
#=
# Fake household: columns 1 (h0), 2 (wage), 3 (govt input)
const NUM_PERIODS = 3
hh = zeros(NUM_PERIODS, 3)
hh[:, 1] .= 0.8  # parent human capital constant across periods
hh[:, 2] = [1.0, 1.05, 1.10]
hh[:, 3] = [0.02, 0.02, 0.02]

params = [0.0, -0.1, -2.0, 0.25, 0.25, 0.25]  # delta ignored

solved = solve_household(params, hh)
println("Solved array (c,e,l,h_next):\n", solved)
=#
