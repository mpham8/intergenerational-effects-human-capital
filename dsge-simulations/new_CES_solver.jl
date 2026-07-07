# Standalone parent policy solver for the Pham-OC project.
#
# This version distinguishes the technology parents use to think about the
# production of child human capital from the true technology that determines the
# state observed at the start of the next period.
#
# Key modeling choices implemented here:
# - Parents enter each period observing the true child human capital state.
# - Parent choices are solved on a 2D state grid: child human capital and parent
#   human capital.
# - The continuation state is rolled forward using the true production function
#   and a multiplicative lognormal shock integrated by Gauss-Hermite quadrature.
# - The perceived production function is also evaluated and returned so the
#   wedge between perceived and true child human capital can be inspected.
#
# No external Julia packages are required.


function as_period_vector(x, periods::Int)
    if x isa Number
        return fill(Float64(x), periods)
    end
    v = Float64.(collect(x))
    length(v) == periods || error("Expected scalar or vector of length $periods.")
    return v
end


function crra_utility(c::Float64, labor::Float64, eta::Float64, gamma::Float64)
    if c <= 0.0 || labor < 0.0 || labor >= 1.0
        return -Inf
    end

    composite = c * (1.0 - labor)^gamma
    if composite <= 0.0
        return -Inf
    end

    if abs(eta - 1.0) < 1e-10
        return log(composite)
    end
    return (composite^(1.0 - eta) - 1.0) / (1.0 - eta)
end


"""
    hc_production(hc, e, time_investment, g, A, theta_h, theta_e, theta_g, rho; min_input)

Nested CES human-capital production from equation 34. The same function is used
for both perceived and true technologies; the caller passes the relevant
parameter vector.

Equation 34 uses monetary investment e, parental time investment tau, and public
input z. In this solver copy, parental time investment is the parent's non-labor
time, 1 - labor. The experiment fixes delta = mu = theta_p = 1/2.
"""
function hc_production(
    hc::Float64,
    e::Float64,
    time_investment::Float64,
    g::Float64,
    A::Float64,
    theta_h::Float64,
    theta_e::Float64,
    theta_g::Float64,
    rho::Float64;
    min_input::Float64 = 1e-12,
)
    hc_pos = max(hc, min_input)
    e_pos = max(e, min_input)
    time_pos = max(time_investment, min_input)
    g_pos = max(g, min_input)
    delta = 0.5
    mu = 0.5
    theta_p = 0.5

    parental_index = e_pos^delta * time_pos^(1.0 - delta)
    if abs(mu) < 1e-8
        contemporaneous_index = parental_index^theta_p * g_pos^(1.0 - theta_p)
    else
        contemporaneous_index = (theta_p * parental_index^mu + (1.0 - theta_p) * g_pos^mu)^(1.0 / mu)
    end

    if abs(rho) < 1e-8
        return A * hc_pos^theta_h * contemporaneous_index^(1.0 - theta_h)
    end

    ces_sum = theta_h * hc_pos^rho + (1.0 - theta_h) * contemporaneous_index^rho
    if ces_sum <= 0.0
        return 0.0
    end
    return A * ces_sum^(1.0 / rho)
end


function linear_interp(x_grid::Vector{Float64}, y_grid::Vector{Float64}, x::Float64)
    n = length(x_grid)

    if x <= x_grid[1]
        x0, x1 = x_grid[1], x_grid[2]
        y0, y1 = y_grid[1], y_grid[2]
        return y0 + (y1 - y0) * (x - x0) / (x1 - x0)
    end

    if x >= x_grid[n]
        x0, x1 = x_grid[n - 1], x_grid[n]
        y0, y1 = y_grid[n - 1], y_grid[n]
        return y0 + (y1 - y0) * (x - x0) / (x1 - x0)
    end

    hi = searchsortedfirst(x_grid, x)
    lo = hi - 1
    x0, x1 = x_grid[lo], x_grid[hi]
    y0, y1 = y_grid[lo], y_grid[hi]
    return y0 + (y1 - y0) * (x - x0) / (x1 - x0)
end


"""
    normal_quadrature(n, sigma)

Return nodes and probabilities for z ~ N(0, 1), plus multiplicative shocks
epsilon = exp(sigma*z). The nodes are Gauss-Hermite nodes transformed from the
exp(-x^2) convention to the standard-normal convention.

Supported n values are 5 and 7, matching the 5-7 shock-node guidance.
"""
function normal_quadrature(n::Int, sigma::Float64)
    if n == 5
        hermite_nodes = [-2.0201828704560856, -0.9585724646138185, 0.0,
                          0.9585724646138185, 2.0201828704560856]
        hermite_weights = [0.01995324205904591, 0.3936193231522412,
                           0.9453087204829419, 0.3936193231522412,
                           0.01995324205904591]
    elseif n == 7
        hermite_nodes = [-2.6519613568352334, -1.6735516287674714,
                         -0.8162878828589647, 0.0, 0.8162878828589647,
                          1.6735516287674714, 2.6519613568352334]
        hermite_weights = [0.000971781245099519, 0.054515582819127,
                           0.4256072526101278, 0.8102646175568073,
                           0.4256072526101278, 0.054515582819127,
                           0.000971781245099519]
    else
        error("quadrature_n must be 5 or 7.")
    end

    z_nodes = sqrt(2.0) .* hermite_nodes
    probs = hermite_weights ./ sqrt(pi)
    eps_nodes = exp.(sigma .* z_nodes)
    return eps_nodes, probs
end


function golden_maximize(f, left::Float64, right::Float64; tolerance::Float64 = 1e-8, max_iter::Int = 200)
    if right <= left
        return left, f(left)
    end

    invphi = (sqrt(5.0) - 1.0) / 2.0
    invphi2 = (3.0 - sqrt(5.0)) / 2.0

    a = left
    b = right
    h = b - a
    c = a + invphi2 * h
    d = a + invphi * h
    fc = f(c)
    fd = f(d)

    iter = 0
    while h > tolerance && iter < max_iter
        if fc < fd
            a = c
            c = d
            fc = fd
            h = b - a
            d = a + invphi * h
            fd = f(d)
        else
            b = d
            d = c
            fd = fc
            h = b - a
            c = a + invphi2 * h
            fc = f(c)
        end
        iter += 1
    end

    x = (a + b) / 2.0
    return x, f(x)
end


function coordinate_maximize(
    objective,
    l_start::Float64,
    e_start::Float64,
    max_l::Float64,
    max_e::Float64,
    resources_fn;
    min_c::Float64 = 1e-8,
    min_e::Float64 = 0.0,
    tolerance::Float64 = 1e-8,
    max_iter::Int = 80,
)
    l = clamp(l_start, 0.0, max_l)
    e = clamp(e_start, min_e, min(max_e, resources_fn(l) - min_c))
    if e < min_e
        e = min_e
    end

    old_value = objective(l, e)
    for _ in 1:max_iter
        e_hi = min(max_e, resources_fn(l) - min_c)
        if e_hi < min_e
            e_hi = min_e
        end
        e, _ = golden_maximize(e_choice -> objective(l, e_choice), min_e, e_hi; tolerance=tolerance)

        l, _ = golden_maximize(
            l_choice -> resources_fn(l_choice) - min_c >= e ? objective(l_choice, e) : -Inf,
            0.0,
            max_l;
            tolerance=tolerance,
        )

        value = objective(l, e)
        if abs(value - old_value) < tolerance
            return l, e, value
        end
        old_value = value
    end

    return l, e, objective(l, e)
end


"""
    compute_parent_policy(...)

Solve parent policy on a 2D state grid over current child human capital and
parent human capital.

The policy objective rolls the next-period state forward using the true
production function and multiplicative lognormal shocks. This matches the story
that parents observe the child's realized human capital before making the next
period's decision. The perceived production function is still evaluated and
returned so the implied perceived/true wedge can be inspected.

Returned arrays have shape:

    periods × length(hc_grid) × length(parent_h_grid)
"""
function compute_parent_policy(
    hc_grid,
    parent_h_grid,
    wages,
    transfers,
    public_inputs,
    perceived_theta_h,
    perceived_theta_e,
    perceived_theta_g,
    perceived_rho,
    perceived_d,
    true_theta_h,
    true_theta_e,
    true_theta_g,
    true_rho,
    true_d;
    beta::Float64,
    eta::Float64,
    gamma::Float64,
    b,
    tau,
    sigma_h::Float64,
    controls = nothing,
    x_coeffs = nothing,
    min_c::Float64 = 1e-8,
    min_e::Float64 = 0.0,
    periods::Int = 3,
    quadrature_n::Int = 7,
    tolerance::Float64 = 1e-8,
)
    hc_grid = Float64.(collect(hc_grid))
    parent_h_grid = Float64.(collect(parent_h_grid))
    wages = as_period_vector(wages, periods)
    transfers = as_period_vector(transfers, periods)
    public_inputs = as_period_vector(public_inputs, periods)
    perceived_theta_h = as_period_vector(perceived_theta_h, periods)
    perceived_theta_e = as_period_vector(perceived_theta_e, periods)
    perceived_theta_g = as_period_vector(perceived_theta_g, periods)
    perceived_rho = as_period_vector(perceived_rho, periods)
    perceived_d = as_period_vector(perceived_d, periods)
    true_theta_h = as_period_vector(true_theta_h, periods)
    true_theta_e = as_period_vector(true_theta_e, periods)
    true_theta_g = as_period_vector(true_theta_g, periods)
    true_rho = as_period_vector(true_rho, periods)
    true_d = as_period_vector(true_d, periods)
    b = as_period_vector(b, periods)
    tau = as_period_vector(tau, periods)

    if controls === nothing || x_coeffs === nothing
        x_beta = zeros(periods)
    else
        coeffs = Float64.(collect(x_coeffs))
        controls_matrix = controls isa AbstractVector ? reshape(Float64.(collect(controls)), 1, :) : Float64.(controls)
        if size(controls_matrix, 1) == 1
            controls_matrix = repeat(controls_matrix, periods, 1)
        end
        size(controls_matrix, 1) == periods || error("controls must have one row per period.")
        x_beta = controls_matrix * coeffs
    end

    eps_nodes, eps_probs = normal_quadrature(quadrature_n, sigma_h)

    n_hc = length(hc_grid)
    n_parent = length(parent_h_grid)
    c_policy = fill(NaN, periods, n_hc, n_parent)
    l_policy = fill(NaN, periods, n_hc, n_parent)
    e_policy = fill(NaN, periods, n_hc, n_parent)
    perceived_hc_next_policy = fill(NaN, periods, n_hc, n_parent)
    true_expected_hc_next_policy = fill(NaN, periods, n_hc, n_parent)
    value_policy = fill(-Inf, periods, n_hc, n_parent)

    for t in periods:-1:1
        perceived_A = exp(perceived_d[t] + x_beta[t])
        true_A = exp(true_d[t] + x_beta[t])

        for ip in 1:n_parent
            parent_h = parent_h_grid[ip]

            for ih in 1:n_hc
                hc0 = hc_grid[ih]
                max_l = 1.0 - min_c

                resources(labor) = (1.0 - tau[t]) * wages[t] * parent_h * labor + transfers[t]
                max_resources = resources(max_l)
                max_e = max(min_e, max_resources - min_c)

                function expected_continuation(hc_true_det)
                    expected_value = 0.0
                    for q in eachindex(eps_nodes)
                        hc_realized = hc_true_det * eps_nodes[q]
                        if t == periods
                            expected_value += eps_probs[q] * log(max(hc_realized, 1e-12))
                        else
                            continuation = linear_interp(hc_grid, vec(value_policy[t + 1, :, ip]), hc_realized)
                            expected_value += eps_probs[q] * continuation
                        end
                    end
                    return expected_value
                end

                function objective(labor, investment)
                    c = resources(labor) - investment
                    if c <= min_c || investment < min_e || labor < 0.0 || labor >= 1.0
                        return -Inf
                    end

                    hc_true_det = hc_production(
                        hc0,
                        investment,
                        1.0 - labor,
                        public_inputs[t],
                        true_A,
                        true_theta_h[t],
                        true_theta_e[t],
                        true_theta_g[t],
                        true_rho[t],
                    )
                    if hc_true_det <= 0.0
                        return -Inf
                    end

                    current_value = crra_utility(c, labor, eta, gamma)
                    if !isfinite(current_value)
                        return -Inf
                    end

                    # Child human capital enters preferences only through the
                    # terminal payoff. Passing b = [0, 0, large_b] keeps the
                    # early-period value of investment purely dynamic.
                    if t == periods
                        return current_value + beta * b[t] * expected_continuation(hc_true_det)
                    end
                    return current_value + beta * expected_continuation(hc_true_det)
                end

                starts = (
                    (0.20, 0.05),
                    (0.45, 0.20),
                    (0.70, 0.45),
                    (0.90, 0.75),
                )

                best_l = 0.0
                best_e = min_e
                best_value = -Inf

                for (l0, e_share) in starts
                    e0 = min_e + e_share * max(max_e - min_e, 0.0)
                    l_candidate, e_candidate, value_candidate = coordinate_maximize(
                        objective,
                        l0,
                        e0,
                        max_l,
                        max_e,
                        resources;
                        min_c=min_c,
                        min_e=min_e,
                        tolerance=tolerance,
                    )

                    if value_candidate > best_value
                        best_l = l_candidate
                        best_e = e_candidate
                        best_value = value_candidate
                    end
                end

                best_c = resources(best_l) - best_e
                perceived_hc_det = hc_production(
                    hc0,
                    best_e,
                    1.0 - best_l,
                    public_inputs[t],
                    perceived_A,
                    perceived_theta_h[t],
                    perceived_theta_e[t],
                    perceived_theta_g[t],
                    perceived_rho[t],
                )
                true_hc_det = hc_production(
                    hc0,
                    best_e,
                    1.0 - best_l,
                    public_inputs[t],
                    true_A,
                    true_theta_h[t],
                    true_theta_e[t],
                    true_theta_g[t],
                    true_rho[t],
                )
                expected_true_hc = sum(eps_probs .* (true_hc_det .* eps_nodes))

                c_policy[t, ih, ip] = best_c
                l_policy[t, ih, ip] = best_l
                e_policy[t, ih, ip] = best_e
                perceived_hc_next_policy[t, ih, ip] = perceived_hc_det
                true_expected_hc_next_policy[t, ih, ip] = expected_true_hc
                value_policy[t, ih, ip] = best_value
            end
        end
    end

    return (
        c_policy=c_policy,
        l_policy=l_policy,
        e_policy=e_policy,
        perceived_hc_next_policy=perceived_hc_next_policy,
        true_expected_hc_next_policy=true_expected_hc_next_policy,
        value_policy=value_policy,
        hc_grid=hc_grid,
        parent_h_grid=parent_h_grid,
        shock_nodes=eps_nodes,
        shock_probs=eps_probs,
    )
end


function run_demo_tests()
    hc_grid = collect(range(0.5, 2.0, length=8))
    parent_h_grid = [0.8, 1.2, 1.8]

    result = compute_parent_policy(
        hc_grid,
        parent_h_grid,
        [2.0, 2.1, 2.3],
        [0.35, 0.35, 0.35],
        [0.8, 0.9, 1.0],
        [0.35, 0.35, 0.35],
        [0.35, 0.35, 0.35],
        [0.30, 0.30, 0.30],
        [0.20, 0.20, 0.20],
        [0.0, 0.0, 0.0],
        [0.35, 0.35, 0.35],
        [0.35, 0.35, 0.35],
        [0.30, 0.30, 0.30],
        [0.20, 0.20, 0.20],
        [0.0, 0.0, 0.0];
        beta=0.93,
        eta=2.0,
        gamma=1.5,
        b=0.6,
        tau=[0.10, 0.10, 0.10],
        sigma_h=0.10,
        min_c=1e-8,
        min_e=1e-8,
        quadrature_n=7,
    )

    @assert size(result.c_policy) == (3, length(hc_grid), length(parent_h_grid))
    @assert all(isfinite, result.c_policy)
    @assert all(isfinite, result.l_policy)
    @assert all(isfinite, result.e_policy)
    @assert all(isfinite, result.perceived_hc_next_policy)
    @assert all(isfinite, result.true_expected_hc_next_policy)
    @assert all(isfinite, result.value_policy)
    @assert all(result.l_policy .>= 0.0)
    @assert all(result.l_policy .<= 1.0)
    @assert all(result.c_policy .> 0.0)
    @assert all(result.e_policy .>= 0.0)
    @assert abs(sum(result.shock_probs) - 1.0) < 1e-10

    for t in 1:3
        for ip in eachindex(parent_h_grid)
            resources = (1.0 - 0.10) .* [2.0, 2.1, 2.3][t] .* parent_h_grid[ip] .* result.l_policy[t, :, ip] .+ 0.35
            @assert all(result.c_policy[t, :, ip] .+ result.e_policy[t, :, ip] .<= resources .+ 1e-7)
        end
    end

    println("parent_policy_solver.jl demo tests passed")
    println("c_policy size: ", size(result.c_policy))
    println("shock probabilities sum: ", sum(result.shock_probs))
    println("labor range: ", minimum(result.l_policy), " to ", maximum(result.l_policy))
    println("investment min: ", minimum(result.e_policy))
    println("consumption min: ", minimum(result.c_policy))
end


if abspath(PROGRAM_FILE) == @__FILE__
    run_demo_tests()
end
