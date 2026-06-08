# Standalone parent policy solver for equations 4.2.1.27-31.
#
# This file intentionally uses no external Julia packages. The parent policy
# problem is solved with a small custom bounded optimizer so the file can be run
# directly with:
#
#  
#
# The human-capital productivity shock is fixed at epsilon = 1 inside the policy
# solver. If a later simulation needs realized stochastic child human capital,
# apply those shocks after computing the parent policy rules.


"""
    as_period_vector(x, periods)

Return `x` as a vector with one value per parent period. Scalars are repeated;
vectors are copied as floating-point arrays.
"""
function as_period_vector(x, periods::Int)
    if x isa Number
        return fill(Float64(x), periods)
    end
    v = Float64.(collect(x))
    length(v) == periods || error("Expected scalar or vector of length $periods.")
    return v
end


"""
    crra_utility(c, labor, eta, gamma)

CRRA utility over the composite `c * (1 - labor)^gamma`.

This corresponds to:

    u(c, 1-l) = ((c * (1-l)^gamma)^(1-eta) - 1) / (1-eta)

with the log utility limit when `eta` is approximately 1.
"""
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
    perceived_hc_next(hc, e, g, A, theta_h, theta_e, theta_g, rho; min_input)

Perceived human-capital production function used by parents.

For general `rho`, this is the CES form:

    hc' = A * (theta_h * hc^rho + theta_e * e^rho + theta_g * g^rho)^(1/rho)

When `rho` is close to zero, the function uses the Cobb-Douglas limit:

    hc' = A * hc^theta_h * e^theta_e * g^theta_g

The productivity shock is deterministic here: epsilon = 1.
"""
function perceived_hc_next(
    hc::Float64,
    e::Float64,
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
    g_pos = max(g, min_input)

    if abs(rho) < 1e-8
        return A * hc_pos^theta_h * e_pos^theta_e * g_pos^theta_g
    end

    ces_sum = theta_h * hc_pos^rho + theta_e * e_pos^rho + theta_g * g_pos^rho
    if ces_sum <= 0.0
        return 0.0
    end
    return A * ces_sum^(1.0 / rho)
end


"""
    linear_interp(x_grid, y_grid, x)

Simple linear interpolation with linear extrapolation at the endpoints. This is
used for the continuation value V_{t+1}(hc').
"""
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
    golden_maximize(f, left, right; tolerance, max_iter)

Maximize a single-variable function on a closed interval using golden-section
search. This is the one-dimensional building block for the coordinate optimizer.
"""
function golden_maximize(
    f,
    left::Float64,
    right::Float64;
    tolerance::Float64 = 1e-8,
    max_iter::Int = 200,
)
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


"""
    coordinate_maximize(objective, l_start, e_start, max_l, max_e, resources_fn; ...)

Maximize the two-choice parent objective over labor and investment. The budget
constraint is handled by limiting the feasible investment interval for each
labor choice:

    e <= (1 - tau_t) * w_t * h * l + T_t - min_c

The optimizer alternates between maximizing over `e` given `l` and maximizing
over `l` given `e`.
"""
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

        l_min_for_e = 0.0
        l, _ = golden_maximize(
            l_choice -> resources_fn(l_choice) - min_c >= e ? objective(l_choice, e) : -Inf,
            l_min_for_e,
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
    compute_parent_policy(...; keyword arguments...)

Solve the three-period parent dynamic optimization problem.

Required inputs:
- `hc_grid`: grid for current child human capital.
- `h`: parent human capital.
- `wages`, `transfers`, `public_inputs`: scalar or length-`periods` vectors.
- `theta_h`, `theta_e`, `theta_g`, `rho`, `d`: perceived production parameters,
  each scalar or length-`periods` vector.

Keyword inputs include `beta`, `eta`, `gamma`, `b`, and `tau`. The function
returns policy and value arrays with shape `(periods, length(hc_grid))`.
"""
function compute_parent_policy(
    hc_grid,
    h,
    wages,
    transfers,
    public_inputs,
    theta_h,
    theta_e,
    theta_g,
    rho,
    d;
    beta::Float64,
    eta::Float64,
    gamma::Float64,
    b::Float64,
    tau,
    controls = nothing,
    x_coeffs = nothing,
    min_c::Float64 = 1e-8,
    min_e::Float64 = 0.0,
    periods::Int = 3,
    tolerance::Float64 = 1e-8,
)
    hc_grid = Float64.(collect(hc_grid))
    wages = as_period_vector(wages, periods)
    transfers = as_period_vector(transfers, periods)
    public_inputs = as_period_vector(public_inputs, periods)
    theta_h = as_period_vector(theta_h, periods)
    theta_e = as_period_vector(theta_e, periods)
    theta_g = as_period_vector(theta_g, periods)
    rho = as_period_vector(rho, periods)
    d = as_period_vector(d, periods)
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

    n_hc = length(hc_grid)
    c_policy = fill(NaN, periods, n_hc)
    l_policy = fill(NaN, periods, n_hc)
    e_policy = fill(NaN, periods, n_hc)
    hc_next_policy = fill(NaN, periods, n_hc)
    value_policy = fill(-Inf, periods, n_hc)

    # Backward induction over parent periods. The final period values terminal
    # child human capital directly; earlier periods use interpolated continuation
    # values from the next-period value function.
    for t in periods:-1:1
        for ih in 1:n_hc
            hc0 = hc_grid[ih]
            A = exp(d[t] + x_beta[t])
            max_l = 1.0 - min_c

            resources(labor) = (1.0 - tau[t]) * wages[t] * Float64(h) * labor + transfers[t]
            max_resources = resources(max_l)
            max_e = max(min_e, max_resources - min_c)

            function objective(labor, investment)
                c = resources(labor) - investment
                if c <= min_c || investment < min_e || labor < 0.0 || labor >= 1.0
                    return -Inf
                end

                hc1 = perceived_hc_next(
                    hc0,
                    investment,
                    public_inputs[t],
                    A,
                    theta_h[t],
                    theta_e[t],
                    theta_g[t],
                    rho[t],
                )
                if hc1 <= 0.0
                    return -Inf
                end

                current_value = crra_utility(c, labor, eta, gamma)
                if !isfinite(current_value)
                    return -Inf
                end

                if t == periods
                    return current_value + beta * b * log(hc1)
                end
                continuation = linear_interp(hc_grid, vec(value_policy[t + 1, :]), hc1)
                return current_value + beta * continuation
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

            # Multi-start coordinate optimization. The shares below initialize
            # investment as a fraction of the feasible investment range.
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
            best_hc_next = perceived_hc_next(
                hc0,
                best_e,
                public_inputs[t],
                A,
                theta_h[t],
                theta_e[t],
                theta_g[t],
                rho[t],
            )

            c_policy[t, ih] = best_c
            l_policy[t, ih] = best_l
            e_policy[t, ih] = best_e
            hc_next_policy[t, ih] = best_hc_next
            value_policy[t, ih] = best_value
        end
    end

    return (
        c_policy=c_policy,
        l_policy=l_policy,
        e_policy=e_policy,
        hc_next_policy=hc_next_policy,
        value_policy=value_policy,
        hc_grid=hc_grid,
    )
end


"""
    run_demo_tests()

Create test data, solve the parent policy problem, and assert basic economic
and numerical properties, running only when the file is executed as a
script, not when it is included by another Julia file.
"""
function run_demo_tests()
    hc_grid = collect(range(0.5, 2.0, length=8))
    h = 1.2
    wages = [2.0, 2.1, 2.3]
    transfers = [0.35, 0.35, 0.35]
    public_inputs = [0.8, 0.9, 1.0]

    result = compute_parent_policy(
        hc_grid,
        h,
        wages,
        transfers,
        public_inputs,
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
        min_c=1e-8,
        min_e=1e-8,
    )

    @assert all(isfinite, result.c_policy)
    @assert all(isfinite, result.l_policy)
    @assert all(isfinite, result.e_policy)
    @assert all(isfinite, result.hc_next_policy)
    @assert all(isfinite, result.value_policy)
    @assert all(result.l_policy .>= 0.0)
    @assert all(result.l_policy .<= 1.0)
    @assert all(result.c_policy .> 0.0)
    @assert all(result.e_policy .>= 0.0)

    for t in 1:3
        resources = (1.0 - 0.10) .* wages[t] .* h .* result.l_policy[t, :] .+ transfers[t]
        @assert all(result.c_policy[t, :] .+ result.e_policy[t, :] .<= resources .+ 1e-7)
    end

    @assert result.value_policy[3, end] > result.value_policy[3, 1]

    println("parent_policy_solver.jl demo tests passed")
    println("c_policy size: ", size(result.c_policy))
    println("labor range: ", minimum(result.l_policy), " to ", maximum(result.l_policy))
    println("investment min: ", minimum(result.e_policy))
    println("consumption min: ", minimum(result.c_policy))
end


if abspath(PROGRAM_FILE) == @__FILE__
    run_demo_tests()
end
