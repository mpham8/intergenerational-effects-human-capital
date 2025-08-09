using Optim
using LinearAlgebra
using DataFrames
using CSV
using IterTools

# =======================================================
# 3-Period Version aligned to original Python 3-period
# =======================================================

# Empirically known parameters (match Python 3-period)
const beta   = 0.96
const gamma_ = 2.0
const eta    = 2.0
const phi    = 0.5
const psi    = 0.3
const xi     = 2.0
const alpha  = 3.0
const e_bar  = 0.1
const g_bar  = 0.01375
const eps1, eps2, eps3 = 1.0, 1.0, 1.0
const G1, G2, G3 = 1.0, 1.0, 1.0

# -------------------------------------------------------
# Household creation
# -------------------------------------------------------
const NUM_PERIODS = 3
function create_simple_household()::Matrix{Float64}
    household = zeros(NUM_PERIODS, 7)
    h_val = 1.0
    household[:, 1] = fill(h_val, NUM_PERIODS)
    household[:, 2] = fill(1.0, NUM_PERIODS)
    household[:, 3] = fill(1.0, NUM_PERIODS)
    return household
end

# -------------------------------------------------------
# Solve household
# -------------------------------------------------------
function solve_household(parameters::Vector{Float64}, household::Matrix{Float64})::Matrix{Float64}
    c1, e1, l1, h2, predh4final = solve_bounded_system_t1(parameters, household)
    c2, e2, l2, h3, l3 = solve_bounded_system_t2(parameters, household, h2)
    c3, e3, l3, h4 = solve_bounded_system_t3(parameters, household, h3; l3_fixed=l3)
    return [c1 e1 l1 h2; c2 e2 l2 h3; c3 e3 l3 h4]
end

# -------------------------------------------------------
# T1
# -------------------------------------------------------
function solve_bounded_system_t1(parameters::Vector{Float64}, household::Matrix{Float64})
    delta = parameters[1]
    rho = parameters[2]
    rho_e = parameters[3]
    theta1, theta2 = parameters[4:end]
    theta3 = 1.0 - theta1 - theta2

    h = household[1, 1]
    w1, w2, w3 = household[:, 2]
    g1, g2, g3 = household[:, 3]
    h1_rho = 1.0

    c_t_star(lt, wt) = ((1.0 - lt) * wt * h) / gamma_
    e_t_star(lt, wt) = begin
        lt_safe = max(lt, 1.0 / (gamma_ + 1.0))
        (wt * h / gamma_) * ((gamma_ + 1) * lt_safe - 1.0)
    end

    function h4c(l1, l2, l3)
        e1 = max(e_t_star(l1, w1), 1e-10)
        e2 = max(e_t_star(l2, w2), 1e-10)
        e3 = max(e_t_star(l3, w3), 1e-10)
        term1 = phi * (eps1 * e1 / e_bar)^psi + (1 - phi) * (G1 * g1 / g_bar)^psi
        term2 = phi * (eps2 * e2 / e_bar)^psi + (1 - phi) * (G2 * g2 / g_bar)^psi
        term3 = phi * (eps3 * e3 / e_bar)^psi + (1 - phi) * (G3 * g3 / g_bar)^psi
        T1 = term1 > 0 ? max(term1, 1e-10)^(rho / psi) : 1e-10
        T2 = term2 > 0 ? max(term2, 1e-10)^(rho / psi) : 1e-10
        T3 = term3 > 0 ? max(term3, 1e-10)^(rho / psi) : 1e-10
        a1 = (1 - theta3) * (1 - theta2) * theta1 * T1 +
             (1 - theta3) * theta2 * T2 +
             theta3 * T3 +
             (1 - theta3) * (1 - theta2) * (1 - theta1) * h1_rho
        return max(a1, 1e-10)^(1.0 / rho)
    end

    function dh4c_de1(l1, l2, l3)
        e1 = max(e_t_star(l1, w1), 1e-10)
        inside = max(phi * (eps1 * e1 / e_bar)^psi + (1 - phi) * (G1 * g1 / g_bar)^psi, 1e-10)
        front = (1 - theta3) * (1 - theta2) * theta1 * phi * (eps1 / e_bar)^psi * (e1^(psi - 1.0))
        h4v = h4c(l1, l2, l3)
        front * inside^((rho / psi) - 1.0) * (h4v^(1.0 - rho))
    end

    function dh4c_de2(l1, l2, l3)
        e2 = max(e_t_star(l2, w2), 1e-10)
        inside = max(phi * (eps2 * e2 / e_bar)^psi + (1 - phi) * (G2 * g2 / g_bar)^psi, 1e-10)
        front = (1 - theta3) * theta2 * phi * (eps2 / e_bar)^psi * (e2^(psi - 1.0))
        h4v = h4c(l1, l2, l3)
        front * inside^((rho / psi) - 1.0) * (h4v^(1.0 - rho))
    end

    function dh4c_de3(l1, l2, l3)
        e3 = max(e_t_star(l3, w3), 1e-10)
        inside = max(phi * (eps3 * e3 / e_bar)^psi + (1 - phi) * (G3 * g3 / g_bar)^psi, 1e-10)
        front = theta3 * phi * (eps3 / e_bar)^psi * (e3^(psi - 1.0))
        h4v = h4c(l1, l2, l3)
        front * inside^((rho / psi) - 1.0) * (h4v^(1.0 - rho))
    end

    function h_2(e1star)
        I1 = (phi * (eps1 * e1star / e_bar)^psi + (1 - phi) * (G1 * g1 / g_bar)^psi)^(rho_e / psi)
        (theta1 * I1 + (1 - theta1) * h1_rho)^(1.0 / rho_e)
    end

    function system(ells::Vector{Float64})
        l1, l2, l3 = ells
        A = (beta^3) * (alpha * log(h + xi) + delta)
        f1 = A * dh4c_de1(l1, l2, l3) - (beta^3) * (c_t_star(l1, w1) * ((1.0 - l1)^gamma_))^(-eta) * ((1.0 - l1)^gamma_)
        f2 = A * dh4c_de2(l1, l2, l3) - (beta^2) * (c_t_star(l2, w2) * ((1.0 - l2)^gamma_))^(-eta) * ((1.0 - l2)^gamma_)
        f3 = A * dh4c_de3(l1, l2, l3) - (beta^1) * (c_t_star(l3, w3) * ((1.0 - l3)^gamma_))^(-eta) * ((1.0 - l3)^gamma_)
        return [f1, f2, f3]
    end

    function system_resid(ells::Vector{Float64})
        r = system(ells)
        return map(x -> isnan(x) || isinf(x) ? 1e10 : x, r)
    end

    min_l = 0.9 / (gamma_ + 1.0)
    max_l = 0.95
    bounds_lower = fill(min_l, 3)
    bounds_upper = fill(max_l, 3)

    initial_guesses = [
        [0.5, 0.5, 0.5], [0.4, 0.4, 0.4], [0.6, 0.6, 0.6], [0.7, 0.7, 0.7],
        [min_l + 0.01, min_l + 0.01, min_l + 0.01], [0.9, 0.9, 0.9],
        [0.5, 0.6, 0.7], [0.7, 0.6, 0.5], [0.4, 0.5, 0.6], [0.6, 0.5, 0.4],
        [0.45, 0.45, 0.45], [0.55, 0.55, 0.55], [0.65, 0.65, 0.65]
    ]

    best_sol = nothing
    best_norm = Inf
    for guess in initial_guesses
        try
            res = optimize(
                x -> sum(abs2, system_resid(x)),
                bounds_lower, bounds_upper, guess,
                Fminbox(NelderMead()),
                Optim.Options(iterations=20000, f_reltol=1e-12, x_abstol=1e-12)
            )
            solx = Optim.minimizer(res)
            nrm = norm(system_resid(solx))
            if nrm < best_norm
                best_norm = nrm
                best_sol = solx
            end
        catch e
            println("T1 failed with guess $guess: $e")
        end
    end

    if best_sol !== nothing && all(isapprox.(best_sol, bounds_lower))
        relaxed_lower = fill(min_l - 0.05, 3)
        for guess in initial_guesses
            try
                res = optimize(
                    x -> sum(abs2, system_resid(x)),
                    relaxed_lower, bounds_upper, guess,
                    Fminbox(NelderMead()),
                    Optim.Options(iterations=20000, f_reltol=1e-12, x_abstol=1e-12)
                )
                solx = Optim.minimizer(res)
                nrm = norm(system_resid(solx))
                if nrm < best_norm
                    best_norm = nrm
                    best_sol = solx
                end
            catch e
                println("T1 (relaxed) failed with guess $guess: $e")
            end
        end
    end

    if best_sol === nothing
        println("No solution found in T1. Returning NaNs.")
        return NaN, NaN, NaN, NaN, NaN
    end

    l1_opt, l2_opt, l3_opt = best_sol
    e1star = e_t_star(l1_opt, w1)
    c1star = c_t_star(l1_opt, w1)
    h2 = h_2(e1star)
    predh4final = h4c(l1_opt, l2_opt, l3_opt)
    return c1star, e1star, l1_opt, h2, predh4final
end

# -------------------------------------------------------
# T2
# -------------------------------------------------------
function solve_bounded_system_t2(parameters::Vector{Float64}, household::Matrix{Float64}, h2::Float64)
    delta = parameters[1]
    rho = parameters[2]
    rho_e = parameters[3]
    theta1, theta2 = parameters[4:end]
    theta3 = 1.0 - theta1 - theta2

    h = household[1, 1]
    w1, w2, w3 = household[:, 2]
    g1, g2, g3 = household[:, 3]

    c_t_star(lt, wt) = ((1.0 - lt) * wt * h) / gamma_
    e_t_star(lt, wt) = begin
        lt_safe = max(lt, 1.0 / (gamma_ + 1.0))
        (wt * h / gamma_) * ((gamma_ + 1) * lt_safe - 1.0)
    end

    function a2(l2, l3)
        e2 = max(e_t_star(l2, w2), 1e-10)
        e3 = max(e_t_star(l3, w3), 1e-10)
        term2 = phi * (eps2 * e2 / e_bar)^psi + (1 - phi) * (G2 * g2 / g_bar)^psi
        term3 = phi * (eps3 * e3 / e_bar)^psi + (1 - phi) * (G3 * g3 / g_bar)^psi
        T2 = term2 > 0 ? max(term2, 1e-10)^(rho / psi) : 1e-10
        T3 = term3 > 0 ? max(term3, 1e-10)^(rho / psi) : 1e-10
        bracket = theta3 * T3 + (1 - theta3) * theta2 * T2 + (1 - theta3) * (1 - theta2) * (h2^rho)
        return max(bracket, 1e-10)
    end

    function dh4c_de2(l2, l3)
        e2 = max(e_t_star(l2, w2), 1e-10)
        inside = max(phi * (eps2 * e2 / e_bar)^psi + (1 - phi) * (G2 * g2 / g_bar)^psi, 1e-10)
        front = (1 - theta3) * theta2 * phi * (eps2 / e_bar)^psi * (e2^(psi - 1.0))
        a2v = a2(l2, l3)
        front * inside^((rho / psi) - 1.0) * (a2v^(1.0 / rho - 1.0))
    end

    function dh4c_de3(l2, l3)
        e3 = max(e_t_star(l3, w3), 1e-10)
        inside = max(phi * (eps3 * e3 / e_bar)^psi + (1 - phi) * (G3 * g3 / g_bar)^psi, 1e-10)
        front = theta3 * phi * (eps3 / e_bar)^psi * (e3^(psi - 1.0))
        a2v = a2(l2, l3)
        front * inside^((rho / psi) - 1.0) * (a2v^(1.0 / rho - 1.0))
    end

    function system(ells::Vector{Float64})
        l2, l3 = ells
        A = (beta^3) * (alpha * log(h + xi) + delta)
        f2 = A * dh4c_de2(l2, l3) - (beta^1) * (c_t_star(l2, w2) * ((1.0 - l2)^gamma_))^(-eta) * ((1.0 - l2)^gamma_)
        f3 = A * dh4c_de3(l2, l3) - (beta^2) * (c_t_star(l3, w3) * ((1.0 - l3)^gamma_))^(-eta) * ((1.0 - l3)^gamma_)
        return [f2, f3]
    end

    function system_resid(ells::Vector{Float64})
        r = system(ells)
        return map(x -> isnan(x) || isinf(x) ? 1e10 : x, r)
    end

    min_l = 0.9 / (gamma_ + 1.0)
    max_l = 0.95
    bounds_lower = fill(min_l, 2)
    bounds_upper = fill(max_l, 2)

    initial_guesses = [
        [0.5, 0.5], [0.4, 0.4], [0.6, 0.6], [0.7, 0.7],
        [min_l + 0.01, min_l + 0.01], [0.9, 0.9],
        [0.6, 0.7], [0.7, 0.6], [0.5, 0.6], [0.6, 0.5],
        [0.45, 0.45], [0.55, 0.55], [0.65, 0.65]
    ]

    best_sol = nothing
    best_norm = Inf
    for g in initial_guesses
        try
            res = optimize(
                x -> sum(abs2, system_resid(x)),
                bounds_lower, bounds_upper, g,
                Fminbox(NelderMead()),
                Optim.Options(iterations=20000, f_reltol=1e-12, x_abstol=1e-12)
            )
            solx = Optim.minimizer(res)
            nrm = norm(system_resid(solx))
            if nrm < best_norm
                best_norm = nrm
                best_sol = solx
            end
        catch e
            println("T2 failed with guess $g: $e")
        end
    end

    if best_sol === nothing
        return NaN, NaN, NaN, NaN, NaN
    end

    l2_opt, l3_opt = best_sol
    e2star = e_t_star(l2_opt, w2)
    c2star = c_t_star(l2_opt, w2)
    I2 = (phi * (eps2 * e2star / e_bar)^psi + (1 - phi) * (G2 * g2 / g_bar)^psi)^(rho_e / psi)
    h3 = (theta2 * I2 + (1 - theta2) * h2)^(1.0 / rho_e)
    return c2star, e2star, l2_opt, h3, l3_opt
end

# -------------------------------------------------------
# T3
# -------------------------------------------------------
function solve_bounded_system_t3(parameters::Vector{Float64}, household::Matrix{Float64}, h3::Float64; l3_fixed::Union{Nothing,Float64}=nothing)
    delta = parameters[1]
    rho = parameters[2]
    rho_e = parameters[3]
    theta1, theta2 = parameters[4:end]
    theta3 = 1.0 - theta1 - theta2

    h = household[1, 1]
    w1, w2, w3 = household[:, 2]
    g1, g2, g3 = household[:, 3]

    c_t_star(lt, wt) = ((1.0 - lt) * wt * h) / gamma_
    e_t_star(lt, wt) = begin
        lt_safe = max(lt, 1.0 / (gamma_ + 1.0))
        (wt * h / gamma_) * ((gamma_ + 1) * lt_safe - 1.0)
    end

    if l3_fixed !== nothing
        l3 = l3_fixed
        e3star = e_t_star(l3, w3)
        c3star = c_t_star(l3, w3)
        I3 = (phi * (eps3 * e3star / e_bar)^psi + (1 - phi) * (G3 * g3 / g_bar)^psi)^(rho_e / psi)
        h4 = (theta3 * I3 + (1 - theta3) * h3)^(1.0 / rho_e)
        return c3star, e3star, l3, h4
    end

    function a2(l3)
        e3 = max(e_t_star(l3, w3), 1e-10)
        term3 = phi * (eps3 * e3 / e_bar)^psi + (1 - phi) * (G3 * g3 / g_bar)^psi
        T3 = term3 > 0 ? max(term3, 1e-10)^(rho / psi) : 1e-10
        bracket = theta3 * T3 + (1 - theta3) * (h3^rho)
        return max(bracket, 1e-10)
    end

    function dh4c_de3(l3)
        e3 = max(e_t_star(l3, w3), 1e-10)
        inside = max(phi * (eps3 * e3 / e_bar)^psi + (1 - phi) * (G3 * g3 / g_bar)^psi, 1e-10)
        front = theta3 * phi * (eps3 / e_bar)^psi * (e3^(psi - 1.0))
        a2v = a2(l3)
        return front * inside^((rho / psi) - 1.0) * (a2v^(1.0 / rho - 1.0))
    end

    function system(ells::Vector{Float64})
        l3 = ells[1]
        A = (beta^3) * (alpha * log(h + xi) + delta)
        f3 = A * dh4c_de3(l3) - (beta^2) * (c_t_star(l3, w3) * ((1.0 - l3)^gamma_))^(-eta) * ((1.0 - l3)^gamma_)
        return [f3]
    end

    function system_resid(ells::Vector{Float64})
        r = system(ells)
        return map(x -> isnan(x) || isinf(x) ? 1e10 : x, r)
    end

    min_l = 0.9 / (gamma_ + 1.0)
    max_l = 0.95
    bounds_lower = [min_l]
    bounds_upper = [max_l]
    initial_guesses = [
        [0.5], [0.4], [0.6], [0.7], [min_l + 0.01], [0.9],
        [0.45], [0.55], [0.65], [0.75], [0.85], [0.8]
    ]

    best_sol = nothing
    best_norm = Inf
    for g in initial_guesses
        try
            res = optimize(
                x -> sum(abs2, system_resid(x)),
                bounds_lower, bounds_upper, g,
                Fminbox(NelderMead()),
                Optim.Options(iterations=20000, f_reltol=1e-12, x_abstol=1e-12)
            )
            solx = Optim.minimizer(res)
            nrm = norm(system_resid(solx))
            if nrm < best_norm
                best_norm = nrm
                best_sol = solx
            end
        catch e
            println("T3 failed with guess $g: $e")
        end
    end

    if best_sol === nothing
        return NaN, NaN, NaN, NaN
    end

    l3 = best_sol[1]
    e3star = e_t_star(l3, w3)
    c3star = c_t_star(l3, w3)
    I3 = (phi * (eps3 * e3star / e_bar)^psi + (1 - phi) * (G3 * g3 / g_bar)^psi)^(rho_e / psi)
    h4 = (theta3 * I3 + (1 - theta3) * h3)^(1.0 / rho_e)
    return c3star, e3star, l3, h4
end

# -------------------------------------------------------
# Test harness
# -------------------------------------------------------
function run_test_simulation(rho_val::Float64, rho_e_val::Float64)::Dict{Symbol,Float64}
    try
        parameters = [20.0, rho_val, rho_e_val, 0.3, 0.2]
        household = create_simple_household()
        c1, e1, l1, h2, predh4final = solve_bounded_system_t1(parameters, household)
        c2, e2, l2, h3, l3 = solve_bounded_system_t2(parameters, household, h2)
        c3, e3, l3, h4 = solve_bounded_system_t3(parameters, household, h3; l3_fixed=l3)
        return Dict(:rho => rho_val, :rho_e => rho_e_val, :predh4final => predh4final, :h4 => h4)
    catch e
        println("Error in run_test_simulation (rho=$rho_val, rho_e=$rho_e_val): $e")
        return Dict(:rho => rho_val, :rho_e => rho_e_val, :predh4final => NaN, :h4 => NaN)
    end
end

function run_all_test_simulations()
    rho_values = range(-2, 0.5, length=5)
    rho_e_values = range(-2, 0.5, length=5)
    rows = Vector{Tuple{Float64,Float64,Float64,Float64}}()
    total = length(rho_values) * length(rho_e_values)
    done = 0
    for (rv, re) in product(rho_values, rho_e_values)
        println("Running simulation $(done+1)/$total: rho=$rv, rho_e=$re")
        d = run_test_simulation(rv, re)
        push!(rows, (d[:rho], d[:rho_e], d[:predh4final], d[:h4]))
        done += 1
        if done % 5 == 0
            df = DataFrame(
                rho = [r[1] for r in rows],
                rho_e = [r[2] for r in rows],
                predh4final = [r[3] for r in rows],
                h4 = [r[4] for r in rows]
            )
            CSV.write("test_results_3periods_intermediate.csv", df)
            println("Saved intermediate results ($done/$total)")
        end
    end
    df = DataFrame(
        rho = [r[1] for r in rows],
        rho_e = [r[2] for r in rows],
        predh4final = [r[3] for r in rows],
        h4 = [r[4] for r in rows]
    )
    CSV.write("julia_test_results_3periods.csv", df)
    println("All test simulations completed. Results saved to julia_test_results_3periods.csv")
end

#run_all_test_simulations()