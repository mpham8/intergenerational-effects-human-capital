using Optim
using LinearAlgebra
using DataFrames
using CSV
using IterTools

# Empirically known parameters
const beta = 0.96 # discount factor
const gamma_ = 2.0 # Disutility of Leisure
const eta = 2.0 # Risk aversion
const phi = 0.5 # Public-Private Share (Jang & Yum, 2021)
const psi = 0.3 # Public-Private Elasticity of Sub. ψ ≤ 1 (Jang & Yum, 2021)
const xi = 2.0 # Placeholder, to be removed in future iterations
const alpha = 3.0 # Placeholder, to be removed in future iterations
const e_bar = 0.1 # Average share of income allocated to parental education investment 
const g_bar = 0.01375 # Average US public education expenditure as a share of GDP
const eps1, eps2, eps3, eps4 = 1.0, 1.0, 1.0, 1.0 # Parental productivity
const G1, G2, G3, G4 = 1.0, 1.0, 1.0, 1.0 # Public education productivity

function solve_household(parameters::Vector{Float64}, household::Matrix{Float64})::Matrix{Float64}
    c1, e1, l1, h2, predh5final = solve_bounded_system_t1(parameters, household)
    c2, e2, l2, h3 = solve_bounded_system_t2(parameters, household, h2)
    c3, e3, l3, h4 = solve_bounded_system_t3(parameters, household, h3)
    c4, e4, l4, h5 = solve_bounded_system_t4(parameters, household, h4)
    return [
        c1 e1 l1 h2;
        c2 e2 l2 h3;
        c3 e3 l3 h4;
        c4 e4 l4 h5
    ]
end

function solve_bounded_system_t1(parameters::Vector{Float64}, household::Matrix{Float64})::Tuple{Float64,Float64,Float64,Float64,Float64}
    delta = parameters[1]
    rho = parameters[2]
    rho_e = parameters[3]
    theta1, theta2, theta3 = parameters[4:end]
    theta4 = 1.0 - theta1 - theta2 - theta3

    h = household[1, 1] # 1-based indexing
    w1, w2, w3, w4 = household[:, 2]
    g1, g2, g3, g4 = household[:, 3]

    h1_rho = 1.0

    c_t_star(lt, wt) = ((1.0 - lt) * wt * h) / gamma_

    e_t_star(lt, wt) = begin
        lt_safe = max(lt, 1.0/(gamma_ + 1.0))
        (wt * h / gamma_) * ((gamma_ + 1) * lt_safe - 1.0)
    end

    function h5c(l1, l2, l3, l4)
        e1_val = max(e_t_star(l1, w1), 1e-10)
        e2_val = max(e_t_star(l2, w2), 1e-10)
        e3_val = max(e_t_star(l3, w3), 1e-10)
        e4_val = max(e_t_star(l4, w4), 1e-10)

        term4 = phi * (eps4 * e4_val / e_bar)^psi + (1-phi) * (G4 * g4 / g_bar)^psi
        term3 = phi * (eps3 * e3_val / e_bar)^psi + (1-phi) * (G3 * g3 / g_bar)^psi
        term2 = phi * (eps2 * e2_val / e_bar)^psi + (1-phi) * (G2 * g2 / g_bar)^psi
        term1 = phi * (eps1 * e1_val / e_bar)^psi + (1-phi) * (G1 * g1 / g_bar)^psi

        term4_pow = term4 > 0 ? max(term4, 1e-10)^(rho/psi) : 1e-10
        term3_pow = term3 > 0 ? max(term3, 1e-10)^(rho/psi) : 1e-10
        term2_pow = term2 > 0 ? max(term2, 1e-10)^(rho/psi) : 1e-10
        term1_pow = term1 > 0 ? max(term1, 1e-10)^(rho/psi) : 1e-10

        bracket = (
            theta4 * term4_pow +
            (1-theta4) * theta3 * term3_pow +
            (1-theta4) * (1-theta3) * theta2 * term2_pow +
            (1-theta4) * (1-theta3) * (1-theta2) * theta1 * term1_pow +
            (1-theta4) * (1-theta3) * (1-theta2) * (1-theta1) * h1_rho
        )
        return max(bracket, 1e-10)^(1.0/rho)
    end

    h_2(e1star) = begin
        I_t = (phi * (eps1 * e1star / e_bar)^psi + (1-phi) * (G1 * g1 / g_bar)^psi)^(rho_e/psi)
        (theta1 * I_t + (1-theta1) * h1_rho)^(1/rho_e)
    end

    function dh5c_de1(l1, l2, l3, l4)
        e1 = max(e_t_star(l1, w1), 1e-10)
        inside = max(phi * (eps1 * e1 / e_bar)^psi + (1.0-phi) * (G1 * g1 / g_bar)^psi, 1e-10)
        front = (1-theta4) * (1-theta3) * (1-theta2) * theta1 * phi * ((eps1/e_bar)^psi) * (e1^(psi - 1.0))
        h5c_val = h5c(l1, l2, l3, l4)
        front * inside^((rho/psi) - 1.0) * (h5c_val^(1.0 - rho))
    end

    function dh5c_de2(l1, l2, l3, l4)
        e2 = max(e_t_star(l2, w2), 1e-10)
        inside = max(phi * (eps2 * e2 / e_bar)^psi + (1.0-phi) * (G2 * g2 / g_bar)^psi, 1e-10)
        front = (1-theta4) * (1-theta3) * theta2 * phi * ((eps2/e_bar)^psi) * (e2^(psi - 1.0))
        h5c_val = h5c(l1, l2, l3, l4)
        front * inside^((rho/psi) - 1.0) * (h5c_val^(1.0 - rho))
    end

    function dh5c_de3(l1, l2, l3, l4)
        e3 = max(e_t_star(l3, w3), 1e-10)
        inside = max(phi * (eps3 * e3 / e_bar)^psi + (1.0-phi) * (G3 * g3 / g_bar)^psi, 1e-10)
        front = (1-theta4) * theta3 * phi * ((eps3/e_bar)^psi) * (e3^(psi - 1.0))
        h5c_val = h5c(l1, l2, l3, l4)
        front * inside^((rho/psi) - 1.0) * (h5c_val^(1.0 - rho))
    end

    function dh5c_de4(l1, l2, l3, l4)
        e4 = max(e_t_star(l4, w4), 1e-10)
        inside = max(phi * (eps4 * e4 / e_bar)^psi + (1.0-phi) * (G4 * g4 / g_bar)^psi, 1e-10)
        front = theta4 * phi * ((eps4/e_bar)^psi) * (e4^(psi - 1.0))
        h5c_val = h5c(l1, l2, l3, l4)
        front * inside^((rho/psi) - 1.0) * (h5c_val^(1.0 - rho))
    end

    function system(ells::Vector{Float64})::Vector{Float64}
        l1, l2, l3, l4 = ells
        A = (beta^4) * (alpha * log(h + xi) + delta)

        f1 = A * dh5c_de1(l1, l2, l3, l4) - (c_t_star(l1, w1) * ((1.0-l1)^gamma_))^(-eta) * ((1.0-l1)^gamma_)
        f2 = A * dh5c_de2(l1, l2, l3, l4) - beta * (c_t_star(l2, w2) * ((1.0-l2)^gamma_))^(-eta) * ((1.0-l2)^gamma_)
        f3 = A * dh5c_de3(l1, l2, l3, l4) - (beta^2) * (c_t_star(l3, w3) * ((1.0-l3)^gamma_))^(-eta) * ((1.0-l3)^gamma_)
        f4 = A * dh5c_de4(l1, l2, l3, l4) - (beta^3) * (c_t_star(l4, w4) * ((1.0-l4)^gamma_))^(-eta) * ((1.0-l4)^gamma_)
        return [f1, f2, f3, f4]
    end

    function system_resid(ells::Vector{Float64})::Vector{Float64}
        try
            return system(ells)
        catch
            return fill(1e10, 4)
        end
    end

    min_l = 1.0/(gamma_ + 1.0)
    bounds_lower = fill(min_l, 4)
    bounds_upper = fill(0.9999, 4)

    initial_guesses = [
        [0.5, 0.5, 0.5, 0.5],
        [0.4, 0.4, 0.4, 0.4],
        [0.6, 0.6, 0.6, 0.6],
        [0.7, 0.7, 0.7, 0.7],
        [min_l + 0.01, min_l + 0.01, min_l + 0.01, min_l + 0.01],
        [0.9, 0.9, 0.9, 0.9],
        [0.5, 0.6, 0.7, 0.8],
        [0.8, 0.7, 0.6, 0.5],
        [0.4, 0.5, 0.6, 0.7],
        [0.7, 0.6, 0.5, 0.4],
        [0.45, 0.45, 0.45, 0.45],
        [0.55, 0.55, 0.55, 0.55],
        [0.65, 0.65, 0.65, 0.65]
    ]

    best_sol = nothing
    best_norm = Inf

    for guess in initial_guesses
        try
            result = optimize(
                x -> sum(abs2, system_resid(x)),
                bounds_lower,
                bounds_upper,
                guess,
                Fminbox(NelderMead()),
                Optim.Options(iterations=20000, f_reltol=1e-12, x_abstol=1e-12)
            )
            sol_x = Optim.minimizer(result)
            norm_val = norm(system_resid(sol_x))
            if norm_val < best_norm
                best_norm = norm_val
                best_sol = sol_x
            end
        catch e
            println("Failed with guess $guess: $e")
            continue
        end
    end

    if best_sol !== nothing && all(isapprox.(best_sol, bounds_lower))
        relaxed_bounds_lower = fill(min_l - 0.05, 4)
        for guess in initial_guesses
            try
                result = optimize(
                    x -> sum(abs2, system_resid(x)),
                    relaxed_bounds_lower,
                    bounds_upper,
                    guess,
                    Fminbox(NelderMead()),
                    Optim.Options(iterations=20000, f_reltol=1e-12, x_abstol=1e-12)
                )
                sol_x = Optim.minimizer(result)
                norm_val = norm(system_resid(sol_x))
                if norm_val < best_norm
                    best_norm = norm_val
                    best_sol = sol_x
                end
            catch e
                println("Failed with relaxed bounds and guess $guess: $e")
                continue
            end
        end
    end

    sol_x = best_sol !== nothing ? best_sol : fill(min_l, 4)

    e1star = e_t_star(sol_x[1], w1)
    c1star = c_t_star(sol_x[1], w1)
    predh5final = h5c(sol_x[1], sol_x[2], sol_x[3], sol_x[4])

    return c1star, e1star, sol_x[1], h_2(e1star), predh5final
end

function solve_bounded_system_t2(parameters::Vector{Float64}, household::Matrix{Float64}, h2::Float64)::Tuple{Float64,Float64,Float64,Float64}
    delta = parameters[1]
    rho = parameters[2]
    rho_e = parameters[3]
    theta1, theta2, theta3 = parameters[4:end]
    theta4 = 1.0 - theta1 - theta2 - theta3

    h = household[1, 1]
    w1, w2, w3, w4 = household[:, 2]
    g1, g2, g3, g4 = household[:, 3]

    h1_rho = 1.0

    c_t_star(lt, wt) = ((1.0 - lt) * wt * h) / gamma_

    e_t_star(lt, wt) = begin
        lt_safe = max(lt, 1.0/(gamma_ + 1.0))
        (wt * h / gamma_) * ((gamma_ + 1) * lt_safe - 1.0)
    end

    function a2(l2, l3, l4)
        e2_val = max(e_t_star(l2, w2), 1e-10)
        e3_val = max(e_t_star(l3, w3), 1e-10)
        e4_val = max(e_t_star(l4, w4), 1e-10)

        term4 = phi * (eps4 * e4_val / e_bar)^psi + (1-phi) * (G4 * g4 / g_bar)^psi
        term3 = phi * (eps3 * e3_val / e_bar)^psi + (1-phi) * (G3 * g3 / g_bar)^psi
        term2 = phi * (eps2 * e2_val / e_bar)^psi + (1-phi) * (G2 * g2 / g_bar)^psi

        term4_pow = term4 > 0 ? max(term4, 1e-10)^(rho/psi) : 1e-10
        term3_pow = term3 > 0 ? max(term3, 1e-10)^(rho/psi) : 1e-10
        term2_pow = term2 > 0 ? max(term2, 1e-10)^(rho/psi) : 1e-10

        bracket = (
            theta4 * term4_pow +
            (1-theta4) * theta3 * term3_pow +
            (1-theta4) * (1-theta3) * theta2 * term2_pow +
            (1-theta4) * (1-theta3) * (1-theta2) * (h2^rho)
        )
        return max(bracket, 1e-10)
    end

    h_3(e2star) = begin
        I_t = (phi * (eps2 * e2star / e_bar)^psi + (1-phi) * (G2 * g2 / g_bar)^psi)^(1/psi)
        (theta2 * (I_t^rho_e) + (1-theta2) * (h2^rho_e))^(1/rho_e)
    end

    function dh5c_de2(l2, l3, l4)
        e2 = max(e_t_star(l2, w2), 1e-10)
        inside = max(phi * (eps2 * e2 / e_bar)^psi + (1.0-phi) * (G2 * g2 / g_bar)^psi, 1e-10)
        front = (1-theta4) * (1-theta3) * theta2 * phi * ((eps2/e_bar)^psi) * (e2^(psi - 1.0))
        a2_val = a2(l2, l3, l4)
        front * inside^((rho/psi) - 1.0) * (a2_val^(1/rho - 1))
    end

    function dh5c_de3(l2, l3, l4)
        e3 = max(e_t_star(l3, w3), 1e-10)
        inside = max(phi * (eps3 * e3 / e_bar)^psi + (1.0-phi) * (G3 * g3 / g_bar)^psi, 1e-10)
        front = (1-theta4) * theta3 * phi * ((eps3/e_bar)^psi) * (e3^(psi - 1.0))
        a2_val = a2(l2, l3, l4)
        front * inside^((rho/psi) - 1.0) * (a2_val^(1/rho - 1))
    end

    function dh5c_de4(l2, l3, l4)
        e4 = max(e_t_star(l4, w4), 1e-10)
        inside = max(phi * (eps4 * e4 / e_bar)^psi + (1.0-phi) * (G4 * g4 / g_bar)^psi, 1e-10)
        front = theta4 * phi * ((eps4/e_bar)^psi) * (e4^(psi - 1.0))
        a2_val = a2(l2, l3, l4)
        front * inside^((rho/psi) - 1.0) * (a2_val^(1/rho - 1))
    end

    function system(ells::Vector{Float64})::Vector{Float64}
        l2, l3, l4 = ells
        A = (beta^4) * (alpha * log(h + xi) + delta)

        f2 = A * dh5c_de2(l2, l3, l4) - beta * (c_t_star(l2, w2) * ((1.0-l2)^gamma_))^(-eta) * ((1.0-l2)^gamma_)
        f3 = A * dh5c_de3(l2, l3, l4) - (beta^2) * (c_t_star(l3, w3) * ((1.0-l3)^gamma_))^(-eta) * ((1.0-l3)^gamma_)
        f4 = A * dh5c_de4(l2, l3, l4) - (beta^3) * (c_t_star(l4, w4) * ((1.0-l4)^gamma_))^(-eta) * ((1.0-l4)^gamma_)
        return [f2, f3, f4]
    end

    function system_resid(ells::Vector{Float64})::Vector{Float64}
        try
            return system(ells)
        catch
            return fill(1e10, 3)
        end
    end

    min_l = 1.0/(gamma_ + 1.0)
    bounds_lower = fill(min_l, 3)
    bounds_upper = fill(0.9999, 3)

    initial_guesses = [
        [0.5, 0.5, 0.5],
        [0.4, 0.4, 0.4],
        [0.6, 0.6, 0.6],
        [0.7, 0.7, 0.7],
        [min_l + 0.01, min_l + 0.01, min_l + 0.01],
        [0.9, 0.9, 0.9],
        [0.6, 0.7, 0.8],
        [0.7, 0.6, 0.5],
        [0.5, 0.6, 0.7],
        [0.6, 0.5, 0.4],
        [0.45, 0.45, 0.45],
        [0.55, 0.55, 0.55],
        [0.65, 0.65, 0.65]
    ]

    best_sol = nothing
    best_norm = Inf

    for guess in initial_guesses
        try
            result = optimize(
                x -> sum(abs2, system_resid(x)),
                bounds_lower,
                bounds_upper,
                guess,
                Fminbox(NelderMead()),
                Optim.Options(iterations=20000, f_reltol=1e-12, x_abstol=1e-12)
            )
            sol_x = Optim.minimizer(result)
            norm_val = norm(system_resid(sol_x))
            if norm_val < best_norm
                best_norm = norm_val
                best_sol = sol_x
            end
        catch e
            println("Failed with guess $guess: $e")
            continue
        end
    end

    if best_sol !== nothing && all(isapprox.(best_sol, bounds_lower))
        relaxed_bounds_lower = fill(min_l - 0.05, 3)
        for guess in initial_guesses
            try
                result = optimize(
                    x -> sum(abs2, system_resid(x)),
                    relaxed_bounds_lower,
                    bounds_upper,
                    guess,
                    Fminbox(NelderMead()),
                    Optim.Options(iterations=20000, f_reltol=1e-12, x_abstol=1e-12)
                )
                sol_x = Optim.minimizer(result)
                norm_val = norm(system_resid(sol_x))
                if norm_val < best_norm
                    best_norm = norm_val
                    best_sol = sol_x
                end
            catch e
                println("Failed with relaxed bounds and guess $guess: $e")
                continue
            end
        end
    end

    sol_x = best_sol !== nothing ? best_sol : fill(min_l, 3)

    e2star = e_t_star(sol_x[1], w2)
    c2star = c_t_star(sol_x[1], w2)

    return c2star, e2star, sol_x[1], h_3(e2star)
end

function solve_bounded_system_t3(parameters::Vector{Float64}, household::Matrix{Float64}, h3::Float64)::Tuple{Float64,Float64,Float64,Float64}
    delta = parameters[1]
    rho = parameters[2]
    rho_e = parameters[3]
    theta1, theta2, theta3 = parameters[4:end]
    theta4 = 1.0 - theta1 - theta2 - theta3

    h = household[1, 1]
    w1, w2, w3, w4 = household[:, 2]
    g1, g2, g3, g4 = household[:, 3]

    h1_rho = 1.0

    c_t_star(lt, wt) = ((1.0 - lt) * wt * h) / gamma_

    e_t_star(lt, wt) = begin
        lt_safe = max(lt, 1.0/(gamma_ + 1.0))
        (wt * h / gamma_) * ((gamma_ + 1) * lt_safe - 1.0)
    end

    function a2(l3, l4)
        e3_val = max(e_t_star(l3, w3), 1e-10)
        e4_val = max(e_t_star(l4, w4), 1e-10)

        term4 = phi * (eps4 * e4_val / e_bar)^psi + (1-phi) * (G4 * g4 / g_bar)^psi
        term3 = phi * (eps3 * e3_val / e_bar)^psi + (1-phi) * (G3 * g3 / g_bar)^psi

        term4_pow = term4 > 0 ? max(term4, 1e-10)^(rho/psi) : 1e-10
        term3_pow = term3 > 0 ? max(term3, 1e-10)^(rho/psi) : 1e-10

        bracket = (
            theta4 * term4_pow +
            (1-theta4) * theta3 * term3_pow +
            (1-theta4) * (1-theta3) * (h3^rho)
        )
        return max(bracket, 1e-10)
    end

    h_4(e3star) = begin
        I_t = (phi * (eps3 * e3star / e_bar)^psi + (1-phi) * (G3 * g3 / g_bar)^psi)^(1/psi)
        (theta3 * (I_t^rho_e) + (1-theta3) * (h3^rho_e))^(1/rho_e)
    end

    function dh5c_de3(l3, l4)
        e3 = max(e_t_star(l3, w3), 1e-10)
        inside = max(phi * (eps3 * e3 / e_bar)^psi + (1.0-phi) * (G3 * g3 / g_bar)^psi, 1e-10)
        front = (1-theta4) * theta3 * phi * ((eps3/e_bar)^psi) * (e3^(psi - 1.0))
        a2_val = a2(l3, l4)
        front * inside^((rho/psi) - 1.0) * (a2_val^(1/rho - 1))
    end

    function dh5c_de4(l3, l4)
        e4 = max(e_t_star(l4, w4), 1e-10)
        inside = max(phi * (eps4 * e4 / e_bar)^psi + (1.0-phi) * (G4 * g4 / g_bar)^psi, 1e-10)
        front = theta4 * phi * ((eps4/e_bar)^psi) * (e4^(psi - 1.0))
        a2_val = a2(l3, l4)
        front * inside^((rho/psi) - 1.0) * (a2_val^(1/rho - 1))
    end

    function system(ells::Vector{Float64})::Vector{Float64}
        l3, l4 = ells
        A = (beta^4) * (alpha * log(h + xi) + delta)

        f3 = A * dh5c_de3(l3, l4) - (beta^2) * (c_t_star(l3, w3) * ((1.0-l3)^gamma_))^(-eta) * ((1.0-l3)^gamma_)
        f4 = A * dh5c_de4(l3, l4) - (beta^3) * (c_t_star(l4, w4) * ((1.0-l4)^gamma_))^(-eta) * ((1.0-l4)^gamma_)
        return [f3, f4]
    end

    function system_resid(ells::Vector{Float64})::Vector{Float64}
        try
            return system(ells)
        catch
            return fill(1e10, 2)
        end
    end

    min_l = 1.0/(gamma_ + 1.0)
    bounds_lower = fill(min_l, 2)
    bounds_upper = fill(0.9999, 2)

    initial_guesses = [
        [0.5, 0.5],
        [0.4, 0.4],
        [0.6, 0.6],
        [0.7, 0.7],
        [min_l + 0.01, min_l + 0.01],
        [0.9, 0.9],
        [0.6, 0.7],
        [0.7, 0.6],
        [0.5, 0.6],
        [0.6, 0.5],
        [0.45, 0.45],
        [0.55, 0.55],
        [0.65, 0.65]
    ]

    best_sol = nothing
    best_norm = Inf

    for guess in initial_guesses
        try
            result = optimize(
                x -> sum(abs2, system_resid(x)),
                bounds_lower,
                bounds_upper,
                guess,
                Fminbox(NelderMead()),
                Optim.Options(iterations=20000, f_reltol=1e-12, x_abstol=1e-12)
            )
            sol_x = Optim.minimizer(result)
            norm_val = norm(system_resid(sol_x))
            if norm_val < best_norm
                best_norm = norm_val
                best_sol = sol_x
            end
        catch e
            println("Failed with guess $guess: $e")
            continue
        end
    end

    if best_sol !== nothing && all(isapprox.(best_sol, bounds_lower))
        relaxed_bounds_lower = fill(min_l - 0.05, 2)
        for guess in initial_guesses
            try
                result = optimize(
                    x -> sum(abs2, system_resid(x)),
                    relaxed_bounds_lower,
                    bounds_upper,
                    guess,
                    Fminbox(NelderMead()),
                    Optim.Options(iterations=20000, f_reltol=1e-12, x_abstol=1e-12)
                )
                sol_x = Optim.minimizer(result)
                norm_val = norm(system_resid(sol_x))
                if norm_val < best_norm
                    best_norm = norm_val
                    best_sol = sol_x
                end
            catch e
                println("Failed with relaxed bounds and guess $guess: $e")
                continue
            end
        end
    end

    sol_x = best_sol !== nothing ? best_sol : fill(min_l, 2)

    e3star = e_t_star(sol_x[1], w3)
    c3star = c_t_star(sol_x[1], w3)

    return c3star, e3star, sol_x[1], h_4(e3star)
end

function solve_bounded_system_t4(parameters::Vector{Float64}, household::Matrix{Float64}, h4::Float64)::Tuple{Float64,Float64,Float64,Float64}
    delta = parameters[1]
    rho = parameters[2]
    rho_e = parameters[3]
    theta1, theta2, theta3 = parameters[4:end]
    theta4 = 1.0 - theta1 - theta2 - theta3

    h = household[1, 1]
    w1, w2, w3, w4 = household[:, 2]
    g1, g2, g3, g4 = household[:, 3]

    h1_rho = 1.0

    c_t_star(lt, wt) = ((1.0 - lt) * wt * h) / gamma_

    e_t_star(lt, wt) = begin
        lt_safe = max(lt, 1.0/(gamma_ + 1.0))
        (wt * h / gamma_) * ((gamma_ + 1) * lt_safe - 1.0)
    end

    function a2(l4)
        e4_val = max(e_t_star(l4, w4), 1e-10)

        term4 = phi * (eps4 * e4_val / e_bar)^psi + (1-phi) * (G4 * g4 / g_bar)^psi

        term4_pow = term4 > 0 ? max(term4, 1e-10)^(rho/psi) : 1e-10

        bracket = (
            theta4 * term4_pow +
            (1-theta4) * (h4^rho)
        )
        return max(bracket, 1e-10)
    end

    h_5(e4star) = begin
        I_t = (phi * (eps4 * e4star / e_bar)^psi + (1-phi) * (G4 * g4 / g_bar)^psi)^(1/psi)
        (theta4 * (I_t^rho_e) + (1-theta4) * (h4^rho_e))^(1/rho_e)
    end

    function dh5c_de4(l4)
        e4 = max(e_t_star(l4, w4), 1e-10)
        inside = max(phi * (eps4 * e4 / e_bar)^psi + (1.0-phi) * (G4 * g4 / g_bar)^psi, 1e-10)
        front = theta4 * phi * ((eps4/e_bar)^psi) * (e4^(psi - 1.0))
        a2_val = a2(l4)
        front * inside^((rho/psi) - 1.0) * (a2_val^(1/rho - 1))
    end

    function system(ells::Vector{Float64})::Vector{Float64}
        l4 = ells[1]
        A = (beta^4) * (alpha * log(h + xi) + delta)

        f4 = A * dh5c_de4(l4) - (beta^3) * (c_t_star(l4, w4) * ((1.0-l4)^gamma_))^(-eta) * ((1.0-l4)^gamma_)
        return [f4]
    end

    function system_resid(ells::Vector{Float64})::Vector{Float64}
        try
            return system(ells)
        catch
            return [1e10]
        end
    end

    min_l = 1.0/(gamma_ + 1.0)
    bounds_lower = [min_l]
    bounds_upper = [0.9999]

    initial_guesses = [
        [0.5],
        [0.4],
        [0.6],
        [0.7],
        [min_l + 0.01],
        [0.9],
        [0.45],
        [0.55],
        [0.65],
        [0.75],
        [0.85],
        [0.8]
    ]

    best_sol = nothing
    best_norm = Inf

    for guess in initial_guesses
        try
            result = optimize(
                x -> sum(abs2, system_resid(x)),
                bounds_lower,
                bounds_upper,
                guess,
                Fminbox(NelderMead()),
                Optim.Options(iterations=20000, f_reltol=1e-12, x_abstol=1e-12)
            )
            sol_x = Optim.minimizer(result)
            norm_val = norm(system_resid(sol_x))
            if norm_val < best_norm
                best_norm = norm_val
                best_sol = sol_x
            end
        catch e
            println("Failed with guess $guess: $e")
            continue
        end
    end

    if best_sol !== nothing && all(isapprox.(best_sol, bounds_lower))
        relaxed_bounds_lower = [min_l - 0.05]
        for guess in initial_guesses
            try
                result = optimize(
                    x -> sum(abs2, system_resid(x)),
                    relaxed_bounds_lower,
                    bounds_upper,
                    guess,
                    Fminbox(NelderMead()),
                    Optim.Options(iterations=20000, f_reltol=1e-12, x_abstol=1e-12)
                )
                sol_x = Optim.minimizer(result)
                norm_val = norm(system_resid(sol_x))
                if norm_val < best_norm
                    best_norm = norm_val
                    best_sol = sol_x
                end
            catch e
                println("Failed with relaxed bounds and guess $guess: $e")
                continue
            end
        end
    end

    sol_x = best_sol !== nothing ? best_sol : [min_l]

    e4star = e_t_star(sol_x[1], w4)
    c4star = c_t_star(sol_x[1], w4)

    return c4star, e4star, sol_x[1], h_5(e4star)
end

const NUM_PERIODS = 4

function create_simple_household()::Matrix{Float64}
    household = zeros(NUM_PERIODS, 7)
    h_val = 0.7
    household[:, 1] = fill(h_val, NUM_PERIODS)
    household[:, 2] = fill(1.0, NUM_PERIODS)
    household[:, 3] = fill(0.013, NUM_PERIODS)
    return household
end

function run_test_simulation(rho_val::Float64, rho_e_val::Float64)::Dict{Symbol,Float64}
    try
        parameters = [20.0, rho_val, rho_e_val, 0.3, 0.2, 0.2]
        household = create_simple_household()
        solution = solve_household(parameters, household)

        c1, e1, l1, h2 = solution[1, :]
        c2, e2, l2, h3 = solution[2, :]
        c3, e3, l3, h4 = solution[3, :]
        c4, e4, l4, h5 = solution[4, :]

        predh5final = solution[1, 4]

        return Dict(
            :rho => rho_val,
            :rho_e => rho_e_val,
            :predh5final => predh5final,
            :h5 => h5
        )
    catch e
        println("Error with rho=$rho_val, rho_e=$rho_e_val: $e")
        return Dict(
            :rho => rho_val,
            :rho_e => rho_e_val,
            :predh5final => NaN,
            :h5 => NaN
        )
    end
end

function run_all_test_simulations()
    rho_values = range(-2, 0.5, length=5)
    rho_e_values = range(-2, 0.5, length=5)

    results = Dict{Symbol,Float64}[]
    total_combinations = length(rho_values) * length(rho_e_values)
    completed = 0

    for (rho_val, rho_e_val) in product(rho_values, rho_e_values)
        println("Running simulation $(completed+1)/$total_combinations: rho=$rho_val, rho_e=$rho_e_val")
        result = run_test_simulation(rho_val, rho_e_val)
        push!(results, result)
        completed += 1

        if completed % 5 == 0
            df = DataFrame(results)
            CSV.write("test_results_intermediate.csv", df)
            println("Saved intermediate results ($completed/$total_combinations)")
        end
    end

    df = DataFrame(results)
    CSV.write("test_results.csv", df)
    println("All test simulations completed. Results saved to test_results.csv")
end

#run_all_test_simulations()

