# Generate example policy-solver scenario data for plotting.
#
# This script uses the standalone parent policy solver, varies one input at a
# time, and writes tidy CSV files that can be plotted by Python/matplotlib.
# It intentionally uses no external Julia packages.

include("parent_policy_solver.jl")


function ensure_dir(path::String)
    isdir(path) || mkpath(path)
end


function csv_escape(x)
    s = string(x)
    if occursin(",", s) || occursin("\"", s) || occursin("\n", s)
        return "\"" * replace(s, "\"" => "\"\"") * "\""
    end
    return s
end


function write_policy_csv(path::String, rows)
    open(path, "w") do io
        println(io, "scenario_family,scenario,parameter_value,period,hc,parent_h,c_policy,l_policy,e_policy,perceived_hc_next_policy,true_expected_hc_next_policy,value_policy")
        for row in rows
            println(io, join(csv_escape.(row), ","))
        end
    end
end


function loggrid(xmin::Float64, xmax::Float64, n::Int, degree::Float64)
    points = collect(range(0.0, 1.0, length=n))
    if xmin == 0.0
        return [xmin + (xmax - xmin) * p^degree for p in points]
    end
    return [xmin * (xmax / xmin)^(p^degree) for p in points]
end


function solve_scenario(; h, public_inputs, rho)
    hc_grid = loggrid(0.5, 2.5, 40, 2.0)
    parent_h_grid = [h]

    return compute_parent_policy(
        hc_grid,
        parent_h_grid,
        [2.0, 2.1, 2.3],         # parent wages over the three parent periods
        [0.35, 0.35, 0.35],      # transfers
        public_inputs,
        [0.35, 0.35, 0.35],      # perceived weight on current child human capital
        [0.35, 0.35, 0.35],      # perceived weight on parental investment
        [0.30, 0.30, 0.30],      # perceived weight on public input
        rho,
        [0.0, 0.0, 0.0],         # perceived productivity intercept d_t
        [0.35, 0.35, 0.35],      # true weight on current child human capital
        [0.35, 0.35, 0.35],      # true weight on parental investment
        [0.30, 0.30, 0.30],      # true weight on public input
        rho,                     # true rho equals perceived rho in these examples
        [0.0, 0.0, 0.0];         # true productivity intercept d_t
        beta=0.93,
        eta=2.0,
        gamma=1.5,
        b=[0.0, 0.0, 40.0],
        tau=[0.10, 0.10, 0.10],
        sigma_h=0.10,
        min_c=1e-8,
        min_e=1e-8,
        periods=3,
        quadrature_n=7,
    )
end


function rows_for_result(family::String, scenario::String, parameter_value, result)
    rows = []
    periods, n_hc, n_parent = size(result.c_policy)
    for t in 1:periods
        for i in 1:n_hc
            for ip in 1:n_parent
                push!(
                    rows,
                    (
                        family,
                        scenario,
                        parameter_value,
                        t,
                        result.hc_grid[i],
                        result.parent_h_grid[ip],
                        result.c_policy[t, i, ip],
                        result.l_policy[t, i, ip],
                        result.e_policy[t, i, ip],
                        result.perceived_hc_next_policy[t, i, ip],
                        result.true_expected_hc_next_policy[t, i, ip],
                        result.value_policy[t, i, ip],
                    ),
                )
            end
        end
    end
    return rows
end


function generate_all_scenarios()
    data_dir = joinpath("output", "policy_plots", "data")
    ensure_dir(data_dir)

    scenario_specs = Dict(
        "parent_human_capital" => [
            ("low h", 0.8, 0.8, [0.9, 1.0, 1.1], [0.20, 0.20, 0.20]),
            ("baseline h", 1.2, 1.2, [0.9, 1.0, 1.1], [0.20, 0.20, 0.20]),
            ("high h", 1.8, 1.8, [0.9, 1.0, 1.1], [0.20, 0.20, 0.20]),
        ],
        "parent_human_capital_complements" => [
            ("low h", 0.8, 0.8, [0.9, 1.0, 1.1], [-0.40, -0.40, -0.40]),
            ("baseline h", 1.2, 1.2, [0.9, 1.0, 1.1], [-0.40, -0.40, -0.40]),
            ("high h", 1.8, 1.8, [0.9, 1.0, 1.1], [-0.40, -0.40, -0.40]),
        ],
        "public_input" => [
            ("low g", 0.6, 1.2, [0.6, 0.7, 0.8], [0.20, 0.20, 0.20]),
            ("baseline g", 1.0, 1.2, [0.9, 1.0, 1.1], [0.20, 0.20, 0.20]),
            ("high g", 1.4, 1.2, [1.2, 1.4, 1.6], [0.20, 0.20, 0.20]),
        ],
        "public_input_complements" => [
            ("low g", 0.6, 1.2, [0.6, 0.7, 0.8], [-0.40, -0.40, -0.40]),
            ("baseline g", 1.0, 1.2, [0.9, 1.0, 1.1], [-0.40, -0.40, -0.40]),
            ("high g", 1.4, 1.2, [1.2, 1.4, 1.6], [-0.40, -0.40, -0.40]),
        ],
        "rho" => [
            ("complements", -0.40, 1.2, [0.9, 1.0, 1.1], [-0.40, -0.40, -0.40]),
            ("baseline rho", 0.20, 1.2, [0.9, 1.0, 1.1], [0.20, 0.20, 0.20]),
            ("substitutes", 0.70, 1.2, [0.9, 1.0, 1.1], [0.70, 0.70, 0.70]),
        ],
    )

    for (family, specs) in scenario_specs
        rows = []
        for (label, parameter_value, h, public_inputs, rho) in specs
            result = solve_scenario(h=h, public_inputs=public_inputs, rho=rho)
            append!(rows, rows_for_result(family, label, parameter_value, result))
        end
        write_policy_csv(joinpath(data_dir, family * ".csv"), rows)
        println("wrote ", joinpath(data_dir, family * ".csv"), " rows=", length(rows))
    end
end


if abspath(PROGRAM_FILE) == @__FILE__
    generate_all_scenarios()
end
