# Del Boca-side partial workaround for the exported Attanasio tau distribution.
# This file leaves Rev3_Data.jl and the Attanasio bridge unchanged.

include(joinpath(@__DIR__, "Rev3_Data.jl"))

const TAU_WORKAROUND_LOG_COLUMNS = [
    findfirst(==("tau$period"), ATTANASIO_MIXTURE_VARIABLES) for period in 1:3
]

function draw_feasible_attanasio_log_factors(
    distribution::AttanasioMixtureDistribution,
    n_households::Int;
    rng::AbstractRNG,
    max_proposals::Int=max(1000, 100 * n_households),
)
    n_households > 0 || error("The synthetic household count must be positive.")
    max_proposals >= n_households || error("max_proposals must be at least n_households.")

    n_variables = length(ATTANASIO_MIXTURE_VARIABLES)
    accepted_draws = Matrix{Float64}(undef, n_households, n_variables)
    accepted_components = Vector{Int}(undef, n_households)
    accepted = 0
    proposals = 0

    # The exported Gaussian mixture has unbounded support, even though tau is
    # interpreted as a share of the parent's time endowment. Redrawing the full
    # joint vector when any log-tau exceeds zero samples from the exported joint
    # distribution conditional on 0 < exp(log_tau) <= 1 in all three periods.
    # Redrawing the full vector, rather than tau alone, preserves its estimated
    # dependence with human capital, income, investment, and public inputs.
    while accepted < n_households
        remaining_capacity = max_proposals - proposals
        remaining_capacity > 0 || error(
            "Tau workaround accepted only $accepted of $n_households households " *
            "after $proposals proposals. Check the exported tau distribution.",
        )
        batch_size = min(max(64, 4 * (n_households - accepted)), remaining_capacity)
        candidate_draws, candidate_components = draw_attanasio_log_factors(
            distribution, batch_size; rng=rng,
        )
        for candidate in 1:batch_size
            proposals += 1
            all(candidate_draws[candidate, TAU_WORKAROUND_LOG_COLUMNS] .<= 0.0) || continue
            accepted += 1
            accepted_draws[accepted, :] = candidate_draws[candidate, :]
            accepted_components[accepted] = candidate_components[candidate]
            accepted == n_households && break
        end
    end

    return accepted_draws, accepted_components, proposals
end

function load_rev3_tau_workaround_data(
    export_dir::String=attanasio_output_dir();
    maximum_households::Union{Nothing, Int}=nothing,
    seed::Int=synthetic_household_seed(),
)
    distribution = load_attanasio_mixture(export_dir)
    n_households = maximum_households === nothing ?
                   synthetic_household_count() : maximum_households
    n_households > 1 || error("At least two synthetic households are required.")
    log_draws, components, proposals = draw_feasible_attanasio_log_factors(
        distribution, n_households; rng=MersenneTwister(seed),
    )

    level_draws = exp.(log_draws)
    all(isfinite, level_draws) || error(
        "Exponentiating the feasible Attanasio draws produced a nonfinite value.",
    )
    column = Dict(name => index for (index, name) in enumerate(ATTANASIO_MIXTURE_VARIABLES))

    observed = fill(NaN, n_households, 3, 7)
    observed[:, :, 1] .= reshape(level_draws[:, column["peducation"]], n_households, 1)
    for period in 1:3
        observed[:, period, 2] = level_draws[:, column["income$period"]]
        observed[:, period, 3] = level_draws[:, column["govinvest$period"]]
        observed[:, period, 5] = level_draws[:, column["pinvest$period"]]
        observed[:, period, 6] = level_draws[:, column["tau$period"]]
        observed[:, period, 7] = level_draws[:, column["hc$(period + 1)"]]
    end

    all((0.0 .< observed[:, :, 6]) .& (observed[:, :, 6] .<= 1.0)) ||
        error("Tau rejection sampling produced an infeasible time share.")

    empirical_moments = observed_moments(observed)
    households = copy(observed)
    households[:, :, 4:7] .= 0.0
    component_counts = [count(==(component), components) for component in 1:2]
    return (
        households=households,
        observed=observed,
        empirical_moments=empirical_moments,
        ids=collect(1:n_households),
        n_households=n_households,
        component_counts=component_counts,
        proposals=proposals,
        rejected_proposals=proposals - n_households,
        acceptance_rate=n_households / proposals,
        mixture=distribution,
        seed=seed,
        source=:attanasio_mixture_tau_workaround,
        filepath=export_dir,
    )
end
