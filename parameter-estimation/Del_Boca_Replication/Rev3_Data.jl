using CSV
using DataFrames
using LinearAlgebra
using Random
using Statistics

const DEFAULT_IEHC_NLSY_DATA_FILE = normpath(
    joinpath(
        @__DIR__, "..", "..", "data-preprocessing", "Fake Merged Data",
        "Fake_Merged_Data_with_cex_education_expenditure.csv",
    ),
)
const DEFAULT_IEHC_ATTANASIO_OUTPUT_DIR = normpath(
    joinpath(@__DIR__, "..", "Attanasio Replication", "VDE DelBoca Export"),
)
const REV3_BRIDGE_FILENAME = "attanasio_rev3_bridge.csv"
const MIXTURE_PROBABILITIES_FILENAME = "mixture_probabilities.csv"
const MIXTURE_MEANS_FILENAME = "mixture_means.csv"
const MIXTURE_COVARIANCE_FILENAMES =
    ["mixture_covariance_1.csv", "mixture_covariance_2.csv"]
const ATTANASIO_MIXTURE_VARIABLES = [
    "hc2", "hc3", "hc4", "tau1", "tau2", "tau3", "pinvest1", "pinvest2",
    "pinvest3", "govinvest1", "govinvest2", "govinvest3", "peducation",
    "income1", "income2", "income3", "centerprice1", "familyprice1",
    "centerprice2", "familyprice2", "centerprice3", "familyprice3",
]
const CEX_EXPENDITURE_COLUMN =
    "CEX Cumulative Household Education Expenditure (Real 2026 Dollars)"
const CEX_MATCHED_YEARS_COLUMN = "CEX Years Successfully Matched"
const CEX_COVERAGE_COLUMN = "CEX Coverage Share"
const HEAD_START_EXPOSURE_COLUMN = "CHILD_EVER_ENROLLED_IN_HEAD"
const PARENT_EDUCATION_COLUMN = "HGC_OF_MOTHER_AS_OF_MAY_1_R"

Base.@kwdef struct TrueCESTechnology
    intercept::Vector{Float64}
    parent_education_coefficient::Vector{Float64}
    theta_h::Vector{Float64}
    theta_p::Vector{Float64}
    ces_delta::Vector{Float64}
    mu::Vector{Float64}
    rho::Vector{Float64}
    control_function_coefficient::Vector{Float64}
    residual_sd::Vector{Float64}
end

Base.@kwdef struct AttanasioMixtureDistribution
    probabilities::Vector{Float64}
    means::Matrix{Float64}
    covariances::Vector{Matrix{Float64}}
    factors::Vector{Matrix{Float64}}
end

function nlsy_data_file()
    return get(ENV, "IEHC_NLSY_DATA_FILE", DEFAULT_IEHC_NLSY_DATA_FILE)
end

function attanasio_output_dir()
    return get(ENV, "IEHC_ATTANASIO_OUTPUT_DIR", DEFAULT_IEHC_ATTANASIO_OUTPUT_DIR)
end

function attanasio_bridge_file()
    return joinpath(attanasio_output_dir(), REV3_BRIDGE_FILENAME)
end

function cex_minimum_coverage()
    value = parse(Float64, get(ENV, "IEHC_MIN_CEX_COVERAGE", "0.5"))
    0.0 <= value <= 1.0 || error("IEHC_MIN_CEX_COVERAGE must be between zero and one.")
    return value
end

function synthetic_household_count()
    value = parse(Int, get(ENV, "IEHC_SYNTHETIC_HOUSEHOLDS", "10000"))
    value > 0 || error("IEHC_SYNTHETIC_HOUSEHOLDS must be positive.")
    return value
end

function synthetic_household_seed()
    return parse(Int, get(ENV, "IEHC_SYNTHETIC_SEED", "20260725"))
end

function validate_true_technology(technology::TrueCESTechnology)
    fields = fieldnames(TrueCESTechnology)
    for field in fields
        values = getfield(technology, field)
        length(values) == 3 || error("Attanasio bridge field $field must contain three periods.")
        all(isfinite, values) || error("Attanasio bridge field $field contains a nonfinite value.")
    end
    all(0.0 .< technology.theta_h .< 1.0) ||
        error("Attanasio theta_h must lie strictly between zero and one.")
    all(0.0 .< technology.theta_p .< 1.0) ||
        error("Attanasio theta_p must lie strictly between zero and one.")
    all(0.0 .< technology.ces_delta .< 1.0) ||
        error("Attanasio ces_delta must lie strictly between zero and one.")
    all(-5.0 .<= technology.mu .<= 1.0) ||
        error("Attanasio mu must lie in [-5, 1].")
    all(-5.0 .<= technology.rho .<= 1.0) ||
        error("Attanasio rho must lie in [-5, 1].")
    all(technology.residual_sd .> 0.0) ||
        error("Attanasio residual standard deviations must be positive.")
    return technology
end

function load_true_technology(filepath::String=attanasio_bridge_file())
    isfile(filepath) || error(
        "Attanasio bridge not found at $filepath. Run the Attanasio stage before Rev3.",
    )
    bridge = CSV.read(filepath, DataFrame)
    required = [
        "period", "intercept", "parent_education_coefficient", "theta_h", "theta_p",
        "ces_delta", "mu", "rho", "control_function_coefficient", "residual_sd",
    ]
    missing_columns = setdiff(required, names(bridge))
    isempty(missing_columns) ||
        error("Attanasio bridge is missing columns: $(join(missing_columns, ", ")).")
    sort!(bridge, :period)
    bridge.period == collect(1:3) ||
        error("Attanasio bridge must contain exactly periods 1, 2, and 3.")

    technology = TrueCESTechnology(
        intercept=Float64.(bridge.intercept),
        parent_education_coefficient=Float64.(bridge.parent_education_coefficient),
        theta_h=Float64.(bridge.theta_h),
        theta_p=Float64.(bridge.theta_p),
        ces_delta=Float64.(bridge.ces_delta),
        mu=Float64.(bridge.mu),
        rho=Float64.(bridge.rho),
        control_function_coefficient=Float64.(bridge.control_function_coefficient),
        residual_sd=Float64.(bridge.residual_sd),
    )
    return validate_true_technology(technology)
end

function finite_observations(values)
    return Float64[
        Float64(value) for value in values
        if !ismissing(value) && value isa Real && isfinite(Float64(value))
    ]
end

function median_impute(values)
    finite = finite_observations(values)
    isempty(finite) && error("A required data column has no finite observations.")
    replacement = median(finite)
    return Float64[
        ismissing(value) || !(value isa Real) || !isfinite(Float64(value)) ?
        replacement : Float64(value)
        for value in values
    ]
end

function positive_median(values)
    finite = filter(>(0.0), finite_observations(values))
    isempty(finite) && error("A required monetary column has no positive observations.")
    return median(finite)
end

function scaled_positive(values; lower::Float64=0.05, upper::Float64=4.0)
    imputed = median_impute(values)
    scale = positive_median(imputed)
    return clamp.(imputed ./ scale, lower, upper)
end

function attanasio_normalization(
    values;
    variable_name::String,
    log_positive::Bool=false,
    unit_interval::Bool=false,
)
    transformed = Float64[]
    for value in values
        if ismissing(value) || !(value isa Real) || !isfinite(Float64(value))
            continue
        end
        numeric = Float64(value)
        if unit_interval && !(0.0 <= numeric <= 1.0)
            error("$variable_name must be between zero and one.")
        end
        if log_positive
            numeric > 0.0 || continue
            numeric = log(numeric)
        end
        push!(transformed, numeric)
    end
    length(transformed) >= 2 || error("$variable_name has too few usable observations.")
    scale = std(transformed)
    isfinite(scale) && scale > 0.0 || error("$variable_name has no usable variation.")
    return (
        mean=mean(transformed),
        sd=scale,
        log_positive=log_positive,
        variable_name=variable_name,
    )
end

function apply_attanasio_normalization(values, normalization)
    result = ones(Float64, length(values))
    for (index, value) in pairs(values)
        if ismissing(value) || !(value isa Real) || !isfinite(Float64(value))
            continue
        end
        numeric = Float64(value)
        if normalization.log_positive
            numeric > 0.0 || continue
            numeric = log(numeric)
        end
        result[index] = exp((numeric - normalization.mean) / normalization.sd)
    end
    all(isfinite, result) && all(>(0.0), result) ||
        error("$(normalization.variable_name) produced an invalid Attanasio-scale input.")
    return result
end

function rms_scale(values)
    imputed = median_impute(values)
    scale = sqrt(mean(abs2, imputed))
    scale > 0.0 || error("A child-skill measure has zero scale.")
    return imputed ./ scale
end

function observed_moments(observed::Array{Float64, 3})
    result = zeros(18)
    for period in 1:3
        leisure = finite_observations(observed[:, period, 6])
        expenditure = finite_observations(observed[:, period, 5])
        child_hc = finite_observations(observed[:, period, 7])
        length(leisure) >= 2 || error("Period $period has too few leisure observations.")
        length(expenditure) >= 2 || error("Period $period has too few qualifying CEX observations.")
        length(child_hc) >= 2 || error("Period $period has too few child-HC observations.")
        result[period] = mean(leisure)
        result[period + 3] = mean(expenditure)
        result[period + 6] = mean(child_hc)
        result[period + 9] = std(leisure)
        result[period + 12] = std(expenditure)
        result[period + 15] = std(child_hc)
    end
    all(isfinite, result) || error("The empirical moment vector contains a nonfinite value.")
    return result
end

function load_rev3_observed_data(
    filepath::String=nlsy_data_file();
    maximum_households::Union{Nothing, Int}=nothing,
)
    isfile(filepath) || error("NLSY/CEX input file not found at $filepath.")
    selected_columns = [
        "id", "period", PARENT_EDUCATION_COLUMN, "LABOR_INCOME", "HRSWK_PCY",
        HEAD_START_EXPOSURE_COLUMN, "TCURELSC_per_student",
        "PIAT_MATH", "PIAT_READ_REC", "PIAT_READ_COMP", "PPVT",
        CEX_EXPENDITURE_COLUMN, CEX_MATCHED_YEARS_COLUMN, CEX_COVERAGE_COLUMN,
    ]
    raw = CSV.read(filepath, DataFrame; select=selected_columns)
    raw = raw[in.(raw.period, Ref(0:2)), :]

    period_ids = [Set(raw.id[raw.period .== period]) for period in 0:2]
    balanced_ids = sort!(collect(intersect(period_ids...)))
    isempty(balanced_ids) && error("No household IDs are observed in all three periods.")
    if maximum_households !== nothing
        maximum_households > 0 || error("maximum_households must be positive.")
        balanced_ids = balanced_ids[1:min(maximum_households, length(balanced_ids))]
    end

    period_data = DataFrame[]
    for period in 0:2
        table = raw[(raw.period .== period) .& in.(raw.id, Ref(Set(balanced_ids))), :]
        sort!(table, :id)
        nrow(table) == length(balanced_ids) ||
            error("Period $period contains duplicate or missing rows for balanced IDs.")
        table.id == balanced_ids ||
            error("Period $period does not align with the balanced household IDs.")
        push!(period_data, table)
    end

    # Attanasio estimates the production function after pooling periods 0-2,
    # z-scoring each observed covariate, and exponentiating the joint-factor
    # draw. Apply that same exp(z-score) map before the bridge coefficients enter
    # Rev3. A missing source value maps to exp(0)=1, the normalized pooled mean.
    normalization_source = raw[
        in.(raw.id, Ref(Set(balanced_ids))),
        :,
    ]
    attanasio_normalizations = (
        parent_education=attanasio_normalization(
            normalization_source[!, PARENT_EDUCATION_COLUMN];
            variable_name=PARENT_EDUCATION_COLUMN,
        ),
        head_start=attanasio_normalization(
            normalization_source[!, HEAD_START_EXPOSURE_COLUMN];
            variable_name=HEAD_START_EXPOSURE_COLUMN,
            unit_interval=true,
        ),
        school_spending=attanasio_normalization(
            normalization_source.TCURELSC_per_student;
            variable_name="TCURELSC_per_student",
            log_positive=true,
        ),
    )

    n_households = length(balanced_ids)
    observed = fill(NaN, n_households, 3, 7)
    parent_hc = apply_attanasio_normalization(
        period_data[1][!, PARENT_EDUCATION_COLUMN],
        attanasio_normalizations.parent_education,
    )
    observed[:, :, 1] .= reshape(parent_hc, n_households, 1)

    baseline_scales = Dict{String, Float64}()
    score_columns = ["PIAT_MATH", "PIAT_READ_REC", "PIAT_READ_COMP", "PPVT"]
    for column in score_columns
        baseline = median_impute(period_data[1][!, column])
        baseline_scales[column] = sqrt(mean(abs2, baseline))
    end

    minimum_coverage = cex_minimum_coverage()
    qualifying_cex = zeros(Int, 3)
    for period in 1:3
        table = period_data[period]
        income_scale = positive_median(table.LABOR_INCOME)
        observed[:, period, 2] = clamp.(median_impute(table.LABOR_INCOME) ./ income_scale, 0.05, 4.0)
        if period == 1
            observed[:, period, 3] = apply_attanasio_normalization(
                table[!, HEAD_START_EXPOSURE_COLUMN],
                attanasio_normalizations.head_start,
            )
        else
            observed[:, period, 3] = apply_attanasio_normalization(
                table.TCURELSC_per_student,
                attanasio_normalizations.school_spending,
            )
        end
        hours = median_impute(table.HRSWK_PCY)
        observed[:, period, 6] = 1.0 .- clamp.(hours ./ (52.0 * 40.0), 0.0, 1.0)

        cumulative = table[!, CEX_EXPENDITURE_COLUMN]
        matched_years = table[!, CEX_MATCHED_YEARS_COLUMN]
        coverage = table[!, CEX_COVERAGE_COLUMN]
        for row in 1:n_households
            eligible = !ismissing(cumulative[row]) && !ismissing(matched_years[row]) &&
                       !ismissing(coverage[row]) && Float64(matched_years[row]) > 0.0 &&
                       Float64(coverage[row]) >= minimum_coverage
            if eligible
                observed[row, period, 5] =
                    (Float64(cumulative[row]) / Float64(matched_years[row])) / income_scale
                qualifying_cex[period] += 1
            end
        end

        score_components = hcat([
            median_impute(table[!, column]) ./ baseline_scales[column]
            for column in score_columns
        ]...)
        observed[:, period, 7] = vec(mean(score_components, dims=2))
    end

    empirical_moments = observed_moments(observed)
    households = copy(observed)
    households[:, :, 4:7] .= 0.0
    return (
        households=households,
        observed=observed,
        empirical_moments=empirical_moments,
        ids=balanced_ids,
        n_households=n_households,
        qualifying_cex=qualifying_cex,
        cex_minimum_coverage=minimum_coverage,
        attanasio_normalization=attanasio_normalizations,
        filepath=filepath,
    )
end

function validated_covariance(table::DataFrame, component::Int)
    names(table) == vcat("variable", ATTANASIO_MIXTURE_VARIABLES) || error(
        "Mixture covariance $component has unexpected columns or column order.",
    )
    String.(table.variable) == ATTANASIO_MIXTURE_VARIABLES || error(
        "Mixture covariance $component has unexpected row labels or row order.",
    )
    covariance = Matrix{Float64}(table[:, ATTANASIO_MIXTURE_VARIABLES])
    all(isfinite, covariance) || error("Mixture covariance $component is nonfinite.")
    isapprox(covariance, covariance'; rtol=1e-8, atol=1e-10) ||
        error("Mixture covariance $component is not symmetric.")
    all(diag(covariance) .> 0.0) ||
        error("Mixture covariance $component has a nonpositive variance.")
    covariance = Matrix(Symmetric(covariance))
    decomposition = eigen(Symmetric(covariance))
    minimum(decomposition.values) >= -1e-8 ||
        error("Mixture covariance $component is not positive semidefinite.")
    factor = decomposition.vectors * Diagonal(sqrt.(max.(decomposition.values, 0.0)))
    return covariance, factor
end

function load_attanasio_mixture(export_dir::String=attanasio_output_dir())
    isdir(export_dir) || error("Attanasio export directory not found at $export_dir.")
    required_files = vcat(
        [MIXTURE_PROBABILITIES_FILENAME, MIXTURE_MEANS_FILENAME],
        MIXTURE_COVARIANCE_FILENAMES,
    )
    missing_files = filter(file -> !isfile(joinpath(export_dir, file)), required_files)
    isempty(missing_files) || error(
        "Attanasio export is missing: $(join(missing_files, ", ")).",
    )

    probability_table = CSV.read(
        joinpath(export_dir, MIXTURE_PROBABILITIES_FILENAME), DataFrame,
    )
    names(probability_table) == ["component", "probability"] ||
        error("mixture_probabilities.csv must contain component and probability.")
    String.(probability_table.component) == ["component_1", "component_2"] ||
        error("Mixture probabilities must contain components 1 and 2 in order.")
    probabilities = Float64.(probability_table.probability)
    all(isfinite, probabilities) && all(probabilities .>= 0.0) ||
        error("Mixture probabilities must be finite and nonnegative.")
    isapprox(sum(probabilities), 1.0; atol=1e-8) ||
        error("Mixture probabilities must sum to one.")
    probabilities ./= sum(probabilities)

    mean_table = CSV.read(joinpath(export_dir, MIXTURE_MEANS_FILENAME), DataFrame)
    names(mean_table) == vcat("component", ATTANASIO_MIXTURE_VARIABLES) ||
        error("mixture_means.csv has unexpected columns or column order.")
    String.(mean_table.component) == ["component_1", "component_2"] ||
        error("Mixture means must contain components 1 and 2 in order.")
    means = Matrix{Float64}(mean_table[:, ATTANASIO_MIXTURE_VARIABLES])
    all(isfinite, means) || error("Mixture means contain a nonfinite value.")

    covariances = Matrix{Float64}[]
    factors = Matrix{Float64}[]
    for (component, filename) in enumerate(MIXTURE_COVARIANCE_FILENAMES)
        covariance, factor = validated_covariance(
            CSV.read(joinpath(export_dir, filename), DataFrame), component,
        )
        push!(covariances, covariance)
        push!(factors, factor)
    end
    return AttanasioMixtureDistribution(
        probabilities=probabilities,
        means=means,
        covariances=covariances,
        factors=factors,
    )
end

function draw_attanasio_log_factors(
    distribution::AttanasioMixtureDistribution,
    n_households::Int;
    rng::AbstractRNG,
)
    n_households > 0 || error("The synthetic household count must be positive.")
    n_variables = length(ATTANASIO_MIXTURE_VARIABLES)
    draws = Matrix{Float64}(undef, n_households, n_variables)
    components = Vector{Int}(undef, n_households)
    cumulative_probabilities = cumsum(distribution.probabilities)
    for household in 1:n_households
        component = searchsortedfirst(cumulative_probabilities, rand(rng))
        components[household] = component
        draws[household, :] = distribution.means[component, :] +
                              distribution.factors[component] * randn(rng, n_variables)
    end
    all(isfinite, draws) || error("Synthetic Attanasio draws contain a nonfinite value.")
    return draws, components
end

function load_rev3_data(
    export_dir::String=attanasio_output_dir();
    maximum_households::Union{Nothing, Int}=nothing,
    seed::Int=synthetic_household_seed(),
)
    distribution = load_attanasio_mixture(export_dir)
    n_households = maximum_households === nothing ?
                   synthetic_household_count() : maximum_households
    n_households > 1 || error("At least two synthetic households are required.")
    log_draws, components = draw_attanasio_log_factors(
        distribution, n_households; rng=MersenneTwister(seed),
    )

    # Attanasio's drawfactor routine exponentiates every joint-distribution
    # draw. Repeating that transformation here puts all synthetic controls and
    # outcome targets on exactly the scale used by the production estimates.
    level_draws = exp.(log_draws)
    all(isfinite, level_draws) || error(
        "Exponentiating the Attanasio draws produced a nonfinite value.",
    )
    column = Dict(name => index for (index, name) in enumerate(ATTANASIO_MIXTURE_VARIABLES))

    observed = fill(NaN, n_households, 3, 7)
    observed[:, :, 1] .= reshape(level_draws[:, column["peducation"]], n_households, 1)
    leisure_clipped = zeros(Int, 3)
    for period in 1:3
        observed[:, period, 2] = level_draws[:, column["income$period"]]
        observed[:, period, 3] = level_draws[:, column["govinvest$period"]]
        observed[:, period, 5] = level_draws[:, column["pinvest$period"]]

        # tau is the non-labor time share that anchors parental time investment.
        # Sampling its estimated log factor can place a draw outside the physical
        # [0, 1] time endowment, so only those impossible draws are projected to
        # the feasible boundary before constructing the SMM leisure moments.
        raw_leisure = level_draws[:, column["tau$period"]]
        leisure_clipped[period] = count(value -> value > 1.0, raw_leisure)
        observed[:, period, 6] = clamp.(raw_leisure, 0.0, 1.0)
        observed[:, period, 7] = level_draws[:, column["hc$(period + 1)"]]
    end

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
        leisure_clipped=leisure_clipped,
        mixture=distribution,
        seed=seed,
        source=:attanasio_mixture,
        filepath=export_dir,
    )
end
