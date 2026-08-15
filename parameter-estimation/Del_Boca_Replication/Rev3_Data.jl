using CSV
using DataFrames
using Statistics

const DEFAULT_IEHC_NLSY_DATA_FILE = normpath(
    joinpath(
        @__DIR__, "..", "..", "data-preprocessing", "Fake Merged Data",
        "Fake_Merged_Data_with_cex_education_expenditure.csv",
    ),
)
const DEFAULT_IEHC_ATTANASIO_OUTPUT_DIR = normpath(
    joinpath(@__DIR__, "..", "Attanasio Replication", "Our Most Recent Output"),
)
const REV3_BRIDGE_FILENAME = "attanasio_rev3_bridge.csv"
const CEX_EXPENDITURE_COLUMN =
    "CEX Cumulative Household Education Expenditure (Real 2026 Dollars)"
const CEX_MATCHED_YEARS_COLUMN = "CEX Years Successfully Matched"
const CEX_COVERAGE_COLUMN = "CEX Coverage Share"

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

function load_rev3_data(
    filepath::String=nlsy_data_file();
    maximum_households::Union{Nothing, Int}=nothing,
)
    isfile(filepath) || error("NLSY/CEX input file not found at $filepath.")
    selected_columns = [
        "id", "period", "HGC_REV_MOM", "LABOR_INCOME", "HRSWK_PCY",
        "HOW_LONG_CHILD_WAS_IN_HEAD", "TCURELSC_per_student",
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

    n_households = length(balanced_ids)
    observed = fill(NaN, n_households, 3, 7)
    parent_hc = scaled_positive(period_data[1].HGC_REV_MOM; lower=0.25, upper=2.0)
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
        public_input = period == 1 ? table.HOW_LONG_CHILD_WAS_IN_HEAD : table.TCURELSC_per_student
        observed[:, period, 3] = scaled_positive(public_input)
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
        filepath=filepath,
    )
end
