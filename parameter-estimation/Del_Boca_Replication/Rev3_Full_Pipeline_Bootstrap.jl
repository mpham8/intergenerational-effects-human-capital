if !isdefined(@__MODULE__, :two_step_smm)
    include(joinpath(@__DIR__, "Julia_DelBoca_SMM.jl"))
end

const MIN_INTERVAL_REPLICATIONS = 399

function read_bootstrap_int(key, default)
    value = tryparse(Int, get(ENV, key, string(default)))
    value === nothing && error("$key must be an integer.")
    return value
end

function run_bootstrap_attanasio(master, rscript, input, output, cluster, strata, r_seed, sample_seed)
    mkpath(output)
    cmd = addenv(`$rscript $master`,
        "IEHC_NLSY_DATA_FILE" => input,
        "IEHC_ATTANASIO_SCRIPT_DIR" => dirname(master),
        "IEHC_ATTANASIO_OUTPUT_DIR" => output,
        "IEHC_DRY_RUN_IDS" => "0",
        "IEHC_RUN_BOOTSTRAP" => "0",
        "IEHC_R_SEED" => string(r_seed),
        "IEHC_BOOTSTRAP_CLUSTER_COLUMN" => cluster,
        "IEHC_BOOTSTRAP_STRATA_COLUMN" => strata,
        "IEHC_BOOTSTRAP_SAMPLE_SEED" => ismissing(sample_seed) ? "" : string(sample_seed),
    )
    logfile = joinpath(output, "attanasio.log")
    open(logfile, "w") do io
        success(pipeline(cmd; stdout=io, stderr=io)) ||
            error("Attanasio failed. See $logfile.")
    end
end

function fit_bootstrap_smm(output; n_households, simulation_seed, evaluations, population, weighting_draws)
    load_and_set_true_technology!(joinpath(output, REV3_BRIDGE_FILENAME))
    data = load_rev3_data(output; maximum_households=n_households, seed=simulation_seed)
    result = two_step_smm(data.empirical_moments, copy(parameters), data.households;
        stage1_evaluations=evaluations, stage2_evaluations=evaluations,
        population_size=population, weighting_draws=weighting_draws,
        random_seed=simulation_seed, return_details=true)
    for stage in (result.stage1, result.stage2)
        all(isfinite, stage.parameters) && isfinite(stage.objective) && stage.objective < 1e12 ||
            error("DX-NES did not return a usable SMM estimate.")
    end
    return result, data
end

function smm_boundary(parameters)
    span = DXNES_UPPER_BOUNDS - DXNES_LOWER_BOUNDS
    distance = min.(parameters - DXNES_LOWER_BOUNDS, DXNES_UPPER_BOUNDS - parameters)
    return any(distance .<= 1e-5 .* span)
end

function local_moment_singular_values(parameters, data)
    # A local numerical rank check, not a test for strong identification.
    span = DXNES_UPPER_BOUNDS - DXNES_LOWER_BOUNDS
    scale = max.(abs.(data.empirical_moments), 1e-8)
    jacobian = zeros(NUM_MOMENTS, length(parameters))
    for j in eachindex(parameters)
        lo, hi = copy(parameters), copy(parameters)
        lo[j] = max(DXNES_LOWER_BOUNDS[j], lo[j] - 1e-3 * span[j])
        hi[j] = min(DXNES_UPPER_BOUNDS[j], hi[j] + 1e-3 * span[j])
        difference = simulate_moments(hi, data.households) - simulate_moments(lo, data.households)
        jacobian[:,j] = difference ./ scale .* (span[j] / (hi[j] - lo[j]))
    end
    return svdvals(jacobian)
end

function bootstrap_summary(estimates, requested, report_intervals, singular_values)
    reference = only(eachrow(filter(:replication => ==(0), estimates)))
    replications = filter(:replication => >(0), estimates)
    successful = nrow(replications)
    reasons = String[]
    report_intervals || push!(reasons, "interval reporting not requested")
    requested >= MIN_INTERVAL_REPLICATIONS || push!(reasons, "too few replications for reporting")
    successful == requested || push!(reasons, "one or more scheduled fits failed")
    any(estimates.attanasio_boundary) && push!(reasons, "Attanasio boundary fit")
    any(estimates.smm_boundary) && push!(reasons, "Del Boca boundary fit")
    if length(singular_values) != length(PARAMETER_NAMES) || !all(isfinite, singular_values) ||
       last(singular_values) <= first(singular_values) * 1e-6
        push!(reasons, "local moment rank not verified")
    end
    observed_spread = [successful >= 2 ? std(replications[!, p]) : missing for p in PARAMETER_NAMES]
    any(s -> ismissing(s) || s <= 0, observed_spread) && push!(reasons, "insufficient variation in replicate estimates")
    allowed = isempty(reasons)
    return DataFrame(
        parameter=PARAMETER_NAMES,
        estimate=[reference[p] for p in PARAMETER_NAMES],
        replicate_sd_descriptive=observed_spread,
        conditional_percentile_lower=[allowed ? quantile(replications[!,p], .025) : missing for p in PARAMETER_NAMES],
        conditional_percentile_upper=[allowed ? quantile(replications[!,p], .975) : missing for p in PARAMETER_NAMES],
        successful_replications=successful,
        requested_replications=requested,
        interval_status=allowed ? "conditional percentile interval; coverage not validated" : join(reasons, "; "),
    )
end

function full_pipeline_bootstrap(; input=nlsy_data_file(),
    output_root=get(ENV, "IEHC_FULL_BOOTSTRAP_OUTPUT_DIR", ""),
    cluster=get(ENV, "IEHC_BOOTSTRAP_CLUSTER_COLUMN", ""),
    strata=get(ENV, "IEHC_BOOTSTRAP_STRATA_COLUMN", ""),
    replications=read_bootstrap_int("IEHC_FULL_BOOTSTRAP_REPS", 0),
    sample_seed=read_bootstrap_int("IEHC_BOOTSTRAP_BASE_SEED", 20261116),
    r_seed=read_bootstrap_int("IEHC_R_SEED", 20261115),
    n_households=read_bootstrap_int("IEHC_SUBSTANTIVE_HOUSEHOLDS", 10000),
    simulation_seed=read_bootstrap_int("IEHC_SUBSTANTIVE_SEED", 20260728),
    evaluations=read_bootstrap_int("IEHC_SUBSTANTIVE_STAGE_EVALUATIONS", DXNES_MAX_EVALUATIONS),
    population=read_bootstrap_int("IEHC_SUBSTANTIVE_POPULATION", DXNES_POPULATION_SIZE),
    weighting_draws=read_bootstrap_int("IEHC_SUBSTANTIVE_WEIGHTING_DRAWS", SMM_WEIGHTING_DRAWS),
    report_intervals=get(ENV, "IEHC_BOOTSTRAP_REPORT_INTERVALS", "0") == "1",
    rscript=get(ENV, "IEHC_RSCRIPT", "Rscript"),
)
    isempty(cluster) && error("Set IEHC_BOOTSTRAP_CLUSTER_COLUMN to the appropriate family or sampling-cluster ID.")
    isempty(output_root) && error("Set IEHC_FULL_BOOTSTRAP_OUTPUT_DIR to an approved output location.")
    simulation_seed >= 0 && simulation_seed < typemax(Int) ||
        error("The simulation seed must be nonnegative and allow an increment for stage two.")
    2 <= replications < typemax(Int32) || error("Set IEHC_FULL_BOOTSTRAP_REPS to at least two.")
    0 <= sample_seed <= typemax(Int32)-replications && 0 <= r_seed <= typemax(Int32) ||
        error("R and cluster-resampling seeds must fit R's nonnegative integer range.")
    n_households >= 2 && evaluations >= 1 && population >= 2 && weighting_draws >= 2 ||
        error("Invalid SMM household count, evaluations, population, or weighting draws.")
    parent_policy_mode() == :household || error("Full-pipeline uncertainty uses household-specific policies.")
    get(ENV, "IEHC_INVOLVEMENT_TEST_MODE", "0") == "0" && !haskey(ENV, "IEHC_INVOLVEMENT_SCALE") ||
        error("Full-pipeline uncertainty requires reestimating kappa; remove test mode and scale overrides.")
    input = abspath(input)
    isfile(input) || error("Input file does not exist: $input")
    input_hash = bytes2hex(open(sha256, input))
    master = normpath(joinpath(@__DIR__, "..", "Attanasio Replication", "Master File.r"))
    output = mktempdir(mkpath(abspath(output_root)); prefix="iehc-full-bootstrap-", cleanup=false)
    println("Full-pipeline bootstrap output: $output")
    CSV.write(joinpath(output, "bootstrap_configuration.csv"), DataFrame(
        cluster=cluster, strata=strata, requested=replications, sample_base_seed=sample_seed,
        r_seed=r_seed, simulation_seed=simulation_seed, households=n_households,
        evaluations_per_stage=evaluations, population=population, weighting_draws=weighting_draws,
        child_grid_points=PARENT_POLICY_GRID_POINTS, parent_grid_points=PARENT_POLICY_PARENT_H_GRID_POINTS,
        minimum_cex_coverage=cex_minimum_coverage(), input_sha256=input_hash,
        minimum_reliable_wage_hours=get(ENV, "IEHC_MIN_RELIABLE_WAGE_HOURS", "208"),
        budget_contract=ATTANASIO_BUDGET_CONTRACT,
        report_intervals=report_intervals, scope="Conditional on supplied CEX imputations and fixed simulation draws",
    ))
    estimates = DataFrame(replication=Int[], kappa=Float64[], stage1_loss=Float64[], stage2_loss=Float64[],
                          attanasio_boundary=Bool[], smm_boundary=Bool[])
    for p in PARAMETER_NAMES
        estimates[!, p] = Float64[]
    end
    status = DataFrame(replication=Int[], sample_seed=Union{Missing,Int}[], stage=String[], message=String[])
    singular_values = Float64[]
    for replication in 0:replications
        dir = joinpath(output, replication == 0 ? "reference" : "replication_$replication")
        seed = replication == 0 ? missing : sample_seed + replication
        stage = "Attanasio"
        try
            run_bootstrap_attanasio(master, rscript, input, dir, cluster, strata, r_seed, seed)
            manifest = validate_attanasio_bundle(dir)
            all(==(input_hash), manifest.input_sha256) || error("Input changed during estimation.")
            design = only(eachrow(CSV.read(joinpath(dir, "bootstrap_design.csv"), DataFrame; types=String)))
            coalesce(design.sample_seed, "") == (ismissing(seed) ? "" : string(seed)) &&
                coalesce(design.cluster_column, "") == cluster &&
                coalesce(design.strata_column, "") == strata ||
                error("Attanasio resampling design/seed does not match the scheduled draw.")
            diagnostics = CSV.read(joinpath(dir, "production_fit_diagnostics.csv"), DataFrame)
            technology_fits = filter(:control_function => identity, diagnostics)
            sort(technology_fits.period) == [1,2,3] && all(technology_fits.converged) ||
                error("Incomplete Attanasio control-function diagnostics.")
            attanasio_boundary = any(s -> !ismissing(s) && !isempty(s), technology_fits.boundary_parameters)

            stage = "Del Boca SMM"
            result, data = fit_bootstrap_smm(dir; n_households, simulation_seed, evaluations,
                                            population, weighting_draws)
            boundary = smm_boundary(result.parameters)
            if replication == 0 && report_intervals && replications >= MIN_INTERVAL_REPLICATIONS &&
               !attanasio_boundary && !boundary
                singular_values = local_moment_singular_values(result.parameters, data)
                CSV.write(joinpath(output, "reference_moment_singular_values.csv"),
                          DataFrame(singular_value=singular_values))
            end
            push!(estimates, (replication, data.calibration.kappa, result.stage1.objective,
                             result.stage2.objective, attanasio_boundary, boundary, result.parameters...))
            push!(status, (replication, seed, "complete", ""))
        catch err
            err isa InterruptException && rethrow()
            push!(status, (replication, seed, stage, sprint(showerror, err)))
            @warn "Bootstrap fit failed" replication stage output exception=err
        end
        CSV.write(joinpath(output, "bootstrap_run_status.csv"), status)
        CSV.write(joinpath(output, "bootstrap_estimates.csv"), estimates)
        replication == 0 && isempty(estimates) &&
            error("Reference fit did not complete; no resamples run. See $output.")
    end
    summary = bootstrap_summary(estimates, replications, report_intervals, singular_values)
    CSV.write(joinpath(output, "bootstrap_summary.csv"), summary)
    println("Completed $(nrow(estimates)-1) of $replications resamples. Summary: $output")
    all(==("complete"), status.stage) || error("Some resamples failed; no intervals were reported.")
    return output
end

if abspath(PROGRAM_FILE) == @__FILE__
    full_pipeline_bootstrap()
end
