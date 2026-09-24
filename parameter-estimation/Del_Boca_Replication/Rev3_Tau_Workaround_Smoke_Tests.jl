using Test

include(joinpath(@__DIR__, "Del_Boca_Prelim.jl"))
include(joinpath(@__DIR__, "Julia_DelBoca_SMM.jl"))

@testset "Attanasio-scale covariate transformations" begin
    head_start = [0.0, 0.5, 1.0]
    normalization = attanasio_normalization(
        head_start;
        variable_name=HEAD_START_EXPOSURE_COLUMN,
        unit_interval=true,
    )
    @test normalization.mean ≈ mean(head_start)
    @test normalization.sd ≈ std(head_start)
    @test apply_attanasio_normalization(head_start, normalization) ≈
          exp.((head_start .- mean(head_start)) ./ std(head_start))
    @test apply_attanasio_normalization([missing, 0.5], normalization)[1] == 1.0
    @test_throws ErrorException attanasio_normalization(
        [0.0, 1.1];
        variable_name=HEAD_START_EXPOSURE_COLUMN,
        unit_interval=true,
    )

    spending = [100.0, 200.0, 400.0]
    spending_normalization = attanasio_normalization(
        spending;
        variable_name="TCURELSC_per_student",
        log_positive=true,
    )
    @test apply_attanasio_normalization(spending, spending_normalization) ≈
          exp.((log.(spending) .- mean(log.(spending))) ./ std(log.(spending)))
end

@testset "Rev3 tau-workaround data and methodology contracts" begin
    mixture = load_attanasio_mixture()
    @test length(mixture.probabilities) == 2
    @test sum(mixture.probabilities) ≈ 1.0
    @test size(mixture.means) == (2, length(ATTANASIO_MIXTURE_VARIABLES))
    @test all(size(covariance) == (22, 22) for covariance in mixture.covariances)
    @test all(minimum(eigvals(Symmetric(covariance))) >= -1e-8 for covariance in mixture.covariances)

    data = load_rev3_tau_workaround_data(maximum_households=100; seed=20260725)
    repeated_data = load_rev3_tau_workaround_data(maximum_households=100; seed=20260725)
    @test data.n_households == 100
    @test data.source == :attanasio_mixture_tau_workaround
    @test sum(data.component_counts) == data.n_households
    @test data.proposals > data.n_households
    @test data.rejected_proposals == data.proposals - data.n_households
    @test 0.0 < data.acceptance_rate < 1.0
    @test isequal(data.observed, repeated_data.observed)
    @test data.proposals == repeated_data.proposals
    @test data.empirical_moments == repeated_data.empirical_moments
    @test all(isfinite, data.empirical_moments)
    @test all(isnan, data.observed[:, :, 4])
    @test all((0.0 .< data.observed[:, :, 6]) .& (data.observed[:, :, 6] .<= 1.0))
    @test maximum(data.observed[:, :, 6]) < 1.0
    @test all(isfinite, data.observed[:, :, 1:3])
    @test all(data.observed[:, :, 1:3] .> 0.0)
    @test all(data.observed[:, :, 5] .> 0.0)
    @test all(data.observed[:, :, 7] .> 0.0)

    baseline_technology = test_true_technology()
    set_true_technology!(baseline_technology)
    baseline_parameters = copy(parameters)
    serial = solve_households(copy(data.households), baseline_parameters, 1)
    threaded = solve_households(copy(data.households), baseline_parameters, 0)
    @test serial ≈ threaded rtol=1e-12 atol=1e-12
    @test all(isfinite, moments(serial))

    changed_beliefs = copy(baseline_parameters)
    changed_beliefs[2] = 0.5
    belief_solution = solve_households(copy(data.households), changed_beliefs, 1)
    @test maximum(abs.(belief_solution[:, :, 5:6] .- serial[:, :, 5:6])) > 1e-6

    changed_true_technology = TrueCESTechnology(
        intercept=baseline_technology.intercept,
        parent_education_coefficient=baseline_technology.parent_education_coefficient,
        theta_h=baseline_technology.theta_h,
        theta_p=baseline_technology.theta_p,
        ces_delta=baseline_technology.ces_delta,
        mu=baseline_technology.mu,
        rho=fill(-1.0, 3),
        control_function_coefficient=baseline_technology.control_function_coefficient,
        residual_sd=baseline_technology.residual_sd,
    )
    baseline_policy = solve_parent_policy(baseline_parameters, data.households).policy
    set_true_technology!(changed_true_technology)
    changed_true_policy = solve_parent_policy(baseline_parameters, data.households).policy
    @test baseline_policy.l_policy ≈ changed_true_policy.l_policy rtol=1e-12 atol=1e-12
    @test baseline_policy.e_policy ≈ changed_true_policy.e_policy rtol=1e-12 atol=1e-12
    changed_true_solution = solve_households(copy(data.households), baseline_parameters, 1)
    @test maximum(abs.(changed_true_solution[:, :, 7] .- serial[:, :, 7])) > 1e-6

    set_true_technology!(baseline_technology)
end

@testset "Reduced DX-NES SMM" begin
    data = load_rev3_tau_workaround_data(maximum_households=40; seed=20260725)
    set_true_technology!(test_true_technology())
    result = two_step_smm(
        data.empirical_moments,
        copy(parameters),
        data.households;
        stage1_evaluations=8,
        stage2_evaluations=8,
        weighting_draws=2,
        population_size=4,
        random_seed=20260725,
        return_details=true,
    )
    @test length(result.parameters) == 5
    @test isfinite(result.objective)
    @test all(isfinite, result.stage2_weighting)
end

@testset "Exported Attanasio bridge integration" begin
    data = load_rev3_tau_workaround_data(maximum_households=20; seed=20260725)
    exported_technology = load_and_set_true_technology!()
    @test validate_true_technology(exported_technology) === exported_technology

    solved = solve_households(copy(data.households), copy(parameters), 1)
    simulated_moments = moments(solved)
    @test size(solved) == size(data.households)
    @test all(isfinite, solved)
    @test all(isfinite, simulated_moments)
    @test length(simulated_moments) == length(data.empirical_moments) == NUM_MOMENTS
    @test isfinite(rev3_smm_objective(parameters, data.empirical_moments, data.households))
end
