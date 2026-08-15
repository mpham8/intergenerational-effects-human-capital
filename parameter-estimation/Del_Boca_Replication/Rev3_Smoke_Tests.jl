using Test

include(joinpath(@__DIR__, "Julia_DelBoca_SMM.jl"))

@testset "Rev3 data and methodology contracts" begin
    data = load_rev3_data(maximum_households=100)
    @test data.n_households == 100
    @test all(data.qualifying_cex .> 0)
    @test all(isfinite, data.empirical_moments)
    @test all(isnan, data.observed[:, :, 4])
    @test all((0.0 .<= data.observed[:, :, 6]) .& (data.observed[:, :, 6] .<= 1.0))

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
    data = load_rev3_data(maximum_households=40)
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

