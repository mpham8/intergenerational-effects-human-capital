using Optim

const beta   = 0.95
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


#3 Period Non-linear System Solver

#consumption (11-13)
function c_star(l, w, h)
    return (1 - l) * w * h / gamma_
end


#minimum labor supply for non-negative education investment
function l_min_for_e_nonneg() # TODO make more specific for this problem
    return 1 / (gamma_ + 1)
end

#parental education investment t1 (14-16)
function e_star(l, w, h)
    return (w * h / gamma_) * ((gamma_ + 1) * l - 1)
end

#shortcut for B, C, D
function D(h1, e1, g1, θ1h, θ1e, θ1g, ρ1)
    return θ1h * h1^ρ1 + θ1e * e1^ρ1 + θ1g * g1^ρ1
end

function C(h1, e1, g1, θ1h, θ1e, θ1g, ρ1, A1, θ2h, θ2e, θ2g, ρ2, e2, g2)
    Dval = D(h1, e1, g1, θ1h, θ1e, θ1g, ρ1)
    return θ2h * A1^ρ2 * Dval^(ρ2/ρ1) + θ2e * e2^ρ2 + θ2g * g2^ρ2
end

function B(h1, e1, g1, θ1h, θ1e, θ1g, ρ1, A1, θ2h, θ2e, θ2g, ρ2, e2, g2, A2, θ3h, θ3e, θ3g, ρ3, e3, g3)
    Cval = C(h1, e1, g1, θ1h, θ1e, θ1g, ρ1, A1, θ2h, θ2e, θ2g, ρ2, e2, g2)
    return θ3h * A2^ρ3 * Cval^(ρ3/ρ2) + θ3e * e3^ρ3 + θ3g * g3^ρ3
end


function hc(θh, θe, θg, ρ, h, e, g, A)
  e_safe = max(e, 1e-10)
  return A * (θh * h^ρ + θe * e_safe^ρ + θg * g^ρ)^(1/ρ)
end

# h4^c (human capital at period 4)
function h4c_full(h1, e1, g1, θ1h, θ1e, θ1g, ρ1, A1,
                  θ2h, θ2e, θ2g, ρ2, e2, g2, A2,
                  θ3h, θ3e, θ3g, ρ3, e3, g3, A3)

    h2 = hc(θ1h, θ1e, θ1g, ρ1, h1, e1, g1, A1)
    h3 = hc(θ2h, θ2e, θ2g, ρ2, h1, e2, g2, A2)
    h4 = hc(θ3h, θ3e, θ3g, ρ3, h1, e3, g3, A3)
    return h4
end

#dh4c/de3
function dh4c_de3_t1(h1, e1, g1, θ1h, θ1e, θ1g, ρ1, A1,
                  θ2h, θ2e, θ2g, ρ2, e2, g2, A2,
                  θ3h, θ3e, θ3g, ρ3, e3, g3, A3)
    h4c = h4c_full(h1, e1, g1, θ1h, θ1e, θ1g, ρ1, A1,
                   θ2h, θ2e, θ2g, ρ2, e2, g2, A2,
                   θ3h, θ3e, θ3g, ρ3, e3, g3, A3)
    return (A3^ρ3) * (h4c)^(1-ρ3) * θ3e * e3^(ρ3-1)
end

#dh4c/de2
function dh4c_de2_t1(h1, e1, g1, θ1h, θ1e, θ1g, ρ1, A1,
                  θ2h, θ2e, θ2g, ρ2, e2, g2, A2,
                  θ3h, θ3e, θ3g, ρ3, e3, g3, A3)
    Cval = C(h1, e1, g1, θ1h, θ1e, θ1g, ρ1, A1, θ2h, θ2e, θ2g, ρ2, e2, g2)
    h4c = h4c_full(h1, e1, g1, θ1h, θ1e, θ1g, ρ1, A1,
                   θ2h, θ2e, θ2g, ρ2, e2, g2, A2,
                   θ3h, θ3e, θ3g, ρ3, e3, g3, A3)
    return (A3^ρ3) * (A2^ρ3) * θ3h * θ2e * (h4c)^(1-ρ3) * e2^(ρ2-1) * Cval^((ρ3/ρ2)-1)
end

#dh4c/de1
function dh4c_de1_t1(h1, e1, g1, θ1h, θ1e, θ1g, ρ1, A1,
                  θ2h, θ2e, θ2g, ρ2, e2, g2, A2,
                  θ3h, θ3e, θ3g, ρ3, e3, g3, A3)
    Dval = D(h1, e1, g1, θ1h, θ1e, θ1g, ρ1)
    Cval = C(h1, e1, g1, θ1h, θ1e, θ1g, ρ1, A1, θ2h, θ2e, θ2g, ρ2, e2, g2)
    h4c = h4c_full(h1, e1, g1, θ1h, θ1e, θ1g, ρ1, A1,
                   θ2h, θ2e, θ2g, ρ2, e2, g2, A2,
                   θ3h, θ3e, θ3g, ρ3, e3, g3, A3)

    return (A3^ρ3) * (A2^ρ3) * (A1^ρ2) * θ3h * θ2h * θ1e * (h4c)^(1-ρ3) * e1^(ρ1-1) * Cval^((ρ3/ρ2)-1) * Dval^((ρ2/ρ1)-1)
end



# Partial derivative dh4c/de3
function dh4c_de3_t2(h2, θ2h, θ2e, θ2g, ρ2, e2, g2, A2,
  θ3h, θ3e, θ3g, ρ3, e3, g3, A3)
  h4c = (θ3h * hc(θ2h, θ2e, θ2g, ρ2, h2, e2, g2, A2)^ρ2 + θ3g * g2^ρ2 + θ3e * e2^ρ2 )^(1/ρ2)
return (A3^ρ3) * (h4c)^(1-ρ3) * θ3e * e3^(ρ3-1)
end


# Partial derivative dh4c/de2
function dh4c_de2_t2(h2, θ2h, θ2e, θ2g, ρ2, e2, g2, A2,
  θ3h, θ3e, θ3g, ρ3, e3, g3, A3)
  Cval = C(h1, e1, g1, θ1h, θ1e, θ1g, ρ1, A1, θ2h, θ2e, θ2g, ρ2, e2, g2)
  h4c = (θ3h * hc(θ2h, θ2e, θ2g, ρ2, h2, e2, g2, A2)^ρ2 + θ3g * g2^ρ2 + θ3e * e2^ρ2 )^(1/ρ2)
  return (A3^ρ3) * (A2^ρ3) * θ3h * θ2e * (h4c)^(1-ρ3) * e2^(ρ2-1) * Cval^((ρ3/ρ2)-1)
  end
  

  # Partial derivative dh4c/de3
function dh4c_de3_t3(h3, θ3h, θ3e, θ3g, ρ3, e3, g3, A3)
  h4c = hc(θ3h, θ3e, θ3g, ρ3, h3, e3, g3, A3)
return (A3^ρ3) * (h4c)^(1-ρ3) * θ3e * e3^(ρ3-1)
end




# solves t1 household maximization problem
function nonlinear_system_eqs_t1(l1, l2, l3, params, wages, h1, g1, g2, g3)
    w1, w2, w3 = wages

    θ1h=params[:θ1h]; θ1e=params[:θ1e]; θ1g=params[:θ1g]
    θ2h=params[:θ2h]; θ2e=params[:θ2e]; θ2g=params[:θ2g]
    θ3h=params[:θ3h]; θ3e=params[:θ3e]; θ3g=params[:θ3g]
    ρ1=params[:ρ1]; ρ2=params[:ρ2]; ρ3=params[:ρ3]
    A1=params[:A1]; A2=params[:A2]; A3=params[:A3]
    fh = params[:fh]

    c1 = c_star(l1, w1, h1)
    c2 = c_star(l2, w2, h1)
    c3 = c_star(l3, w3, h1)
    e1 = e_star(l1, w1, h1)
    e2 = e_star(l2, w2, h1)
    e3 = e_star(l3, w3, h1)

    dh4de1 = dh4c_de1_t1(h1, e1, g1, θ1h, θ1e, θ1g, ρ1, A1,
                      θ2h, θ2e, θ2g, ρ2, e2, g2, A2,
                      θ3h, θ3e, θ3g, ρ3, e3, g3, A3)
    dh4de2 = dh4c_de2_t1(h1, e1, g1, θ1h, θ1e, θ1g, ρ1, A1,
                      θ2h, θ2e, θ2g, ρ2, e2, g2, A2,
                      θ3h, θ3e, θ3g, ρ3, e3, g3, A3)
    dh4de3 = dh4c_de3_t1(h1, e1, g1, θ1h, θ1e, θ1g, ρ1, A1,
                      θ2h, θ2e, θ2g, ρ2, e2, g2, A2,
                      θ3h, θ3e, θ3g, ρ3, e3, g3, A3)

    h4c = h4c_full(h1, e1, g1, θ1h, θ1e, θ1g, ρ1, A1,
                   θ2h, θ2e, θ2g, ρ2, e2, g2, A2,
                   θ3h, θ3e, θ3g, ρ3, e3, g3, A3)

    eq1 = fh * (beta^3 / h4c) * dh4de1 - (c1 * (1 - l1)^gamma_)^(-eta) * (1 - l1)^gamma_
    eq2 = fh * (beta^3 / h4c) * dh4de2 - beta * (c2 * (1 - l2)^gamma_)^(-eta) * (1 - l2)^gamma_
    eq3 = fh * (beta^3 / h4c) * dh4de3 - beta^2 * (c3 * (1 - l3)^gamma_)^(-eta) * (1 - l3)^gamma_

    return (eq1, eq2, eq3)
end

function nonlinear_system_eqs_t2(l2, l3, params, wages, h2, g2, g3)
  # params: Dict or NamedTuple with elements required for human capital computation: θ1h...A3, ρ1,ρ2,ρ3, etc.
  # wages: Tuple (w1, w2, w3)
  w1, w2, w3 = wages # need to fix to actual wages
  # theta and rho values loaded from params
  # θ1h=params[:θ1h]; θ1e=params[:θ1e]; θ1g=params[:θ1g]
  θ2h=params[:θ2h]; θ2e=params[:θ2e]; θ2g=params[:θ2g]
  θ3h=params[:θ3h]; θ3e=params[:θ3e]; θ3g=params[:θ3g]
  ρ2=params[:ρ2]; ρ3=params[:ρ3]
  A2=params[:A2]; A3=params[:A3]
  fh = params[:fh]

  # Human capital and education efforts
  c2 = c2_star(l2, w2, h1)
  c3 = c3_star(l3, w3, h1)
  e2 = e2_star(l2, w2, h1)
  e3 = e3_star(l3, w3, h1)

  dh4de2 = dh4c_de2_t2(h2, θ2h, θ2e, θ2g, ρ2, e2, g2, A2,
  θ3h, θ3e, θ3g, ρ3, e3, g3, A3)
  dh4de3 = dh4c_de3_t2(h3, θ3h, θ3e, θ3g, ρ3, e3, g3, A3)

  eq2 = fh * (beta^3 / h4c) * dh4de2 - beta * (c2 * (1 - l2)^gamma_)^(-eta) * (1 - l2)^gamma_
  eq3 = fh * (beta^3 / h4c) * dh4de3 - beta^2 * (c3 * (1 - l3)^gamma_)^(-eta) * (1 - l3)^gamma_

  return (eq2, eq3)
end


function nonlinear_system_eqs_t2(l3, params, wages, h3, g3)
  # params: Dict or NamedTuple with elements required for human capital computation: θ1h...A3, ρ1,ρ2,ρ3, etc.
  # wages: Tuple (w1, w2, w3)
  w1, w2, w3 = wages # need to fix to actual wages
  # theta and rho values loaded from params
  # θ1h=params[:θ1h]; θ1e=params[:θ1e]; θ1g=params[:θ1g]
  θ3h=params[:θ3h]; θ3e=params[:θ3e]; θ3g=params[:θ3g]
  ρ3=params[:ρ3]
  A3=params[:A3]
  fh = params[:fh]

  # Human capital and education efforts
  c3 = c3_star(l3, w3, h1)
  e3 = e3_star(l3, w3, h1)

  # Human capital derivatives
  dh4de3 = dh4c_de3_t3(h3, θ3h, θ3e, θ3g, ρ3, e3, g3, A3)

  # Utility multiplier f(h): Not specified, if needed can be replaced (set to 1 here)
  eq3 = fh * (beta^3 / h4c) * dh4de3 - beta^2 * (c3 * (1 - l3)^gamma_)^(-eta) * (1 - l3)^gamma_

  return (eq3)
end

#test function 
function test_optimal_l_e(params, h1, g1, g2, g3, w1, w2, w3)


    # --- Compute legal lower bounds for (l1, l2, l3) to ensure e_t >= 0 using inverse ---
    lmin = l_min_for_e_nonneg()
    # Force lower bound into [0.01, 0.99] for safety
    lmin = min(max(lmin, 0.01), 0.99)
    lower = [lmin, lmin, lmin]  # All l_t >= 1/(gamma_+1)
    upper = fill(0.99, 3)
    initial_guess = fill(max(0.5, lmin), 3)

    # objective function that minimizes the squared residuals of the system of equations
    function obj(ells)
        # enforce penalty if outside legal (so we never see e_t < 0, even inside box)
        e1 = e_star(ells[1], w1, h1)
        e2 = e_star(ells[2], w2, h1)
        e3 = e_star(ells[3], w3, h1)
        if (e1 < -1e-10) || (e2 < -1e-10) || (e3 < -1e-10)
            return 1e20 + abs(e1 < 0 ? e1 : 0) + abs(e2 < 0 ? e2 : 0) + abs(e3 < 0 ? e3 : 0)
        end
        eqs = nonlinear_system_eqs_t1(ells[1], ells[2], ells[3], params, (w1, w2, w3), h1, g1, g2, g3)
        return sum(abs2, eqs)
    end

    result = optimize(obj, lower, upper, initial_guess, Fminbox())
    #todo: finish this out
    l1, l2, l3 = Optim.minimizer(result)

    e1 = e1_star(l1, w1, h1)
    e2 = e2_star(l2, w2, h1)
    e3 = e3_star(l3, w3, h1)

    println("Optimal l values: ", l1, ", ", l2, ", ", l3)
    println("Optimal e values: ", e1, ", ", e2, ", ", e3)
    c1 = ((1 - l1) * w1 * h1) / gamma_
    c2 = ((1 - l2) * w2 * h1) / gamma_
    c3 = ((1 - l3) * w3 * h1) / gamma_
    c = [c1, c2, c3]
    println("c: ", c)
    h4c = h1 * (1 + params[:θ3h]*l3 + params[:θ3e]*e3 + params[:θ3g]*g3)
    println("h4c: ", h4c)

    return (l1, l2, l3), (e1, e2, e3)
end


#TESTING
params = Dict(
  :θ1h=>0.55, :θ1e=>0.3, :θ1g=>0.15,
  :θ2h=>0.55, :θ2e=>0.3, :θ2g=>0.15,
  :θ3h=>0.55, :θ3e=>0.3, :θ3g=>0.15,
  :ρ1=>-1, :ρ2=>-1, :ρ3=>-1,
  :A1=>1.0, :A2=>1.0, :A3=>1.0,
  :fh=>50
)
h1 = 1.0
g1 = 0.01375/2
g2 = 0.01375*1.25
g3 = 0.01375*1.25
w1, w2, w3 = 1.0, 1.04, 1.08

test_optimal_l_e(params, h1, g1, g2, g3, w1, w2, w3)