# Period 3
# Inputs: period 3 human capital, period 3 parental investments, period 3 governmental investments
# Output: period 4 human capital

ensure_tau <- function(alldata, period) {
  tau_name <- paste0("tau", period)
  if (!tau_name %in% names(alldata)) {
    stop(paste("Missing estimated parental-time factor", tau_name))
  }
  return(alldata)
}

ces_log_pair <- function(log_x, log_y, weight, exponent) {
  if (exponent == 0) return(weight * log_x + (1 - weight) * log_y)
  gap <- exponent * (log_x - log_y)
  # Factor out y for small gaps so the numerator retains precision near zero.
  near <- abs(gap) <= 0.5
  log_sum <- numeric(length(gap))
  log_sum[near] <- log1p(weight * expm1(gap[near]))
  a <- log(weight) + gap[!near]
  b <- log1p(-weight)
  top <- pmax(a, b)
  log_sum[!near] <- top + log1p(exp(-abs(a - b)))
  log_y + log_sum / exponent
}

nested_ces_log <- function(h0, e, tau, z, theta_h, theta_p, ces_delta, mu, rho) {
  private_input <- ces_delta * log(e) + (1 - ces_delta) * log(tau)
  private_public <- ces_log_pair(private_input, log(z), theta_p, mu)
  ces_log_pair(log(h0), private_public, theta_h, rho)
}

source(file.path(dir, "Our_Export_Rev3.R"))

# A boundary estimate is not itself a coding error. Preserve economically
# admissible bounds, but refuse nonconvergence and report local identification.
production_fit_diagnostics <- list()
production_start_diagnostics <- list()
production_profile_diagnostics <- list()
production_bounds_diagnostics <- list()
ces_lower <- c(theta_h=.01, theta_p=.01, ces_delta=.01, mu=-5, rho=-5)
ces_upper <- c(theta_h=.99, theta_p=.99, ces_delta=.99, mu=.99, rho=.99)

ces_boundary_parameters <- function(coefficients) {
  p <- intersect(names(ces_lower), names(coefficients))
  side <- ifelse(abs(coefficients[p] - ces_lower[p]) < 1e-5, "lower",
           ifelse(abs(coefficients[p] - ces_upper[p]) < 1e-5, "upper", ""))
  setNames(side[side != ""], p[side != ""])
}

check_production_fit <- function(fit, period, control_function) {
  if (inherits(fit, "try-error") || !inherits(fit, "nls")) stop("Production fit failed in period ", period)
  info <- fit$convInfo
  if (!isTRUE(info$isConv)) stop("Production fit did not converge in period ", period, ": ", info$stopMessage)
  coefficients <- coef(fit)
  at_bound <- ces_boundary_parameters(coefficients)
  gradient <- fit$m$gradient()
  # Scale the columns so units alone do not imply weak local identification.
  column_norms <- sqrt(colSums(gradient^2))
  zero_columns <- any(column_norms == 0)
  singular_values <- svd(sweep(gradient, 2, ifelse(column_norms == 0, 1, column_norms), "/"),
                         nu = 0, nv = 0)$d
  raw_singular_values <- svd(gradient, nu = 0, nv = 0)$d
  raw_condition <- if (min(raw_singular_values) == 0) Inf else max(raw_singular_values) / min(raw_singular_values)
  condition <- if (zero_columns || min(singular_values) <= max(singular_values) * .Machine$double.eps) Inf else max(singular_values) / min(singular_values)
  row <- data.frame(period = period, control_function = control_function,
                    converged = info$isConv, iterations = info$finIter,
                    stop_message = info$stopMessage, residual_ss = deviance(fit),
                    jacobian_condition = raw_condition, scaled_jacobian_condition = condition,
                    boundary_parameters = paste(names(at_bound), collapse = ";"),
                    boundary_sides = paste(at_bound, collapse = ";"))
  production_fit_diagnostics[[length(production_fit_diagnostics) + 1L]] <<- row
  if (length(at_bound)) warning("Period ", period, " production estimates at bounds: ",
                                 paste(paste0(names(at_bound), " (", at_bound, ")"), collapse = ", "),
                                 ". Check identification and sensitivity before structural interpretation.")
  invisible(fit)
}

production_starts <- function(control_function) {
  linear <- c(delta=.3, if (control_function) c(alpha=0), alpha3=.05)
  list(
    historical=c(linear, theta_h=.3, theta_p=.5, ces_delta=.5, mu=-.5, rho=-.5),
    complementary=c(linear, theta_h=.65, theta_p=.3, ces_delta=.65, mu=-2, rho=-2),
    substitutable=c(linear, theta_h=.65, theta_p=.7, ces_delta=.35, mu=.65, rho=.65),
    mixed=c(linear, theta_h=.2, theta_p=.55, ces_delta=.6, mu=.4, rho=-1.25)
  )
}

try_production_fit <- function(formula, data, start, lower=ces_lower, upper=ces_upper,
                               fixed=numeric()) {
  if (length(fixed)) {
    formula[[3L]] <- do.call("substitute", list(formula[[3L]], as.list(fixed)))
    start <- start[!names(start) %in% names(fixed)]
  }
  bound_vector <- function(bounds, default) {
    result <- setNames(rep(default, length(start)), names(start))
    parameters <- intersect(names(start), names(bounds))
    result[parameters] <- bounds[parameters]
    result
  }
  lower <- bound_vector(lower, -Inf)
  upper <- bound_vector(upper, Inf)
  fit <- tryCatch(suppressWarnings(minpack.lm::nlsLM(formula, data=data, start=start,
    lower=lower, upper=upper, control=minpack.lm::nls.lm.control(
      maxiter=1024, maxfev=12000, ftol=1e-10, ptol=1e-10, gtol=1e-10))), error=identity)
  if (inherits(fit, "error")) return(list(fit=NULL, rss=NA_real_, message=conditionMessage(fit)))
  rss <- deviance(fit)
  usable <- isTRUE(fit$convInfo$isConv) && is.finite(rss) && all(is.finite(coef(fit)))
  list(fit=if (usable) fit, rss=if (usable) rss else NA_real_, message=fit$convInfo$stopMessage)
}

best_production_attempt <- function(attempts) {
  rss <- vapply(attempts, function(x) x$rss, 0.0)
  if (all(is.na(rss))) return(NA_integer_)
  which.min(rss)
}

profile_production_fit <- function(formula, data, fit, parameter, grid, fallback_start) {
  fitted_rss <- deviance(fit)
  warm <- coef(fit)
  do.call(rbind, lapply(grid, function(value) {
    attempts <- lapply(list(warm, fallback_start), function(start) {
      try_production_fit(formula, data, start, fixed=setNames(value, parameter))
    })
    best <- best_production_attempt(attempts)
    rss <- if (is.na(best)) NA_real_ else attempts[[best]]$rss
    data.frame(parameter=parameter, fixed_value=value, residual_ss=rss,
               delta_residual_ss=rss-fitted_rss, converged=!is.na(best))
  }))
}

fit_production_ces <- function(formula, data, period, control_function, diagnose=TRUE) {
  starts <- production_starts(control_function)
  attempts <- lapply(starts, function(start) try_production_fit(formula, data, start))
  best <- best_production_attempt(attempts)
  if (is.na(best)) stop("No CES start converged in period ", period, " (control function=",
    control_function, "): ", paste(unique(vapply(attempts, function(x) x$message, "")), collapse="; "))
  fit <- attempts[[best]]$fit
  check_production_fit(fit, period, control_function)
  if (!diagnose) return(fit)
  add_context <- function(row) data.frame(period=period, control_function=control_function, row)
  production_start_diagnostics[[length(production_start_diagnostics)+1L]] <<- add_context(
    data.frame(start=names(starts), selected=seq_along(starts)==best,
               converged=vapply(attempts, function(x) !is.null(x$fit), TRUE),
               residual_ss=vapply(attempts, function(x) x$rss, 0.0),
               stop_message=vapply(attempts, function(x) x$message, "")))

  for (parameter in names(ces_boundary_parameters(coef(fit)))) {
    lo <- ces_lower[[parameter]]
    hi <- ces_upper[[parameter]]
    grid <- sort(unique(c(coef(fit)[[parameter]], lo + (hi-lo)*c(0,.02,.1,.25,.5,.75,.9,.98,1))))
    profile <- profile_production_fit(formula, data, fit, parameter, grid, starts[[1L]])
    if (any(profile$delta_residual_ss < -1e-8 * max(1, deviance(fit)), na.rm=TRUE)) warning(
      "Period ", period, " ", parameter, " profile improved on the selected multistart fit. ",
      "Inspect the objective profile before using the estimate.")
    production_profile_diagnostics[[length(production_profile_diagnostics)+1L]] <<- add_context(profile)

    # Relax only the active end and only for sensitivity; never export this fit.
    lower <- ces_lower
    upper <- ces_upper
    side <- ces_boundary_parameters(coef(fit))[[parameter]]
    if (side == "lower") lower[[parameter]] <- if (parameter %in% c("mu", "rho")) -7 else .001
    if (side == "upper") upper[[parameter]] <- .999
    alternatives <- lapply(list(coef(fit), starts[[1L]]), function(start)
      try_production_fit(formula, data, start, lower=lower, upper=upper))
    choice <- best_production_attempt(alternatives)
    alt <- if (!is.na(choice)) alternatives[[choice]]
    production_bounds_diagnostics[[length(production_bounds_diagnostics)+1L]] <<- add_context(
      data.frame(parameter=parameter, side=side, tested_bound=if (side=="lower") lower[[parameter]] else upper[[parameter]],
                 estimate=unname(coef(fit)[[parameter]]),
                 alternative_estimate=if (is.null(alt)) NA_real_ else unname(coef(alt$fit)[[parameter]]),
                 residual_ss=deviance(fit),
                 alternative_residual_ss=if (is.null(alt)) NA_real_ else alt$rss,
                 converged=!is.null(alt)))
  }
  fit
}

write_ces_fit_diagnostics <- function(output_dir) {
  write_diagnostics <- function(rows, filename, columns=NULL) {
    # Write headers even when no boundary was hit, replacing old diagnostics.
    table <- if (length(rows)) do.call(rbind, rows) else
      as.data.frame(setNames(rep(list(logical(0)), length(columns)), columns))
    write.csv(table, file.path(output_dir, filename), row.names=FALSE)
  }
  context <- c("period", "control_function")
  write_diagnostics(production_fit_diagnostics, "production_fit_diagnostics.csv")
  write_diagnostics(production_start_diagnostics, "production_fit_starts.csv",
                    c(context, "start", "selected", "converged", "residual_ss", "stop_message"))
  write_diagnostics(production_profile_diagnostics, "production_boundary_profiles.csv",
                    c(context, "parameter", "fixed_value", "residual_ss", "delta_residual_ss", "converged"))
  write_diagnostics(production_bounds_diagnostics, "production_bound_sensitivity.csv",
                    c(context, "parameter", "side", "tested_bound", "estimate", "alternative_estimate",
                      "residual_ss", "alternative_residual_ss", "converged"))
}

CES3_nocf <- function(alldata, diagnose=TRUE) {
  alldata <- ensure_tau(alldata, 3)
  out <- log(alldata$hc4)
  cesout <- fit_production_ces(
    out ~ nested_ces_log(hc3, pinvest3, tau3, govinvest3, theta_h, theta_p, ces_delta, mu, rho)
          + delta + alpha3*peducation,
    alldata, 3L, FALSE, diagnose=diagnose
  )
  if (length(summary(cesout)) == 11) {  
    npar <- nrow(summary(cesout)[[10]])
    outer_elast <- 1 / (1 - (summary(cesout)[[10]][npar,1]))
    inner_elast <- 1 / (1 - (summary(cesout)[[10]][npar-1,1]))
    residual_sd <- summary(cesout)[[3]]
    estim <- c(summary(cesout)[[10]][1:npar,1], outer_elast, inner_elast, residual_sd)
    names(estim) <- c("delta", "peducation", "theta_h", "theta_p", "ces_delta", "mu", "rho",
                      "outer elast", "inner elast", "residual sd")
  } else {
    print("nlsLM failed with error:")
    print(cesout)
    estim <- rep(NA, 10)
  }
  return(estim)
}

CES3_cf <- function(alldata, diagnose=TRUE) {
  alldata <- ensure_tau(alldata, 3)
  out <- log(alldata$hc4)
  inst3 <- lm(log(pinvest3) ~ log(hc3) + log(peducation) + log(income3), data=alldata)
  cf <- inst3$residuals
  # Extract readable summary statistics
  inst3_summary <- data.frame(
    coefficients = summary(inst3)$coefficients[,1],
    std_errors = summary(inst3)$coefficients[,2],
    t_values = summary(inst3)$coefficients[,3],
    p_values = summary(inst3)$coefficients[,4],
    r_squared = summary(inst3)$r.squared,
    adj_r_squared = summary(inst3)$adj.r.squared,
    f_statistic = summary(inst3)$fstatistic[1],
    f_pvalue = pf(summary(inst3)$fstatistic[1], 
                  summary(inst3)$fstatistic[2], 
                  summary(inst3)$fstatistic[3], 
                  lower.tail = FALSE),
    row.names = names(summary(inst3)$coefficients[,1])
  )
  cesout <- fit_production_ces(
    out ~ nested_ces_log(hc3, pinvest3, tau3, govinvest3, theta_h, theta_p, ces_delta, mu, rho)
          + delta + alpha*cf + alpha3*peducation,
    alldata, 3L, TRUE, diagnose=diagnose
  )
  if (length(summary(cesout))==11) {  
    npar <- nrow(summary(cesout)[[10]])
    outer_elast <- 1 / (1 - (summary(cesout)[[10]][npar,1]))
    inner_elast <- 1 / (1 - (summary(cesout)[[10]][npar-1,1]))
    residual_sd <- summary(cesout)[[3]]
    estim <- c(summary(cesout)[[10]][1:npar,1], outer_elast, inner_elast, residual_sd)
    names(estim) <- c("delta", "controlfn", "peducation", "theta_h", "theta_p", "ces_delta", "mu", "rho",
                      "outer elast", "inner elast", "residual sd")
    gamma = summary(cesout)[[10]][2,1]
    structural_res = residuals(cesout) + gamma*cf
    reg <- lm(structural_res ~ log(hc3) + log(peducation) + log(income3), data=alldata)
    coef <- as.vector(summary.lm(reg)[[4]][,1])
    se <- as.vector(summary.lm(reg)[[4]][,2])
    vcov <- as.matrix(vcov(reg))
  } else {
    estim <- rep(NA, 11)
    coef <- rep(NA, 4)
  }
  return(list(estim, summary(inst3)[[4]][,1], coef, inst3_summary))
}

# Period 2

CES2_nocf <- function(alldata, diagnose=TRUE) {
  alldata <- ensure_tau(alldata, 2)
  out <- log(alldata$hc3)
  cesout <- fit_production_ces(
    out ~ nested_ces_log(hc2, pinvest2, tau2, govinvest2, theta_h, theta_p, ces_delta, mu, rho)
          + delta + alpha3*peducation,
    alldata, 2L, FALSE, diagnose=diagnose
  )
  if (length(summary(cesout))==11) {  
    npar <- nrow(summary(cesout)[[10]])
    outer_elast <- 1 / (1 - (summary(cesout)[[10]][npar,1]))
    inner_elast <- 1 / (1 - (summary(cesout)[[10]][npar-1,1]))
    residual_sd <- summary(cesout)[[3]]
    estim <- c(summary(cesout)[[10]][1:npar,1], outer_elast, inner_elast, residual_sd)
    names(estim) <- c("delta", "peducation", "theta_h", "theta_p", "ces_delta", "mu", "rho",
                      "outer elast", "inner elast", "residual sd")
  } else {
    estim <- rep(NA, 10)
  }
  return(estim)
}

CES2_cf <- function(alldata, diagnose=TRUE) {
  alldata <- ensure_tau(alldata, 2)
  out <- log(alldata$hc3)
  inst2 <- lm(log(pinvest2) ~ log(hc2) + log(peducation) + log(income2), data=alldata)
  cf <- inst2$residuals
  # Extract readable summary statistics
  inst2_summary <- data.frame(
    coefficients = summary(inst2)$coefficients[,1],
    std_errors = summary(inst2)$coefficients[,2],
    t_values = summary(inst2)$coefficients[,3],
    p_values = summary(inst2)$coefficients[,4],
    r_squared = summary(inst2)$r.squared,
    adj_r_squared = summary(inst2)$adj.r.squared,
    f_statistic = summary(inst2)$fstatistic[1],
    f_pvalue = pf(summary(inst2)$fstatistic[1], 
                  summary(inst2)$fstatistic[2], 
                  summary(inst2)$fstatistic[3], 
                  lower.tail = FALSE),
    row.names = names(summary(inst2)$coefficients[,1])
  )
  cesout <- fit_production_ces(
    out ~ nested_ces_log(hc2, pinvest2, tau2, govinvest2, theta_h, theta_p, ces_delta, mu, rho)
          + delta + alpha*cf + alpha3*peducation,
    alldata, 2L, TRUE, diagnose=diagnose
  )
  if (length(summary(cesout))==11) {  
    npar <- nrow(summary(cesout)[[10]])
    outer_elast <- 1 / (1 - (summary(cesout)[[10]][npar,1]))
    inner_elast <- 1 / (1 - (summary(cesout)[[10]][npar-1,1]))
    residual_sd <- summary(cesout)[[3]]
    estim <- c(summary(cesout)[[10]][1:npar,1], outer_elast, inner_elast, residual_sd)
    names(estim) <- c("delta", "controlfn", "peducation", "theta_h", "theta_p", "ces_delta", "mu", "rho",
                      "outer elast", "inner elast", "residual sd")
    gamma = summary(cesout)[[10]][2,1]
    structural_res = residuals(cesout) + gamma*cf
    reg <- lm(structural_res ~ log(hc2) + log(peducation) + log(income2), data=alldata)
    coef <- as.vector(summary.lm(reg)[[4]][,1])
    se <- as.vector(summary.lm(reg)[[4]][,2])
    vcov <- as.matrix(vcov(reg))
  } else {
    estim <- rep(NA, 11)
    coef <- rep(NA, 4)
  }
  return(list(estim, summary(inst2)[[4]][,1], coef, inst2_summary))
}

# Period 1

CES1_nocf <- function(alldata, diagnose=TRUE) {
  alldata <- ensure_tau(alldata, 1)
  out <- log(alldata$hc2)
  cesout <- fit_production_ces(
    out ~ nested_ces_log(hc1, pinvest1, tau1, govinvest1, theta_h, theta_p, ces_delta, mu, rho)
          + delta + alpha3*peducation,
    alldata, 1L, FALSE, diagnose=diagnose
  )
  if (length(summary(cesout))==11) {  
    npar <- nrow(summary(cesout)[[10]])
    outer_elast <- 1 / (1 - (summary(cesout)[[10]][npar,1]))
    inner_elast <- 1 / (1 - (summary(cesout)[[10]][npar-1,1]))
    residual_sd <- summary(cesout)[[3]]
    estim <- c(summary(cesout)[[10]][1:npar,1], outer_elast, inner_elast, residual_sd)
    names(estim) <- c("delta", "peducation", "theta_h", "theta_p", "ces_delta", "mu", "rho",
                      "outer elast", "inner elast", "residual sd")
  } else {
    estim <- rep(NA, 10)
  }
  return(estim)
}

CES1_cf <- function(alldata, diagnose=TRUE) {
  alldata <- ensure_tau(alldata, 1)
  out <- log(alldata$hc2)
  inst1 <- lm(log(pinvest1) ~ log(peducation) + log(income1), data=alldata)
  cf <- inst1$residuals
  # Extract readable summary statistics
  inst1_summary <- data.frame(
    coefficients = summary(inst1)$coefficients[,1],
    std_errors = summary(inst1)$coefficients[,2],
    t_values = summary(inst1)$coefficients[,3],
    p_values = summary(inst1)$coefficients[,4],
    r_squared = summary(inst1)$r.squared,
    adj_r_squared = summary(inst1)$adj.r.squared,
    f_statistic = summary(inst1)$fstatistic[1],
    f_pvalue = pf(summary(inst1)$fstatistic[1], 
                  summary(inst1)$fstatistic[2], 
                  summary(inst1)$fstatistic[3], 
                  lower.tail = FALSE),
    row.names = names(summary(inst1)$coefficients[,1])
  )
  cesout <- fit_production_ces(
    out ~ nested_ces_log(hc1, pinvest1, tau1, govinvest1, theta_h, theta_p, ces_delta, mu, rho)
          + delta + alpha*cf + alpha3*peducation,
    alldata, 1L, TRUE, diagnose=diagnose
  )
  if (length(summary(cesout))==11) {  
    npar <- nrow(summary(cesout)[[10]])
    outer_elast <- 1 / (1 - (summary(cesout)[[10]][npar,1]))
    inner_elast <- 1 / (1 - (summary(cesout)[[10]][npar-1,1]))
    residual_sd <- summary(cesout)[[3]]
    estim <- c(summary(cesout)[[10]][1:npar,1], outer_elast, inner_elast, residual_sd)
    names(estim) <- c("delta", "controlfn", "peducation", "theta_h", "theta_p", "ces_delta", "mu", "rho",
                      "outer elast", "inner elast", "residual sd")
    gamma = summary(cesout)[[10]][2,1]
    structural_res = residuals(cesout) + gamma*cf
    reg <- lm(structural_res ~ log(peducation) + log(income1), data=alldata)
    coef <- as.vector(summary.lm(reg)[[4]][,1])
    se <- as.vector(summary.lm(reg)[[4]][,2])
    vcov <- as.matrix(vcov(reg))
  } else {
    estim <- rep(NA, 11)
    coef <- rep(NA, 3)
  }
  return(list(estim, summary(inst1)[[4]][,1], coef, inst1_summary))
}

##ESTIMATION##
for (boot in 0:Bootstrap){
  # Estimation on true data
  if (boot==0){
    setwd(dir_output)
    monetary_contract <- NULL
    time_contract <- NULL
    load("trueFM.R")
    if (!identical(time_contract, "activity_index_capacity_v3")) stop("Rerun the activity-index factor estimation; incompatible trueFM.R.")
    if (!identical(monetary_contract, "annual_real_2026_maternal_wage_fixed_income_v4")) {
      stop("trueFM.R uses an incompatible budget. Rerun the factor estimation.")
    }
    data <- drawfactor(mean.mix, cov.mix, prob.mix)
    alldata <- data[[1]]
    # CES period 3, without control functions
    out_ces3_nocf <- CES3_nocf(alldata)
    # CES period 3, with control functions
    out3 <- CES3_cf(alldata)
    out_ces3_cf <- out3[[1]]
    coef_ces3 <- out3[[3]]
    # Investment in period 3
    trueInvest3 <- out3[[2]]
    # CES period 2, without control functions
    out_ces2_nocf <- CES2_nocf(alldata)
    # CES period 2, with control functions
    out2 <- CES2_cf(alldata)
    out_ces2_cf <- out2[[1]]
    coef_ces2 <- out2[[3]]
    # Investment in period 2
    trueInvest2 <- out2[[2]]
    # CES period 1, without control functions
    out_ces1_nocf <- CES1_nocf(alldata)
    # CES period 1, with control functions
    out1 <- CES1_cf(alldata)
    out_ces1_cf <- out1[[1]]
    coef_ces1 <- out1[[3]]
    # Investment in period 1
    trueInvest1 <- out1[[2]]
    # Put estimates of production functions with and without control functions together
    npari <- length(out_ces3_nocf)
    nocf3_vector <- c(out_ces3_nocf[1], rep(0,1), out_ces3_nocf[2:npari])
    out_ces3_nocf <- matrix(nocf3_vector, ncol=1)
    out_ces3_cf <- matrix(out_ces3_cf, ncol=1)
    trueCESperiod3 <- cbind(out_ces3_nocf, out_ces3_cf)
    nocf2_vector <- c(out_ces2_nocf[1], rep(0,1), out_ces2_nocf[2:npari])
    out_ces2_nocf <- matrix(nocf2_vector, ncol=1)
    out_ces2_cf <- matrix(out_ces2_cf, ncol=1)
    trueCESperiod2 <- cbind(out_ces2_nocf, out_ces2_cf)
    nocf1_vector <- c(out_ces1_nocf[1], rep(0,1), out_ces1_nocf[2:npari])
    out_ces1_nocf <- matrix(nocf1_vector, ncol=1)
    out_ces1_cf <- matrix(out_ces1_cf, ncol=1)
    trueCESperiod1 <- cbind(out_ces1_nocf, out_ces1_cf)
    rownames(trueCESperiod3) <- c("delta", "control function", "peducation", "theta_h", "theta_p",
                                  "ces_delta", "mu", "rho", "outer elast", "inner elast", "residual sd")
    colnames(trueCESperiod3) <- c("Human Capital without Control Function", "Human Capital with Control Function")
    rownames(trueCESperiod2) <- c("delta", "control function", "peducation", "theta_h", "theta_p",
                                  "ces_delta", "mu", "rho", "outer elast", "inner elast", "residual sd")
    colnames(trueCESperiod2) <- c("Human Capital without Control Function", "Human Capital with Control Function")
    rownames(trueCESperiod1) <- c("delta", "control function", "peducation", "theta_h", "theta_p",
                                  "ces_delta", "mu", "rho", "outer elast", "inner elast", "residual sd")
    colnames(trueCESperiod1) <- c("Human Capital without Control Function", "Human Capital with Control Function")
    # Save output in the proper directory 
    setwd(dir_output)
    save(trueCESperiod3, file="trueCESperiod3.R")
    save(trueCESperiod2, file="trueCESperiod2.R")
    save(trueCESperiod1, file="trueCESperiod1.R")
    save(trueInvest3, file="trueInvest3.R")
    save(trueInvest2, file="trueInvest2.R")
    save(trueInvest1, file="trueInvest1.R")
    save(coef_ces3, file="coef_ces3.R")
    save(coef_ces2, file="coef_ces2.R")
    save(coef_ces1, file="coef_ces1.R")
    write_ces_fit_diagnostics(dir_output)
    export_rev3_bundle(
      dir_output, list(trueCESperiod1, trueCESperiod2, trueCESperiod3),
      prob.mix, mean.mix, cov.mix, namef, data_file, monetary_contract,
      time_contract, nonlabor_means, calibration_sample_size
    )
  }
  # Estimation on bootstrap data
  else if (boot==1){
    setwd(dir_output)
    monetary_contract <- NULL
    time_contract <- NULL
    load("allbootFM.R")
    if (!identical(time_contract, "activity_index_capacity_v3")) stop("Rerun the activity-index bootstrap; incompatible allbootFM.R.")
    if (!identical(monetary_contract, "annual_real_2026_maternal_wage_fixed_income_v4")) {
      stop("allbootFM.R uses an incompatible budget. Rerun the factor bootstrap.")
    }
    # Define a function that runs the estimation on each bootstrap sample
    bootprod <- function(b){  
      prob.boot <- bootFM[[b]]$prob
      mean.boot <- bootFM[[b]]$mean
      cov.boot <- rbind(make.positive.definite(bootFM[[b]]$cov[[1]]), make.positive.definite(bootFM[[b]]$cov[[2]]))
      alldatab <- drawfactor(mean.boot, cov.boot, prob.boot)[[1]]
      print(summary(alldatab[, c("hc3", "govinvest3", "pinvest3", "peducation")]))
      # CES period 3, without control functions
      out_ces3_nocf <- CES3_nocf(alldatab, diagnose=FALSE)
      # CES period 3, with control functions
      out3 <- CES3_cf(alldatab, diagnose=FALSE)
      out_ces3_cf <- out3[[1]]
      coef_ces3 <- out3[[3]]
      # Investment in period 3
      outInvest3 <- out3[[2]]
      # CES period 2, without control functions
      out_ces2_nocf <- CES2_nocf(alldatab, diagnose=FALSE)
      # CES period 2, with control functions
      out2 <- CES2_cf(alldatab, diagnose=FALSE)
      out_ces2_cf <- out2[[1]]
      coef_ces2 <- out2[[3]]
      # Investment in period 2
      outInvest2 <- out2[[2]]
      # CES period 1, without control functions
      out_ces1_nocf <- CES1_nocf(alldatab, diagnose=FALSE)
      # CES period 1, with control functions
      out1 <- CES1_cf(alldatab, diagnose=FALSE)
      out_ces1_cf <- out1[[1]]
      coef_ces1 <- out1[[3]]
      # Investment in period 1
      outInvest1 <- out1[[2]]
      estim <- list(out_ces3_nocf, out_ces3_cf, 
                    out_ces2_nocf, out_ces2_cf, 
                    out_ces1_nocf, out_ces1_cf,
                    outInvest3, outInvest2, outInvest1,
                    coef_ces3, coef_ces2, coef_ces1)
      return(estim)
    }
    # Create array to save estimates from each bootstrap
    bootCES3 <- array(0, dim=c(11, 2, bsample))
    bootCES2 <- array(0, dim=c(11, 2, bsample))
    bootCES1 <- array(0, dim=c(11, 2, bsample))
    bootInvest3 <- array(0, dim=c(4, 1, bsample))
    bootInvest2 <- array(0, dim=c(4, 1, bsample))
    bootInvest1 <- array(0, dim=c(3, 1, bsample))
    bootcoef_ces3 <- array(0, dim=c(4, bsample))
    bootcoef_ces2 <- array(0, dim=c(4, bsample))
    bootcoef_ces1 <- array(0, dim=c(3, bsample))
    npar <- 11
    npar1 <- npar - 1
    # Run the estimation on each bootstrap sample 
    for (b in 1:bsample) {
      print(b)      
      estim <- bootprod(b)
      nocf_vector <- c(estim[[1]][1], rep(0,1), estim[[1]][2:npar1])
      bootCES3[,,b] <- cbind(nocf_vector, estim[[2]][1:npar])
      nocf_vector <- c(estim[[3]][1], rep(0,1), estim[[3]][2:npar1])
      bootCES2[,,b] <- cbind(nocf_vector, estim[[4]][1:npar])
      nocf_vector <- c(estim[[5]][1], rep(0,1), estim[[5]][2:npar1])
      bootCES1[,,b] <- cbind(nocf_vector, estim[[6]][1:npar])
      bootInvest3[,,b] <- matrix(estim[[7]], nrow=4, ncol=1)
      bootInvest2[,,b] <- matrix(estim[[8]], nrow=4, ncol=1)
      bootInvest1[,,b] <- matrix(estim[[9]], nrow=3, ncol=1)
      bootcoef_ces3[,b] <- estim[[10]]
      bootcoef_ces2[,b] <- estim[[11]]
      bootcoef_ces1[,b] <- estim[[12]]
    }
    # Save output
    setwd(dir_output)
    save(bootCES3, file="bootCES3.R")
    save(bootCES2, file="bootCES2.R")
    save(bootCES1, file="bootCES1.R")
    save(bootInvest3, file="bootInvest3.R")
    save(bootInvest2, file="bootInvest2.R")
    save(bootInvest1, file="bootInvest1.R")
    save(bootcoef_ces3, file="bootcoef_ces3.R")
    save(bootcoef_ces2, file="bootcoef_ces2.R")
    save(bootcoef_ces1, file="bootcoef_ces1.R")
  }
}

# MAKE TABLES OF RESULTS WITH BOOTSTRAPPED STANDARD ERRORS

if (Bootstrap == 1L) {
# Estimates of the production functions:
npar <- 10

cestable_3 <- matrix(0, (npar*3), 2)
cestable_2 <- matrix(0, (npar*3), 2)
cestable_1 <- matrix(0, (npar*3), 2)

# Estimates of the production functions (Period 3):
for (i in 1:npar) { 
  cestable_3[(i*3-2), ] <- round(trueCESperiod3[i,1:2],3)       # Point estimates                                      
  for (l in 1:2) {
    cestable_3[(i*3-1), l] <- round(sd(bootCES3[i,l,], na.rm=TRUE), 3)   # Standard errors 
    cestable_3[(i*3), l] <- paste("[", round(quantile(bootCES3[i,l,], 0.05, na.rm=TRUE), 3), ",", 
                                  round(quantile(bootCES3[i,l,], 0.95, na.rm=TRUE), 3), "]", sep="") 
  } 
}
colnames(cestable_3) <- c("Human Capital without Control Function", "Human Capital with Control Function")
rownames(cestable_3) <- c("delta","","",
                          "control function","","",
                          "peducation","","",
                          "theta_h","","",
                          "theta_p","","",
                          "ces_delta","","",
                          "mu","","",
                          "rho","","",
                          "outer elasticity of substitution","","",
                          "inner elasticity of substitution","","")

# Estimates of the investment functions (Period 3)
investtable_3 <- matrix(0, (4*3), 1)
for (i in 1:4) {
  investtable_3[(i*3-2), 1] <- round(trueInvest3[i],3)
  investtable_3[(i*3-1), 1] <- round(sd(bootInvest3[i,1,], na.rm=TRUE),3)
  investtable_3[(i*3), 1] <- paste("[", round(quantile(bootInvest3[i,1,], 0.05, na.rm=TRUE),3), ",", 
                                   round(quantile(bootInvest3[i,1,], 0.95, na.rm=TRUE),3), "]", sep="")  
}
colnames(investtable_3) <- "Period 3"
rownames(investtable_3) <- c("Intercept","","",
                             "Human Capital 3","","",
                             "Highest Grade Completed Mother","","",
                             "Income 3","","")

# Estimates of the production functions (Period 2):
for (i in 1:npar) { 
  cestable_2[(i*3-2), ] <- round(trueCESperiod2[i,1:2],3)       # Point estimates                                      
  for (l in 1:2) {
    cestable_2[(i*3-1), l] <- round(sd(bootCES2[i,l,], na.rm=TRUE), 3)   # Standard errors 
    cestable_2[(i*3), l] <- paste("[", round(quantile(bootCES2[i,l,], 0.05, na.rm=TRUE), 3), ",", 
                                  round(quantile(bootCES2[i,l,], 0.95, na.rm=TRUE), 3), "]", sep="") 
  } 
}
colnames(cestable_2) <- c("Human Capital without Control Function", "Human Capital with Control Function")
rownames(cestable_2) <- c("delta","","",
                          "control function","","",
                          "peducation","","",
                          "theta_h","","",
                          "theta_p","","",
                          "ces_delta","","",
                          "mu","","",
                          "rho","","",
                          "outer elasticity of substitution","","",
                          "inner elasticity of substitution","","")

# Estimates of the investment functions (Period 2)
investtable_2 <- matrix(0, (4*3), 1)
for (i in 1:4) {
  investtable_2[(i*3-2), 1] <- round(trueInvest2[i],3)
  investtable_2[(i*3-1), 1] <- round(sd(bootInvest2[i,1,], na.rm=TRUE),3)
  investtable_2[(i*3), 1] <- paste("[", round(quantile(bootInvest2[i,1,], 0.05, na.rm=TRUE),3), ",", 
                                   round(quantile(bootInvest2[i,1,], 0.95, na.rm=TRUE),3), "]", sep="")  
}
colnames(investtable_2) <- "Period 2"
rownames(investtable_2) <- c("Intercept","","",
                             "Human Capital 2","","",
                             "Highest Grade Completed Mother","","",
                             "Income 2","","")

# Estimates of the production functions (Period 1):
for (i in 1:npar) { 
  cestable_1[(i*3-2), ] <- round(trueCESperiod1[i,1:2],3)       # Point estimates                                      
  for (l in 1:2) {
    cestable_1[(i*3-1), l] <- round(sd(bootCES1[i,l,], na.rm=TRUE), 3)   # Standard errors 
    cestable_1[(i*3), l] <- paste("[", round(quantile(bootCES1[i,l,], 0.05, na.rm=TRUE), 3), ",", 
                                  round(quantile(bootCES1[i,l,], 0.95, na.rm=TRUE), 3), "]", sep="") 
  } 
}
colnames(cestable_1) <- c("Human Capital without Control Function", "Human Capital with Control Function")
rownames(cestable_1) <- c("delta","","",
                          "control function","","",
                          "peducation","","",
                          "theta_h","","",
                          "theta_p","","",
                          "ces_delta","","",
                          "mu","","",
                          "rho","","",
                          "outer elasticity of substitution","","",
                          "inner elasticity of substitution","","")

# Estimates of the investment functions (Period 1)
investtable_1 <- matrix(0, (3*3), 1)
for (i in 1:3) {
  investtable_1[(i*3-2), 1] <- round(trueInvest1[i],3)
  investtable_1[(i*3-1), 1] <- round(sd(bootInvest1[i,1,], na.rm=TRUE),3)
  investtable_1[(i*3), 1] <- paste("[", round(quantile(bootInvest1[i,1,], 0.05, na.rm=TRUE),3), ",", 
                                   round(quantile(bootInvest1[i,1,], 0.95, na.rm=TRUE),3), "]", sep="")  
}
colnames(investtable_1) <- "Period 1"
rownames(investtable_1) <- c("Intercept","","",
                             "Highest Grade Completed Mother","","",
                             "Income 1","","")

# Save tables:
write.csv(cestable_3, file="cestable_3.csv")
write.csv(cestable_2, file="cestable_2.csv")
write.csv(cestable_1, file="cestable_1.csv")
write.csv(investtable_3, file="investtable3.csv")
write.csv(investtable_2, file="investtable2.csv")
write.csv(investtable_1, file="investtable1.csv")

} else {
  # Publish point estimates only; never read stale bootstrap artifacts.
  for (period in 1:3) {
    write.csv(get(paste0("trueCESperiod", period)), paste0("cestable_", period, ".csv"))
    write.csv(get(paste0("trueInvest", period)), paste0("investtable", period, ".csv"))
  }
}

# Save summary of first regression
inst3_summary <- out3[[4]]
write.csv(inst3_summary, "inst3_regression_summary.csv", row.names = TRUE)
inst2_summary <- out2[[4]]
write.csv(inst2_summary, "inst2_regression_summary.csv", row.names = TRUE)  
inst1_summary <- out1[[4]]
write.csv(inst1_summary, "inst1_regression_summary.csv", row.names = TRUE)
