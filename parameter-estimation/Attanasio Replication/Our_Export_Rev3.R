# Publish the technology and the exact fitted joint distribution as one handoff.
export_rev3_bridge <- function(output_dir, period_estimates) {
  extract_period <- function(period, estimate_matrix) {
    if (is.null(dim(estimate_matrix)) || ncol(estimate_matrix) < 2) {
      stop(paste("Period", period, "does not contain control-function estimates."))
    }
    estimate <- estimate_matrix[, 2]
    names(estimate) <- rownames(estimate_matrix)
    required <- c(
      "delta", "control function", "peducation", "theta_h", "theta_p",
      "ces_delta", "mu", "rho", "residual sd"
    )
    if (!all(required %in% names(estimate))) {
      stop(paste("Period", period, "is missing required nested-CES estimates."))
    }
    values <- estimate[required]
    if (any(!is.finite(values))) {
      stop(paste("Period", period, "contains nonfinite nested-CES estimates."))
    }
    if (values[["theta_h"]] <= 0 || values[["theta_h"]] >= 1 ||
        values[["theta_p"]] <= 0 || values[["theta_p"]] >= 1 ||
        values[["ces_delta"]] <= 0 || values[["ces_delta"]] >= 1 ||
        values[["mu"]] < -5 || values[["mu"]] > 1 ||
        values[["rho"]] < -5 || values[["rho"]] > 1 ||
        values[["residual sd"]] <= 0) {
      stop(paste("Period", period, "contains an out-of-domain nested-CES estimate."))
    }
    data.frame(
      period = period,
      intercept = unname(values[["delta"]]),
      parent_education_coefficient = unname(values[["peducation"]]),
      theta_h = unname(values[["theta_h"]]),
      theta_p = unname(values[["theta_p"]]),
      ces_delta = unname(values[["ces_delta"]]),
      mu = unname(values[["mu"]]),
      rho = unname(values[["rho"]]),
      control_function_coefficient = unname(values[["control function"]]),
      residual_sd = unname(values[["residual sd"]])
    )
  }

  bridge <- do.call(
    rbind,
    lapply(seq_along(period_estimates), function(period) {
      extract_period(period, period_estimates[[period]])
    })
  )
  bridge_file <- file.path(output_dir, "attanasio_rev3_bridge.csv")
  write.csv(bridge, bridge_file, row.names = FALSE)
  message("Wrote Rev3 technology bridge: ", bridge_file)
  invisible(bridge)
}


export_rev3_bundle <- function(output_dir, period_estimates, prob.mix, mean.mix, cov.mix,
                               variable_names, input_file, monetary_contract,
                               time_contract, nonlabor_means, calibration_sample_size) {
  if (!identical(time_contract, "activity_index_capacity_v3"))
    stop("Reestimate the activity-index measurement model; legacy time factors cannot be relabeled.")
  if (!identical(monetary_contract, "annual_real_2026_maternal_wage_fixed_income_v4")) {
    stop("Reestimate Attanasio using the maternal-wage and fixed-income budget in real 2026 dollars.")
  }
  expected <- c("hc2", "hc3", "hc4", "tau1", "tau2", "tau3",
                "pinvest1", "pinvest2", "pinvest3", "govinvest1", "govinvest2", "govinvest3",
                "peducation", "income1", "income2", "income3", "centerprice1", "familyprice1",
                "centerprice2", "familyprice2", "centerprice3", "familyprice3",
                "capacity1", "capacity2", "capacity3", "fixedincome1", "fixedincome2", "fixedincome3")
  if (!requireNamespace("digest", quietly = TRUE)) stop("Install the R package digest to export a verified Rev3 bundle.")
  k <- length(expected)
  stopifnot(identical(as.character(variable_names), expected),
            length(prob.mix) == 2L, all(is.finite(prob.mix)), all(prob.mix >= 0),
            abs(sum(prob.mix) - 1) < 1e-8,
            identical(dim(mean.mix), c(2L, k)), all(is.finite(mean.mix)),
            identical(dim(cov.mix), c(2L * k, k)), all(is.finite(cov.mix)))
  staging <- tempfile("rev3-bundle-", tmpdir = output_dir)
  dir.create(staging)
  on.exit(unlink(staging, recursive = TRUE), add = TRUE)
  export_rev3_bridge(staging, period_estimates)
  components <- paste0("component_", 1:2)
  write.csv(data.frame(component = components, probability = prob.mix),
            file.path(staging, "mixture_probabilities.csv"), row.names = FALSE)
  means <- data.frame(component = components, mean.mix, check.names = FALSE)
  names(means) <- c("component", expected)
  write.csv(means, file.path(staging, "mixture_means.csv"), row.names = FALSE)
  for (m in 1:2) {
    covariance <- cov.mix[((m - 1) * k + 1):(m * k), , drop = FALSE]
    stopifnot(max(abs(covariance - t(covariance))) < 1e-8,
              all(diag(covariance) > 0),
              min(eigen(covariance, symmetric = TRUE, only.values = TRUE)$values) >= -1e-8)
    table <- data.frame(variable = expected, covariance, check.names = FALSE)
    names(table) <- c("variable", expected)
    write.csv(table, file.path(staging, paste0("mixture_covariance_", m, ".csv")), row.names = FALSE)
  }
  files <- c("attanasio_rev3_bridge.csv", "mixture_probabilities.csv", "mixture_means.csv",
             "mixture_covariance_1.csv", "mixture_covariance_2.csv", "involvement_calibration.csv")
  # Exact mixture moments avoid Monte Carlo noise in the fixed conversion.
  stopifnot(length(nonlabor_means)==3L, all(is.finite(nonlabor_means)),
            all(nonlabor_means >= 0 & nonlabor_means < 1),
            mean(nonlabor_means)>0, calibration_sample_size>1)
  index_mean <- index_variance <- numeric(3)
  for (t in 1:3) {
    j <- match(paste0("tau", t), expected)
    variances <- vapply(1:2, function(m) cov.mix[(m-1)*k+j, j], 0.0)
    index_mean[t] <- sum(prob.mix * exp(mean.mix[,j] + variances/2))
    index_variance[t] <- sum(prob.mix * exp(2*mean.mix[,j] + 2*variances)) - index_mean[t]^2
  }
  kappa <- mean(index_mean) / mean(nonlabor_means)
  stopifnot(is.finite(kappa), kappa>0, all(is.finite(index_variance)))
  calibration <- data.frame(period=1:3, kappa=kappa,
      mean_involvement=index_mean, variance_involvement=index_variance,
      mean_nonlabor=nonlabor_means, calibration_households=calibration_sample_size,
      minimum_feasible_kappa=index_mean + index_variance/index_mean,
      method="pooled_mean_ratio_activity_index")
  calibration$moment_feasible <- kappa >= calibration$minimum_feasible_kappa
  if (!all(calibration$moment_feasible)) warning(
      "Pooled kappa cannot match all involvement means/variances. Review involvement_calibration.csv; do not silently enlarge kappa.")
  write.csv(calibration, file.path(staging, "involvement_calibration.csv"), row.names=FALSE)
  manifest <- data.frame(
    schema_version = 4L,
    monetary_contract = monetary_contract,
    time_contract = time_contract,
    run_id = basename(staging),
    created_utc = format(Sys.time(), tz = "UTC", usetz = TRUE),
    input_file = normalizePath(input_file, mustWork = TRUE),
    input_sha256 = digest::digest(file = input_file, algo = "sha256"),
    file = files,
    sha256 = vapply(file.path(staging, files), function(f) digest::digest(file = f, algo = "sha256"), "")
  )
  write.csv(manifest, file.path(staging, "rev3_export_manifest.csv"), row.names = FALSE)
  # Mark an in-progress handoff. Julia refuses it, including on the first run.
  marker <- file.path(output_dir, "rev3_export_in_progress")
  writeLines(basename(staging), marker)
  for (file in c(files, "rev3_export_manifest.csv")) {
    if (!file.rename(file.path(staging, file), file.path(output_dir, file))) {
      stop("Could not publish Rev3 bundle; in-progress marker retained: ", file)
    }
  }
  unlink(marker)
  invisible(manifest)
}
