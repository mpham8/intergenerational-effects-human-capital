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
                               variable_names, input_file) {
  expected <- c("hc2", "hc3", "hc4", "tau1", "tau2", "tau3",
                "pinvest1", "pinvest2", "pinvest3", "govinvest1", "govinvest2", "govinvest3",
                "peducation", "income1", "income2", "income3", "centerprice1", "familyprice1",
                "centerprice2", "familyprice2", "centerprice3", "familyprice3")
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
             "mixture_covariance_1.csv", "mixture_covariance_2.csv")
  manifest <- data.frame(
    schema_version = 1L,
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
