# Run from the repository root. Only digest and minpack.lm are needed.
library(minpack.lm)
script_dir <- normalizePath("parameter-estimation/Attanasio Replication")
production <- parse(file.path(script_dir, "Our_Production_Functions_No4_Incomeinst.r"))
loop_index <- which(vapply(production, function(expr) is.call(expr) && identical(expr[[1]], as.name("for")), TRUE))[[1]]
env <- new.env(parent = globalenv())
env$dir <- script_dir
for (expr in production[seq_len(loop_index - 1L)]) eval(expr, env)

set.seed(73)
x <- matrix(rnorm(1000), 200, 5)
y <- as.vector(x %*% c(2, .2, .5, -1, -.3)) + rnorm(200, sd = .001)
fit <- nlsLM(y ~ theta_h*x[,1] + theta_p*x[,2] + ces_delta*x[,3] + mu*x[,4] + rho*x[,5],
             start = c(theta_h=.3, theta_p=.5, ces_delta=.5, mu=-.5, rho=-.5),
             lower = c(.01,.01,.01,-5,-5), upper = c(.99,.99,.99,.99,.99))
suppressWarnings(env$check_production_fit(fit, 1L, TRUE))
stopifnot(grepl("theta_h", env$production_fit_diagnostics[[1]]$boundary_parameters),
          env$production_fit_diagnostics[[1]]$converged)
failed_fit <- fit
failed_fit$convInfo$isConv <- FALSE
stopifnot(inherits(try(env$check_production_fit(failed_fit, 1L, TRUE), silent = TRUE), "try-error"))

# Exercise the complete bootstrap-disabled production/export/table control flow.
# Fit values are fixed here to isolate I/O from econometric identification.
workspace <- tempfile("iehc-export-test-")
dir.create(workspace)
env$dir_output <- workspace
env$Bootstrap <- 0L
env$bsample <- 2L
env$data_file <- file.path(workspace, "synthetic_input.csv")
write.csv(data.frame(id=1:5), env$data_file, row.names=FALSE)
env$namef <- c("hc2", "hc3", "hc4", "tau1", "tau2", "tau3", "pinvest1", "pinvest2", "pinvest3",
               "govinvest1", "govinvest2", "govinvest3", "peducation", "income1", "income2", "income3",
               "centerprice1", "familyprice1", "centerprice2", "familyprice2", "centerprice3", "familyprice3")
prob.mix <- c(.4, .6)
mean.mix <- matrix(seq_len(44)/100 - 1, 2, 22)
cov.mix <- rbind(diag(.1, 22), diag(.2, 22))
save(prob.mix, mean.mix, cov.mix, file=file.path(workspace, "trueFM.R"))
# An invalid stale bootstrap file must never be opened when the switch is off.
writeLines("must not load", file.path(workspace, "allbootFM.R"))
env$drawfactor <- function(...) list(data.frame(dummy=1))
base <- c(.1, .03, .3, .5, .5, -.4, -.2, 1/1.2, 1/1.4, .1)
cf <- append(base, .02, after=1)
for (period in 1:3) {
  assign(paste0("CES", period, "_nocf"), function(...) base, env)
  cf_function <- local({
    p <- period
    function(...) list(cf, rep(.1, if (p==1) 3 else 4), rep(.1, if (p==1) 3 else 4), data.frame(coefficient=.1))
  })
  assign(paste0("CES", period, "_cf"), cf_function, env)
}
old_wd <- getwd()
for (expr in production[loop_index:length(production)]) eval(expr, env)
setwd(old_wd)
files <- c("attanasio_rev3_bridge.csv", "mixture_probabilities.csv", "mixture_means.csv",
           "mixture_covariance_1.csv", "mixture_covariance_2.csv")
stopifnot(all(file.exists(file.path(workspace, files))),
          !file.exists(file.path(workspace, "rev3_export_in_progress")),
          !file.exists(file.path(workspace, "bootCES1.R")))
manifest <- read.csv(file.path(workspace, "rev3_export_manifest.csv"))
stopifnot(length(unique(manifest$run_id))==1L,
          identical(manifest$sha256, unname(vapply(file.path(workspace, manifest$file), function(f) digest::digest(file=f, algo="sha256"), ""))))
means <- as.matrix(read.csv(file.path(workspace, "mixture_means.csv"))[, -1])
stopifnot(max(abs(means - mean.mix)) < 1e-14)
for (period in 1:3) {
  table <- read.csv(file.path(workspace, paste0("cestable_", period, ".csv")))
  stopifnot(nrow(table)==11L, ncol(table)==3L)
}
bad_mean <- mean.mix
bad_mean[1,1] <- NA_real_
stopifnot(inherits(try(env$export_rev3_bundle(workspace, list(env$trueCESperiod1, env$trueCESperiod2, env$trueCESperiod3),
                                           prob.mix, bad_mean, cov.mix, env$namef, env$data_file), silent=TRUE), "try-error"))
cat("R export, bootstrap switch, and production diagnostics checks passed.\n")
# Optionally copy the fixture for a Julia interoperability check.
fixture_destination <- Sys.getenv("IEHC_TEST_BUNDLE_DIR", "")
if (nzchar(fixture_destination)) {
  dir.create(fixture_destination, recursive=TRUE, showWarnings=FALSE)
  stopifnot(all(file.copy(file.path(workspace, c(files, "rev3_export_manifest.csv")), fixture_destination, overwrite=TRUE)))
}
unlink(workspace, recursive=TRUE)
