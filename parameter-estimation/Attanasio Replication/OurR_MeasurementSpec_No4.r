suppressMessages(library(R.utils))
suppressMessages(library(ks))
suppressMessages(library(gdata))
suppressMessages(library(corpcor))
suppressMessages(library(minpack.lm))
suppressMessages(library(Matrix))
suppressMessages(library(scales))

calibrate_family_budget <- function(income, hours, earnings, education, min_reliable_hours=208) {
  if (any(!is.finite(income) | income <= 0) || any(!is.finite(hours) | hours < 0) ||
      any(!is.finite(education)))
    stop("Budget calibration requires positive family income, finite nonnegative hours, and maternal education.")
  labor <- pmin(hours / 2080, 1)
  invalid_earnings <- !is.finite(earnings) | earnings < 0
  earnings_above_income <- !invalid_earnings & earnings > income
  earnings_at_zero_hours <- !invalid_earnings & earnings > 0 & hours == 0
  reliable <- !invalid_earnings & !earnings_above_income & earnings > 0 &
              hours >= min_reliable_hours
  if (sum(reliable) < 3L || length(unique(education[reliable])) < 2L)
    stop("Too few reliable working mothers to predict a wage in this period.")

  rate <- numeric(length(income))
  rate[reliable] <- earnings[reliable] / labor[reliable]
  # Predict the model's annual full-time earnings rate, not a causal wage
  # equation. The worker regression is refit by age period and bootstrap draw.
  fit_data <- data.frame(education=education[reliable], log_rate=log(rate[reliable]))
  fit <- lm(log_rate ~ education, data=fit_data)
  rate[!reliable] <- exp(predict(fit, newdata=data.frame(education=education[!reliable])))
  if (any(!is.finite(rate) | rate <= 0)) stop("Invalid fitted maternal earnings rate.")

  # Imputed rates must fit the reported budget at baseline hours. Constrain
  # imputed maternal earnings, not reported earnings; flag every binding case.
  feasible_cap <- !reliable & labor > 0 & rate * labor > income
  rate[feasible_cap] <- income[feasible_cap] / labor[feasible_cap]
  maternal <- rate * labor
  maternal[reliable] <- earnings[reliable]
  maternal[feasible_cap] <- income[feasible_cap]
  fixed <- income - maternal
  stopifnot(all(fixed>=0), all(abs(fixed + rate*labor - income) <= 1e-10 * income))
  list(rate=rate, fixed=fixed, labor=labor, reliable=reliable, diagnostics=data.frame(
    records=length(income), observed_earnings_rate=sum(reliable), predicted_rate=sum(!reliable),
    zero_hours=sum(hours==0), positive_earnings_at_zero_hours=sum(earnings_at_zero_hours),
    earnings_above_family_income=sum(earnings_above_income),
    missing_or_negative_earnings=sum(invalid_earnings),
    positive_hours_below_threshold=sum(hours>0 & hours<min_reliable_hours),
    overtime_capped=sum(hours>2080), predicted_wage_bound=sum(feasible_cap),
    zero_fixed_income=sum(fixed==0), minimum_reliable_hours=min_reliable_hours))
}

cluster_bootstrap_panel <- function(panel, cluster_column, strata_column="", seed=NULL) {
  columns <- c(cluster_column, if (nzchar(strata_column)) strata_column)
  if (!nzchar(cluster_column) || !all(columns %in% names(panel)))
    stop("Specify existing bootstrap cluster/strata columns.")
  if (!nrow(panel) || anyNA(panel$id) || anyDuplicated(panel[c("id","period")]))
    stop("Cluster bootstrap requires a nonempty panel with unique child-period rows.")
  cluster <- as.character(panel[[cluster_column]])
  strata <- if (nzchar(strata_column)) as.character(panel[[strata_column]]) else rep("all", nrow(panel))
  if (anyNA(cluster) || anyNA(strata) || any(!nzchar(trimws(c(cluster,strata)))))
    stop("Bootstrap cluster and strata keys must be nonmissing.")
  membership <- unique(data.frame(id=panel$id, cluster=cluster))
  if (anyDuplicated(membership$id)) stop("Each child must have one cluster across periods.")
  cluster_strata <- unique(data.frame(cluster=cluster, strata=strata))
  if (anyDuplicated(cluster_strata$cluster))
    stop("Cluster keys must be unique across strata. Supply a nested cluster identifier.")
  by_stratum <- split(cluster_strata$cluster, cluster_strata$strata)
  if (any(lengths(by_stratum)<2L)) stop("Cluster bootstrap needs at least two clusters per stratum.")
  result <- list(panel=panel, clusters=nrow(cluster_strata), strata=length(by_stratum))
  if (is.null(seed)) return(result)

  # Use a separate RNG stream for resampling; production draws keep their seed.
  old_seed <- get0(".Random.seed", envir=.GlobalEnv, inherits=FALSE)
  if (is.null(old_seed)) on.exit(rm(".Random.seed", envir=.GlobalEnv))
  else on.exit(assign(".Random.seed", old_seed, envir=.GlobalEnv))
  set.seed(seed)
  sampled <- unlist(lapply(by_stratum, function(keys) keys[sample.int(length(keys), replace=TRUE)]),
                    use.names=FALSE)
  cluster_rows <- split(seq_len(nrow(panel)), cluster)
  copies <- unname(cluster_rows[sampled])
  rows <- unlist(copies, use.names=FALSE)
  # A repeated family gets new child IDs, while all its siblings and periods
  # stay together. This avoids cross-wiring duplicate children in the panel.
  result$panel <- panel[rows,,drop=FALSE]
  child_copy <- paste(rep(seq_along(copies), lengths(copies)), panel$id[rows], sep=":")
  result$panel$id <- match(child_copy, unique(child_copy))
  result
}

#dir_data <- "C:/Users/kahna/Dropbox/OConnell 2025 Research/"
# Master File.r resolves overrides relative to the launch directory. Do not
# reread a relative environment path after changing the working directory.
setwd(dir_data)
if (!file.exists(data_file)) {
  stop(paste("NLSY input file not found:", data_file))
}
bootstrap_cluster_column <- Sys.getenv("IEHC_BOOTSTRAP_CLUSTER_COLUMN")
bootstrap_strata_column <- Sys.getenv("IEHC_BOOTSTRAP_STRATA_COLUMN")
bootstrap_sample_seed <- Sys.getenv("IEHC_BOOTSTRAP_SAMPLE_SEED")
bootstrap_keys <- unique(c(bootstrap_cluster_column, bootstrap_strata_column))
bootstrap_keys <- bootstrap_keys[nzchar(bootstrap_keys)]
childPeriodData <- read.csv(data_file, header=TRUE, check.names=FALSE,
                           colClasses=setNames(rep("character", length(bootstrap_keys)), bootstrap_keys))

# Both full and dry runs use the same balanced three-period panel. A dry run
# additionally keeps the first sorted IDs so sampling is deterministic across
# macOS and Windows.
dry_run_ids <- as.integer(Sys.getenv("IEHC_DRY_RUN_IDS", unset = "0"))
period_sets <- lapply(0:2, function(p) {
  unique(childPeriodData$id[childPeriodData$period == p])
})
balanced_ids <- sort(Reduce(intersect, period_sets))
if (!is.na(dry_run_ids) && dry_run_ids > 0) {
  selected_ids <- head(balanced_ids, dry_run_ids)
} else {
  selected_ids <- balanced_ids
}
childPeriodData <- childPeriodData[
  childPeriodData$period %in% 0:2 & childPeriodData$id %in% selected_ids,
]
if (length(bootstrap_keys) || nzchar(bootstrap_sample_seed)) {
  seed <- if (nzchar(bootstrap_sample_seed)) suppressWarnings(as.integer(bootstrap_sample_seed)) else NULL
  if (!is.null(seed) && (!grepl("^[0-9]+$", bootstrap_sample_seed) || is.na(seed)))
    stop("IEHC_BOOTSTRAP_SAMPLE_SEED must be a nonnegative R integer.")
  design <- cluster_bootstrap_panel(childPeriodData, bootstrap_cluster_column, bootstrap_strata_column, seed)
  childPeriodData <- design$panel
  write.csv(data.frame(cluster_column=bootstrap_cluster_column, strata_column=bootstrap_strata_column,
                       sample_seed=bootstrap_sample_seed, clusters=design$clusters, strata=design$strata),
            file.path(dir_output, "bootstrap_design.csv"), row.names=FALSE)
}
childPeriodData <- childPeriodData[order(childPeriodData$id, childPeriodData$period), ]

standardize <- function(x, variable_name, period_label = "pooled") {
  x_mean <- mean(x, na.rm=TRUE)
  x_sd <- sd(x, na.rm=TRUE)
  if (!is.finite(x_sd) || x_sd == 0) {
    stop(paste(variable_name, "has no usable variation in", period_label))
  }
  return((x - x_mean) / x_sd)
}

standardize_by_period <- function(x, period, variable_name, active_periods) {
  standardized <- rep(NA_real_, length(x))
  for (p in active_periods) {
    in_period <- !is.na(period) & period == p
    standardized[in_period] <- standardize(x[in_period], variable_name,
                                            paste("period", p))
  }
  return(standardized)
}

# CEX expenditure is the cardinal parental-investment measure in every period.
# Annualize cumulative expenditure, enforce the same coverage rule as Rev3, and
# retain real 2026 dollars, the same units as annual family income. Missing or
# insufficient CEX coverage remains missing.
cex_expenditure_column <-
  "CEX Cumulative Household Education Expenditure (Real 2026 Dollars)"
cex_years_column <- "CEX Years Successfully Matched"
cex_coverage_column <- "CEX Coverage Share"
required_input_columns <- c(
  cex_expenditure_column, cex_years_column, cex_coverage_column,
  "TNFI_TRUNC", "HRSWK_PCY", "LABOR_INCOME", "CHILD_EVER_ENROLLED_IN_HEAD",
  "TCURELSC_per_student", "HGC_OF_MOTHER_AS_OF_MAY_1_R",
  "HOW_OFTEN_MOM_READS", "MOM_HELPS_CH_LEARN_NUMBERS",
  "MOM_HELPS_CH_LEARN_SHAPES", "HOW_OFT_CH_TAKEN_TO_PERFORMANCE"
)
missing_input_columns <- setdiff(required_input_columns, names(childPeriodData))
if (length(missing_input_columns) > 0) {
  stop(paste("Missing required Attanasio input columns:",
             paste(missing_input_columns, collapse=", ")))
}
minimum_cex_coverage <- as.numeric(
  Sys.getenv("IEHC_MIN_CEX_COVERAGE", unset="0.5")
)
if (!is.finite(minimum_cex_coverage) || minimum_cex_coverage < 0 ||
    minimum_cex_coverage > 1) {
  stop("IEHC_MIN_CEX_COVERAGE must be between zero and one")
}

# The restricted NLSY extract reports Head Start exposure as the fraction of
# the available enrollment period, rather than duration in months.
observed_head_start <- is.finite(childPeriodData$CHILD_EVER_ENROLLED_IN_HEAD)
if (any(childPeriodData$CHILD_EVER_ENROLLED_IN_HEAD[observed_head_start] < 0 |
        childPeriodData$CHILD_EVER_ENROLLED_IN_HEAD[observed_head_start] > 1)) {
  stop("CHILD_EVER_ENROLLED_IN_HEAD must be between zero and one")
}

childPeriodData$CEX_ANNUAL_INVESTMENT <- NA_real_
for (p in 0:2) {
  in_period <- !is.na(childPeriodData$period) & childPeriodData$period == p
  eligible <- in_period &
              is.finite(childPeriodData[[cex_expenditure_column]]) &
              childPeriodData[[cex_expenditure_column]] >= 0 &
              is.finite(childPeriodData[[cex_years_column]]) &
              childPeriodData[[cex_years_column]] > 0 &
              is.finite(childPeriodData[[cex_coverage_column]]) &
              childPeriodData[[cex_coverage_column]] >= minimum_cex_coverage
  childPeriodData$CEX_ANNUAL_INVESTMENT[eligible] <-
    (childPeriodData[[cex_expenditure_column]][eligible] /
       childPeriodData[[cex_years_column]][eligible])
}

# Work hours calibrate resources and the involvement conversion, not a latent
# measurement anchor. Preserve the agreed annual-hours denominator and cap.
childPeriodData$NONLABOR_TIME_SHARE <-
  1 - pmin(pmax(childPeriodData$HRSWK_PCY / (52 * 40), 0), 1)

# PIAT anchors child HC. Reading and performance participation connect the
# activity index across ages; these are involvement units, not childcare hours.
pooled_measure_periods <- list(
  PIAT_MATH = 0:2,
  HOW_OFTEN_MOM_READS = 0:1,
  MUSIC_INSTMT_CH = 1:2,
  HOW_OFT_CH_TAKEN_TO_PERFORMANCE = 1:2
)
for (measure in names(pooled_measure_periods)) {
  for (p in pooled_measure_periods[[measure]]) {
    observed <- childPeriodData$period == p &
                is.finite(childPeriodData[[measure]])
    if (sum(observed) < 2) {
      stop(paste(measure, "is missing from required bridge period", p))
    }
  }
  active <- childPeriodData$period %in% pooled_measure_periods[[measure]]
  childPeriodData[[measure]][active] <- standardize(
    childPeriodData[[measure]][active], measure
  )
}

# Other noisy measures are normalized separately within each period.
period_normalized_measures <- list(
  PIAT_READ_REC = 0:2,
  PIAT_READ_COMP = 0:2,
  PPVT = 0:2,
  MOM_HELPS_CH_LEARN_SHAPES = 0,
  MOM_HELPS_CH_LEARN_NUMBERS = 0,
  CH_GET_SPEC_LESSON = 1:2,
  HOW_OFT_CH_W_DAD_OUTDOORS = 1:2,
  HOW_OFT_CH_SPEND_TIME_W_DAD = 1:2
)
for (measure in names(period_normalized_measures)) {
  childPeriodData[[measure]] <- standardize_by_period(
    childPeriodData[[measure]], childPeriodData$period, measure,
    period_normalized_measures[[measure]]
  )
}

# Normalize observed production-function covariates over the pooled sample.
childPeriodData$ANNUAL_FAMILY_INCOME <- childPeriodData$TNFI_TRUNC
childPeriodData <- transform(childPeriodData,
    CHILD_EVER_ENROLLED_IN_HEAD = standardize(CHILD_EVER_ENROLLED_IN_HEAD, "CHILD_EVER_ENROLLED_IN_HEAD"),
    TCURELSC_per_student = standardize(ifelse(TCURELSC_per_student > 0, log(TCURELSC_per_student), NA), "TCURELSC_per_student"),
    HGC_OF_MOTHER_AS_OF_MAY_1_R = standardize(HGC_OF_MOTHER_AS_OF_MAY_1_R, "HGC_OF_MOTHER_AS_OF_MAY_1_R"),
    # drawfactor exponentiates this: preserve annual family income in dollars,
    # not an exponentiated z-score. Nonpositive income cannot enter a log model.
    TNFI_TRUNC = ifelse(is.finite(TNFI_TRUNC) & TNFI_TRUNC > 0, log(TNFI_TRUNC), NA),
    Center_Care_Price = standardize(Center_Care_Price, "Center_Care_Price"),
    Family_Care_Price = standardize(Family_Care_Price, "Family_Care_Price")
)

childPeriod0Data <- subset(childPeriodData, period == 0)
childPeriod1Data <- subset(childPeriodData, period == 1)
childPeriod2Data <- subset(childPeriodData, period == 2)


# Data
hc2  <- cbind(childPeriod0Data$PIAT_MATH, 
   childPeriod0Data$PIAT_READ_REC, 
   childPeriod0Data$PIAT_READ_COMP, 
   childPeriod0Data$PPVT)
hc3 <- cbind(childPeriod1Data$PIAT_MATH,
   childPeriod1Data$PIAT_READ_REC,
   childPeriod1Data$PIAT_READ_COMP,
   childPeriod1Data$PPVT)
hc4 <- cbind(childPeriod2Data$PIAT_MATH,
   childPeriod2Data$PIAT_READ_REC,
   childPeriod2Data$PIAT_READ_COMP,
   childPeriod2Data$PPVT)


tau1_measures <- cbind(childPeriod0Data$HOW_OFTEN_MOM_READS,
   childPeriod0Data$MOM_HELPS_CH_LEARN_NUMBERS,
   #scale(childPeriod0Data$HOW_MANY_BOOKS, center=F, scale=T), 
   #scale(childPeriod0Data$HOW_OFT_CH_EATS_W, center=F, scale=T), 
   ###childPeriod0Data$MOM_HELPS_CH_LEARN_NUMBERS, #No scaling for 0/1 categorical variables
   ###childPeriod0Data$MOM_HELPS_CH_LEARN_ALPHABET, 
   ###childPeriod0Data$MOM_HELPS_CH_LEARN_COLORS, 
   childPeriod0Data$MOM_HELPS_CH_LEARN_SHAPES
   #scale(childPeriod0Data$HOW_OFT_CH_TAKEN_TO_MUSEUM, center=F, scale=T),
   ##scale(childPeriod0Data$MUSIC_INSTMT_CH, center=F, scale=T),
   ##scale(childPeriod0Data$CH_GET_SPEC_LESSON, center=F, scale=T),
   ##scale(childPeriod0Data$HOW_OFT_CH_TAKEN_TO_PERFORMANCE, center=F, scale=T),
   ##scale(childPeriod0Data$HOW_OFT_CH_W_DAD_OUTDOORS, center=F, scale=T),
   ##scale(childPeriod0Data$HOW_OFT_CH_SPEND_TIME_W_DAD, center=F, scale=T)
   #scale(childPeriod0Data$DO_PARS_DISCUSS_TV, center=F, scale=T),
   #scale(childPeriod0Data$MOM_HELPS_CH_W_NONE, center=F, scale=T),
   #scale(childPeriod0Data$OWN_A_HOME_COMPUTER, center=F, scale=T),
   #scale(childPeriod0Data$PARS_HELP_W_HOMEWORK, center=F, scale=T)
   )
tau2_measures <- cbind(childPeriod1Data$HOW_OFTEN_MOM_READS,
   childPeriod1Data$HOW_OFT_CH_TAKEN_TO_PERFORMANCE,
   #scale(childPeriod1Data$HOW_MANY_BOOKS, center=F, scale=T),
   #scale(childPeriod1Data$HOW_OFT_CH_EATS_W, center=F, scale=T),
   ##scale(childPeriod1Data$MOM_HELPS_CH_LEARN_NUMBERS, center=F, scale=T),
   ##scale(childPeriod1Data$MOM_HELPS_CH_LEARN_ALPHABET, center=F, scale=T),
   ##scale(childPeriod1Data$MOM_HELPS_CH_LEARN_COLORS, center=F, scale=T),
   ##scale(childPeriod1Data$MOM_HELPS_CH_LEARN_SHAPES, center=F, scale=T),
   #scale(childPeriod1Data$HOW_OFT_CH_TAKEN_TO_MUSEUM, center=F, scale=T),
   childPeriod1Data$MUSIC_INSTMT_CH,
   childPeriod1Data$CH_GET_SPEC_LESSON,
   childPeriod1Data$HOW_OFT_CH_W_DAD_OUTDOORS,
   childPeriod1Data$HOW_OFT_CH_SPEND_TIME_W_DAD
   #scale(childPeriod1Data$DO_PARS_DISCUSS_TV, center=F, scale=T),
   #scale(childPeriod1Data$MOM_HELPS_CH_W_NONE, center=F, scale=T),
   #scale(childPeriod1Data$OWN_A_HOME_COMPUTER, center=F, scale=T),
   #scale(childPeriod1Data$PARS_HELP_W_HOMEWORK, center=F, scale=T)
   )
tau3_measures <- cbind(childPeriod2Data$HOW_OFT_CH_TAKEN_TO_PERFORMANCE,
   childPeriod2Data$MUSIC_INSTMT_CH,
   #scale(childPeriod2Data$HOW_MANY_BOOKS, center=F, scale=T),
   #scale(childPeriod2Data$HOW_OFT_CH_EATS_W, center=F, scale=T),
   ##scale(childPeriod2Data$MOM_HELPS_CH_LEARN_NUMBERS, center=F, scale=T),
   ##scale(childPeriod2Data$MOM_HELPS_CH_LEARN_ALPHABET, center=F, scale=T),
   ##scale(childPeriod2Data$MOM_HELPS_CH_LEARN_COLORS, center=F, scale=T),
   ##scale(childPeriod2Data$MOM_HELPS_CH_LEARN_SHAPES, center=F, scale=T),
   #scale(childPeriod2Data$HOW_OFT_CH_TAKEN_TO_MUSEUM, center=F, scale=T),
   childPeriod2Data$CH_GET_SPEC_LESSON,
   childPeriod2Data$HOW_OFT_CH_W_DAD_OUTDOORS,
   childPeriod2Data$HOW_OFT_CH_SPEND_TIME_W_DAD
   #scale(childPeriod2Data$DO_PARS_DISCUSS_TV, center=F, scale=T),
   #scale(childPeriod2Data$MOM_HELPS_CH_W_NONE, center=F, scale=T),
   #scale(childPeriod2Data$OWN_A_HOME_COMPUTER, center=F, scale=T),
   #scale(childPeriod2Data$PARS_HELP_W_HOMEWORK, center=F, scale=T)
   )

# CEX is observed directly in the consumption units used by Rev3. Store its
# log in the joint distribution because drawfactor exponentiates every input.
pinvest1 <- log(pmax(childPeriod0Data$CEX_ANNUAL_INVESTMENT, 1e-8))
pinvest2 <- log(pmax(childPeriod1Data$CEX_ANNUAL_INVESTMENT, 1e-8))
pinvest3 <- log(pmax(childPeriod2Data$CEX_ANNUAL_INVESTMENT, 1e-8))

govinvest1 <- childPeriod0Data$CHILD_EVER_ENROLLED_IN_HEAD
govinvest2 <- childPeriod1Data$TCURELSC_per_student
govinvest3 <- childPeriod2Data$TCURELSC_per_student

peducation <- childPeriod0Data$HGC_OF_MOTHER_AS_OF_MAY_1_R

income1 <- childPeriod0Data$TNFI_TRUNC
income2 <- childPeriod1Data$TNFI_TRUNC
income3 <- childPeriod2Data$TNFI_TRUNC

prices1 <- cbind(childPeriod0Data$Center_Care_Price,
   childPeriod0Data$Family_Care_Price)
prices2 <- cbind(childPeriod1Data$Center_Care_Price,
   childPeriod1Data$Family_Care_Price)
prices3 <- cbind(childPeriod2Data$Center_Care_Price,
   childPeriod2Data$Family_Care_Price)

# Base R returns NULL for ncol(vector), but each scalar observed input still
# contributes one column to the measurement-system matrices.
num_columns <- function(x) if (is.null(dim(x))) 1L else ncol(x)
Nhc2 <- num_columns(hc2)
Nhc3 <- num_columns(hc3)
Nhc4 <- num_columns(hc4)
Ntau1 <- num_columns(tau1_measures)
Ntau2 <- num_columns(tau2_measures)
Ntau3 <- num_columns(tau3_measures)
Npinvest1 <- num_columns(pinvest1)
Npinvest2 <- num_columns(pinvest2)
Npinvest3 <- num_columns(pinvest3)
Ngovinvest1 <- num_columns(govinvest1)
Ngovinvest2 <- num_columns(govinvest2)
Ngovinvest3 <- num_columns(govinvest3)
Npeducation <- num_columns(peducation)
Nincome1 <- num_columns(income1)
Nincome2 <- num_columns(income2)
Nincome3 <- num_columns(income3)
Nprices1 <- num_columns(prices1)
Nprices2 <- num_columns(prices2)
Nprices3 <- num_columns(prices3)


y <- data.matrix(cbind(hc2, hc3, hc4,
   tau1_measures, tau2_measures, tau3_measures,
   pinvest1, pinvest2, pinvest3,
   govinvest1, govinvest2, govinvest3,
   peducation,
   income1, income2, income3,
   prices1, prices2, prices3))
complete_measurements <- complete.cases(y)
if (sum(complete_measurements)<3L) stop("Too few complete households for budget calibration.")
min_reliable_hours <- suppressWarnings(as.numeric(Sys.getenv("IEHC_MIN_RELIABLE_WAGE_HOURS", "208")))
if (!is.finite(min_reliable_hours) || min_reliable_hours <= 0 || min_reliable_hours > 2080)
  stop("IEHC_MIN_RELIABLE_WAGE_HOURS must be positive and at most 2080.")
period_data <- list(childPeriod0Data, childPeriod1Data, childPeriod2Data)
budget <- lapply(seq_along(period_data), function(t) {
  raw <- period_data[[t]][complete_measurements,,drop=FALSE]
  calibrate_family_budget(raw$ANNUAL_FAMILY_INCOME, raw$HRSWK_PCY, raw$LABOR_INCOME,
                          peducation[complete_measurements], min_reliable_hours)
})
labor_observed <- do.call(cbind, lapply(budget, function(x) x$labor))
capacity <- do.call(cbind, lapply(budget, function(x) log(x$rate)))
# log1p allows exact zero in the observed budget. Simulated negative logs
# map to zero under the documented censored log1p distribution.
fixed_income <- do.call(cbind, lapply(budget, function(x) log1p(x$fixed)))
y <- cbind(y[complete_measurements, , drop=FALSE], capacity, fixed_income)
if (nrow(y)<2L || any(!is.finite(y))) stop("Too few complete households or nonfinite prepared measurements/capacities.")
nonlabor_means <- colMeans(1-labor_observed)
budget_diagnostics <- do.call(rbind, lapply(1:3, function(t) data.frame(period=t, budget[[t]]$diagnostics)))
write.csv(budget_diagnostics, file.path(dir_output, "budget_calibration_diagnostics.csv"), row.names=FALSE)
print(budget_diagnostics)
calibration_sample_size <- nrow(y)
write.csv(summary(y), file.path(dir_output, "y_summary.csv"))

nZ <- ncol(y)       # nZ : number of measurements

nobs <- nrow(y)     # nObs : number of observations

ninst <- Npinvest1+Npinvest2+Npinvest3+Ngovinvest1+Ngovinvest2+Ngovinvest3+Npeducation+Nincome1+Nincome2+Nincome3+Nprices1+Nprices2+Nprices3+6L

nZinst <- nZ-ninst


##########################################################
# Set the inputs for the EM continuous function     
##########################################################      

# Initial values for M step: this is a list with (sigma, mean, prop)  
# Dollar logs are far from zero. Start EM near the observed means so the first
# likelihood evaluation does not underflow simply because monetary units changed.
mean0 <- rbind(colMeans(y) - .00005, colMeans(y) + .00005)
cov.start <- make.positive.definite(cov(y, use ="complete.obs"))
sigma0 <- list(cov.start, cov.start)
prop0 <- c(.5,.5)
mstep.start <- list(mean0, sigma0, prop0)
  
# Number of mixtures              
nM <- 2

# Number of factors 
nF <-6

# Convergence criterion            
conv <- 1e-05

# Configuration of factor loadings  (2 = free param, 1 = normalized to 1, 0 = fixed to 0) 
startSeq <- c(1,
            1+Nhc2,
            1+Nhc2 + Nhc3,
            1+Nhc2 + Nhc3 + Nhc4,
            1+Nhc2 + Nhc3 + Nhc4 + Ntau1,
            1+Nhc2 + Nhc3 + Nhc4 + Ntau1 + Ntau2,
            1+Nhc2 + Nhc3 + Nhc4 + Ntau1 + Ntau2 + Ntau3)


endSeq <- c(startSeq[2:(nF)]-1,nZ-ninst)

freelambda <- matrix(0, nZinst, nF) 
for (i in 1:nF) freelambda[startSeq[i]:endSeq[i], i]     <- 2 
# Start with unit proxies; the overlapping period-3 activity proxy below shares
# its estimated loading with period 2 rather than receiving another unit anchor.
anchor_rows <- startSeq[1:nF]
for (i in 1:nF) freelambda[anchor_rows[i], i]             <- 1
# Reading anchors periods 1-2. Performances bridge periods 2-3 with the same
# estimated loading and intercept; do not impose a second arbitrary unit scale.
freelambda[startSeq[6], 6] <- 3
loading_ties <- list(c(startSeq[5]+1L, 5L, startSeq[6], 6L))
constant_ties <- list(c(startSeq[5]+1L, startSeq[6]))

#Configuration of constants (2 = free param, otherwise constant is set according to normalized measure in base period)
constparam <- matrix(0,nZinst,1)
meanbase<-colMeans(y,na.rm=TRUE)

#Set constants for baseline measures and age-invariant measures to true values
for (i in 1:nF) constparam[startSeq[i]:endSeq[i]] <- 2
#age invariant/normalized
# Reading has zero intercept in periods 1-2 in pooled standardized units.
for (i in 1:3) constparam[startSeq[i]]               <- meanbase[startSeq[1]]
for (i in 4:5) constparam[startSeq[i]]               <- 0
constparam[startSeq[6]] <- 3


#remaining first period
# Only first-period child HC uses a zero-mean baseline normalization. Activity
# means and nonanchor intercepts are estimated under the overlapping restrictions.
baseline <- 1:Nhc2


for (i in (baseline)) constparam[i] <- meanbase[i]
for (i in 4:5) constparam[startSeq[i]] <- 0

freeconstant<-constparam
freeconstant[which(freeconstant!=2)]  <- 1


baselinemean<-c(1)
baselinelambda <- NULL # Retained legacy interface; loading restrictions are explicit above.

#Construct matrix to pull values for income covariances from EM to add to LS step

instcovpull <- c(1,
            1+Nhc2,
            1+Nhc2 + Nhc3,
            1+Nhc2 + Nhc3 + Nhc4,
            1+Nhc2 + Nhc3 + Nhc4 + Ntau1,
            1+Nhc2 + Nhc3 + Nhc4 + Ntau1 + Ntau2,
            (1+Nhc2 + Nhc3 + Nhc4 + Ntau1 + Ntau2 + Ntau3):nZ)

# Initial values of parameters for the minimum distance step (if choose measures for latent factors normalized to 1, don't have to divide by lambda^2)        
param.start <- c(rep(1,nZinst), rep(1, nM*0.5*nF*(nF+1)), rep(1,nF*2-length(baselinemean)), rep(1, length(which(freelambda==2))), rep(1, length(which(freeconstant==2))) ) 

EMstart1 <- list(mstep.start, param.start, y, nM, nF, freelambda, constparam, instcovpull, ninst, nobs, conv)
save(EMstart1, file=file.path(dir_output, "EMstart1.Rdata"))



namef<- c("hc2", "hc3", "hc4",
          "tau1", "tau2", "tau3",
          "pinvest1", "pinvest2", "pinvest3",
          "govinvest1", "govinvest2", "govinvest3",
          "peducation",
          "income1", "income2", "income3",
          "centerprice1", "familyprice1", "centerprice2", "familyprice2",
          "centerprice3", "familyprice3", "capacity1", "capacity2", "capacity3",
          "fixedincome1", "fixedincome2", "fixedincome3")
