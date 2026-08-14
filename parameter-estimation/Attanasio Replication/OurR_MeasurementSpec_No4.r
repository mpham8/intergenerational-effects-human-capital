suppressMessages(library(R.utils))
suppressMessages(library(ks))
suppressMessages(library(gdata))
suppressMessages(library(corpcor))
suppressMessages(library(minpack.lm))
suppressMessages(library(Matrix))
suppressMessages(library(scales))
suppressMessages(library(openxlsx))

#dir_data <- "C:/Users/kahna/Dropbox/OConnell 2025 Research/"
setwd(dir_data)
data_file <- Sys.getenv(
  "IEHC_NLSY_DATA_FILE",
  unset = "/Users/joshualeinwand/Desktop/Summer/Expenditure Per Student/outputs/Fake_Merged_Data_with_cex_education_expenditure.csv"
)
if (!file.exists(data_file)) {
  stop(paste("NLSY input file not found:", data_file))
}
childPeriodData <- read.csv(data_file, header=TRUE, check.names=FALSE)

# A dry run uses the same balanced three-period panel as the full pipeline but
# limits the number of IDs before the expensive factor-model estimation. The
# first sorted IDs make this deterministic across macOS and Windows.
dry_run_ids <- as.integer(Sys.getenv("IEHC_DRY_RUN_IDS", unset = "0"))
if (!is.na(dry_run_ids) && dry_run_ids > 0) {
  period_sets <- lapply(0:2, function(p) unique(childPeriodData$id[childPeriodData$period == p]))
  balanced_ids <- sort(Reduce(intersect, period_sets))
  selected_ids <- head(balanced_ids, dry_run_ids)
  childPeriodData <- childPeriodData[
    childPeriodData$period %in% 0:2 & childPeriodData$id %in% selected_ids,
  ]
}

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
# divide by each period's median labor income so investment and resources share
# consumption units. Missing or insufficient CEX coverage remains missing.
cex_expenditure_column <-
  "CEX Cumulative Household Education Expenditure (Real 2026 Dollars)"
cex_years_column <- "CEX Years Successfully Matched"
cex_coverage_column <- "CEX Coverage Share"
cex_required_columns <- c(
  cex_expenditure_column, cex_years_column, cex_coverage_column,
  "LABOR_INCOME", "HRSWK_PCY"
)
missing_cex_columns <- setdiff(cex_required_columns, names(childPeriodData))
if (length(missing_cex_columns) > 0) {
  stop(paste("Missing CEX cardinal-measure columns:",
             paste(missing_cex_columns, collapse=", ")))
}
minimum_cex_coverage <- as.numeric(
  Sys.getenv("IEHC_MIN_CEX_COVERAGE", unset="0.5")
)
if (!is.finite(minimum_cex_coverage) || minimum_cex_coverage < 0 ||
    minimum_cex_coverage > 1) {
  stop("IEHC_MIN_CEX_COVERAGE must be between zero and one")
}

childPeriodData$CEX_ANNUAL_INVESTMENT <- NA_real_
for (p in 0:2) {
  in_period <- !is.na(childPeriodData$period) & childPeriodData$period == p
  valid_income <- in_period & is.finite(childPeriodData$LABOR_INCOME) &
                  childPeriodData$LABOR_INCOME > 0
  income_scale <- median(childPeriodData$LABOR_INCOME[valid_income])
  if (!is.finite(income_scale) || income_scale <= 0) {
    stop(paste("LABOR_INCOME has no positive period", p, "median"))
  }
  eligible <- in_period &
              is.finite(childPeriodData[[cex_expenditure_column]]) &
              childPeriodData[[cex_expenditure_column]] >= 0 &
              is.finite(childPeriodData[[cex_years_column]]) &
              childPeriodData[[cex_years_column]] > 0 &
              is.finite(childPeriodData[[cex_coverage_column]]) &
              childPeriodData[[cex_coverage_column]] >= minimum_cex_coverage
  childPeriodData$CEX_ANNUAL_INVESTMENT[eligible] <-
    (childPeriodData[[cex_expenditure_column]][eligible] /
       childPeriodData[[cex_years_column]][eligible]) / income_scale
}

# Use the same non-labor time share as Rev3 as the cardinal parental-time
# measure. The measurement model is in logs, so anchoring to log share makes
# the exponentiated factor draws return shares on the original 0-to-1 scale.
childPeriodData$NONLABOR_TIME_SHARE <-
  1 - pmin(pmax(childPeriodData$HRSWK_PCY / (52 * 40), 0), 1)
childPeriodData$LOG_NONLABOR_TIME_SHARE <-
  log(pmax(childPeriodData$NONLABOR_TIME_SHARE, 1e-8))

# PIAT Math and non-labor time set the common child-HC and parental-time scales.
# CEX remains the observed monetary input. Other time indicators retain pooled
# or period-specific normalization and have freely estimated factor loadings.
pooled_measure_periods <- list(
  PIAT_MATH = 0:2,
  HOW_OFTEN_MOM_READS = 0:1,
  MUSIC_INSTMT_CH = 1:2
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
  CH_GET_SPEC_LESSON = 1:2,
  HOW_OFT_CH_TAKEN_TO_PERFORMANCE = 1:2,
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
childPeriodData <- transform(childPeriodData,
    HOW_LONG_CHILD_WAS_IN_HEAD = standardize(HOW_LONG_CHILD_WAS_IN_HEAD, "HOW_LONG_CHILD_WAS_IN_HEAD"),
    TCURELSC_per_student = standardize(ifelse(TCURELSC_per_student > 0, log(TCURELSC_per_student), NA), "TCURELSC_per_student"),
    HGC_OF_MOTHER_AS_OF_MAY_1_R = standardize(HGC_OF_MOTHER_AS_OF_MAY_1_R, "HGC_OF_MOTHER_AS_OF_MAY_1_R"),
    TNFI_TRUNC = standardize(ifelse(TNFI_TRUNC > 0, log(TNFI_TRUNC), NA), "TNFI_TRUNC"),
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


tau1_measures <- cbind(childPeriod0Data$LOG_NONLABOR_TIME_SHARE,
   childPeriod0Data$HOW_OFTEN_MOM_READS,
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
tau2_measures <- cbind(childPeriod1Data$LOG_NONLABOR_TIME_SHARE,
   childPeriod1Data$HOW_OFTEN_MOM_READS,
   #scale(childPeriod1Data$HOW_MANY_BOOKS, center=F, scale=T),
   #scale(childPeriod1Data$HOW_OFT_CH_EATS_W, center=F, scale=T),
   ##scale(childPeriod1Data$MOM_HELPS_CH_LEARN_NUMBERS, center=F, scale=T),
   ##scale(childPeriod1Data$MOM_HELPS_CH_LEARN_ALPHABET, center=F, scale=T),
   ##scale(childPeriod1Data$MOM_HELPS_CH_LEARN_COLORS, center=F, scale=T),
   ##scale(childPeriod1Data$MOM_HELPS_CH_LEARN_SHAPES, center=F, scale=T),
   #scale(childPeriod1Data$HOW_OFT_CH_TAKEN_TO_MUSEUM, center=F, scale=T),
   childPeriod1Data$MUSIC_INSTMT_CH,
   childPeriod1Data$CH_GET_SPEC_LESSON,
   childPeriod1Data$HOW_OFT_CH_TAKEN_TO_PERFORMANCE,
   childPeriod1Data$HOW_OFT_CH_W_DAD_OUTDOORS,
   childPeriod1Data$HOW_OFT_CH_SPEND_TIME_W_DAD
   #scale(childPeriod1Data$DO_PARS_DISCUSS_TV, center=F, scale=T),
   #scale(childPeriod1Data$MOM_HELPS_CH_W_NONE, center=F, scale=T),
   #scale(childPeriod1Data$OWN_A_HOME_COMPUTER, center=F, scale=T),
   #scale(childPeriod1Data$PARS_HELP_W_HOMEWORK, center=F, scale=T)
   )
tau3_measures <- cbind(childPeriod2Data$LOG_NONLABOR_TIME_SHARE,
   childPeriod2Data$MUSIC_INSTMT_CH,
   #scale(childPeriod2Data$HOW_MANY_BOOKS, center=F, scale=T),
   #scale(childPeriod2Data$HOW_OFT_CH_EATS_W, center=F, scale=T),
   ##scale(childPeriod2Data$MOM_HELPS_CH_LEARN_NUMBERS, center=F, scale=T),
   ##scale(childPeriod2Data$MOM_HELPS_CH_LEARN_ALPHABET, center=F, scale=T),
   ##scale(childPeriod2Data$MOM_HELPS_CH_LEARN_COLORS, center=F, scale=T),
   ##scale(childPeriod2Data$MOM_HELPS_CH_LEARN_SHAPES, center=F, scale=T),
   #scale(childPeriod2Data$HOW_OFT_CH_TAKEN_TO_MUSEUM, center=F, scale=T),
   childPeriod2Data$CH_GET_SPEC_LESSON,
   childPeriod2Data$HOW_OFT_CH_TAKEN_TO_PERFORMANCE,
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

govinvest1 <- childPeriod0Data$HOW_LONG_CHILD_WAS_IN_HEAD
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
y <- y[complete.cases(y), ]
write.csv(summary(y), "y_summary.csv")

nZ <- ncol(y)       # nZ : number of measurements

nobs <- nrow(y)     # nObs : number of observations

ninst <- Npinvest1+Npinvest2+Npinvest3+Ngovinvest1+Ngovinvest2+Ngovinvest3+Npeducation+Nincome1+Nincome2+Nincome3+Nprices1+Nprices2+Nprices3

nZinst <- nZ-ninst


##########################################################
# Set the inputs for the EM continuous function     
##########################################################      

# Initial values for M step: this is a list with (sigma, mean, prop)  
mean0 <- rbind(rep(-.00005, nZ), rep(.00005, nZ))       
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
# The first measure anchors every factor. For parental time, the first measure
# is the same non-labor time share in all three periods, so the remaining noisy
# activity measures are expressed relative to that common scale.
anchor_rows <- startSeq[1:nF]
for (i in 1:nF) freelambda[anchor_rows[i], i]             <- 1

#Configuration of constants (2 = free param, otherwise constant is set according to normalized measure in base period)
constparam <- matrix(0,nZinst,1)
meanbase<-colMeans(y,na.rm=TRUE)

#Set constants for baseline measures and age-invariant measures to true values
for (i in 1:nF) constparam[startSeq[i]:endSeq[i]] <- 2
#age invariant/normalized
# PIAT uses the existing zero-mean normalization. Parental time is anchored in
# log non-labor-share units, so its intercept is zero in all three periods.
for (i in 1:3) constparam[startSeq[i]]               <- meanbase[startSeq[1]]
for (i in 4:6) constparam[startSeq[i]]               <- 0


#remaining first period
baseline<-c(1:Nhc2, (Nhc2+Nhc3+Nhc4+1):(Nhc2+Nhc3+Nhc4+Ntau1))


for (i in (baseline)) constparam[i] <- meanbase[i]
for (i in 4:6) constparam[startSeq[i]] <- 0

freeconstant<-constparam
freeconstant[which(freeconstant!=2)]  <- 1


baselinemean<-c(1)

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
save(EMstart1, file="EMstart1.Rdata")



namef<- c("hc2", "hc3", "hc4",
          "tau1", "tau2", "tau3",
          "pinvest1", "pinvest2", "pinvest3",
          "govinvest1", "govinvest2", "govinvest3",
          "peducation",
          "income1", "income2", "income3",
          "centerprice1", "familyprice1", "centerprice2", "familyprice2",
          "centerprice3", "familyprice3")
