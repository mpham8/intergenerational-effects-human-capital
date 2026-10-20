rm(list=ls())

command_args <- commandArgs(trailingOnly = FALSE)
file_argument <- command_args[grepl("^--file=", command_args)]
configured_script_dir <- Sys.getenv("IEHC_ATTANASIO_SCRIPT_DIR", unset = "")
if (nzchar(configured_script_dir)) {
  dir <- normalizePath(configured_script_dir, mustWork = TRUE)
} else if (length(file_argument) == 1) {
  master_file <- gsub("~\\+~", " ", sub("^--file=", "", file_argument))
  dir <- dirname(normalizePath(master_file, mustWork = TRUE))
} else {
  dir <- normalizePath(getwd(), mustWork = TRUE)
}

# Default to the bundled fake data; the VDE input remains an environment override.
default_data_file <- normalizePath(file.path(
  dir, "..", "..", "data-preprocessing", "Fake Merged Data",
  "Fake_Merged_Data_with_cex_education_expenditure.csv"
), mustWork = FALSE)
data_file <- Sys.getenv("IEHC_NLSY_DATA_FILE", unset = default_data_file)
dir_output <- Sys.getenv(
  "IEHC_ATTANASIO_OUTPUT_DIR",
  unset = file.path(dir, "Our Most Recent Output")
)
data_file <- normalizePath(path.expand(data_file), mustWork = TRUE)
dir_output <- path.expand(dir_output)
dir_data <- dirname(data_file)

if (!file.exists(data_file)) {
  stop(paste("IEHC_NLSY_DATA_FILE does not exist:", data_file))
}
dir.create(dir_output, recursive = TRUE, showWarnings = FALSE)
dir_output <- normalizePath(dir_output, mustWork = TRUE)



# Specification of the measurement systems, the setup for the EM algorithm

nameFM             <- c("OurR_MeasurementSpec_No4")                    # Name of the specification 

inputFM            <- paste(nameFM, ".r", sep="")   # Name of the file with all the details of the spec

#You should add the following packages to your R 
suppressMessages(library(R.utils))
suppressMessages(library(ks))
suppressMessages(library(gdata))
suppressMessages(library(corpcor))
suppressMessages(library(minpack.lm))
suppressMessages(library(Matrix))
#suppressMessages(library(R.oo))
#suppressMessages(library(micEconCES))
#suppressMessages(library(stargazer))
#suppressMessages(library(lmtest))
#suppressMessages(library(GenSA))
#suppressMessages(library(foreach))
#suppressMessages(library(mixtools))
#suppressMessages(library(mvtnorm))
suppressMessages(library(foreign))

setwd(dir)
# Load details of the empirical specification: 
source(inputFM)                             # file specifying the measurement system 

cor_matrix <- cor(y, use = "complete.obs")
print("Correlations close to 1 or -1:")
print(which(abs(cor_matrix) > 0.95 & cor_matrix != 1, arr.ind = TRUE))

cat("Number of complete observations:", sum(complete.cases(y)), "\n")
cat("Number of variables:", ncol(y), "\n")
cat("Ratio:", sum(complete.cases(y)) / ncol(y), "\n")


setwd(dir)
# Function for simulating data based on the results of the EM algorithm, used to find parameter estimates
source("Our_Procedures.R")
# Function for the EM algorithm
source("Our_estim_meas_model.R")


# Specify values for bootstrap:

bsample <- as.integer(Sys.getenv("IEHC_BOOTSTRAP_SAMPLES", unset = "2"))
Bootstrap          <- as.integer(Sys.getenv("IEHC_RUN_BOOTSTRAP", unset = "1"))
if (!Bootstrap %in% 0:1) stop("IEHC_RUN_BOOTSTRAP must be 0 or 1.")
if (Bootstrap == 1L && (is.na(bsample) || bsample < 2L)) stop("At least two bootstrap samples are required.")
onlyboot           <- 0                   # 1 if we only want to perform the bootstrap; 0 if we want to estimate the model on true data and bootstrap data

setwd(dir)
source ("Our_Estimate_FactorModel.R") # This file estimates the joint distribution of factors. In addition, it also estimates the distribution for each bootstrap iteration.

#Estimate production function parameters using only prices as instruments, income is in the production function
setwd(dir)
source("Our_Production_Functions_No4_Incomeinst.r")
