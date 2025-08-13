rm(list=ls())

dir<-('C:/Users/kahna/Dropbox/OConnell 2025 Research/Attanasio Replication/')

dir_data           <- ('C:/Users/kahna/Dropbox/OConnell 2025 Research/')
dir_output <- paste(dir, c("Output"), sep="")



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

bsample <- 2 # number of bootstrap samples
Bootstrap          <- 1                   # 1 if run the bootstrap; 0 otherwise  
onlyboot           <- 0                   # 1 if we only want to perform the bootstrap; 0 if we want to estimate the model on true data and bootstrap data

setwd(dir)
source ("Our_Estimate_FactorModel.R") # This file estimates the joint distribution of factors. In addition, it also estimates the distribution for each bootstrap iteration.

#Estimate production function parameters using only prices as instruments, income is in the production function
setwd(dir)
source("Our_Production_Functions_No4_Incomeinst.R")