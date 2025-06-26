suppressMessages(library(R.utils))
suppressMessages(library(ks))
suppressMessages(library(gdata))
suppressMessages(library(corpcor))
suppressMessages(library(minpack.lm))
suppressMessages(library(Matrix))
suppressMessages(library(scales))
suppressMessages(library(openxlsx))



childPeriodData <- read.csv('C:/Users/kahna/Dropbox/OConnell 2025 Research/Fake_Merged_Data.csv', header=TRUE)
childPeriod0Data <- subset(childPeriodData, period == 0)
childPeriod1Data <- subset(childPeriodData, period == 1)
childPeriod2Data <- subset(childPeriodData, period == 2)
childPeriod3Data <- subset(childPeriodData, period == 3)


# Data
hc1  <- cbind(scale(childPeriod0Data$PIAT_MATH, center=F, scale=T), 
   scale(childPeriod0Data$PIAT_READ_REC, center=F, scale=T), 
   scale(childPeriod0Data$PIAT_READ_COMP, center=F, scale=T), 
   scale(childPeriod0Data$PPVT, center=F, scale=T))
hc2 <- cbind(scale(childPeriod1Data$PIAT_MATH, center=F, scale=T),
   scale(childPeriod1Data$PIAT_READ_REC, center=F, scale=T),
   scale(childPeriod1Data$PIAT_READ_COMP, center=F, scale=T),
   scale(childPeriod1Data$PPVT, center=F, scale=T))
hc3 <- cbind(scale(childPeriod2Data$PIAT_MATH, center=F, scale=T),
   scale(childPeriod2Data$PIAT_READ_REC, center=F, scale=T),
   scale(childPeriod2Data$PIAT_READ_COMP, center=F, scale=T),
   scale(childPeriod2Data$PPVT, center=F, scale=T))
hc4 <- cbind(scale(childPeriod3Data$PIAT_MATH, center=F, scale=T),
   scale(childPeriod3Data$PIAT_READ_REC, center=F, scale=T),
   scale(childPeriod3Data$PIAT_READ_COMP, center=F, scale=T),
   scale(childPeriod3Data$PPVT, center=F, scale=T))

pinvest1 <- cbind(scale(childPeriod0Data$HOW_OFTEN_MOM_READS, center=F, scale=T), 
   scale(childPeriod0Data$HOW_MANY_BOOKS, center=F, scale=T), 
   scale(childPeriod0Data$HOW_OFT_CH_EATS_W, center=F, scale=T), 
   scale(childPeriod0Data$MOM_HELPS_CH_LEARN_NUMBERS, center=F, scale=T), 
   scale(childPeriod0Data$MOM_HELPS_CH_LEARN_ALPHABET, center=F, scale=T), 
   scale(childPeriod0Data$MOM_HELPS_CH_LEARN_COLORS, center=F, scale=T),
   scale(childPeriod0Data$MOM_HELPS_CH_LEARN_SHAPES, center=F, scale=T),
   scale(childPeriod0Data$HOW_OFT_CH_TAKEN_TO_MUSEUM, center=F, scale=T),
   scale(childPeriod0Data$MUSIC_INSTMT_CH, center=F, scale=T),
   scale(childPeriod0Data$CH_GET_SPEC_LESSON, center=F, scale=T),
   scale(childPeriod0Data$HOW_OFT_CH_TAKEN_TO_PERFORMANCE, center=F, scale=T),
   scale(childPeriod0Data$HOW_OFT_CH_W_DAD_OUTDOORS, center=F, scale=T),
   scale(childPeriod0Data$HOW_OFT_CH_SPEND_TIME_W_DAD, center=F, scale=T),
   #scale(childPeriod0Data$DO_PARS_DISCUSS_TV, center=F, scale=T),
   #scale(childPeriod0Data$MOM_HELPS_CH_W_NONE, center=F, scale=T),
   #scale(childPeriod0Data$OWN_A_HOME_COMPUTER, center=F, scale=T),
   scale(childPeriod0Data$PARS_HELP_W_HOMEWORK, center=F, scale=T))
pinvest2 <- cbind(scale(childPeriod1Data$HOW_OFTEN_MOM_READS, center=F, scale=T),
   scale(childPeriod1Data$HOW_MANY_BOOKS, center=F, scale=T),
   scale(childPeriod1Data$HOW_OFT_CH_EATS_W, center=F, scale=T),
   scale(childPeriod1Data$MOM_HELPS_CH_LEARN_NUMBERS, center=F, scale=T),
   scale(childPeriod1Data$MOM_HELPS_CH_LEARN_ALPHABET, center=F, scale=T),
   scale(childPeriod1Data$MOM_HELPS_CH_LEARN_COLORS, center=F, scale=T),
   scale(childPeriod1Data$MOM_HELPS_CH_LEARN_SHAPES, center=F, scale=T),
   scale(childPeriod1Data$HOW_OFT_CH_TAKEN_TO_MUSEUM, center=F, scale=T),
   scale(childPeriod1Data$MUSIC_INSTMT_CH, center=F, scale=T),
   scale(childPeriod1Data$CH_GET_SPEC_LESSON, center=F, scale=T),
   scale(childPeriod1Data$HOW_OFT_CH_TAKEN_TO_PERFORMANCE, center=F, scale=T),
   scale(childPeriod1Data$HOW_OFT_CH_W_DAD_OUTDOORS, center=F, scale=T),
   scale(childPeriod1Data$HOW_OFT_CH_SPEND_TIME_W_DAD, center=F, scale=T),
   #scale(childPeriod1Data$DO_PARS_DISCUSS_TV, center=F, scale=T),
   #scale(childPeriod1Data$MOM_HELPS_CH_W_NONE, center=F, scale=T),
   #scale(childPeriod1Data$OWN_A_HOME_COMPUTER, center=F, scale=T),
   scale(childPeriod1Data$PARS_HELP_W_HOMEWORK, center=F, scale=T))
pinvest3 <- cbind(scale(childPeriod2Data$HOW_OFTEN_MOM_READS, center=F, scale=T),
   scale(childPeriod2Data$HOW_MANY_BOOKS, center=F, scale=T),
   scale(childPeriod2Data$HOW_OFT_CH_EATS_W, center=F, scale=T),
   scale(childPeriod2Data$MOM_HELPS_CH_LEARN_NUMBERS, center=F, scale=T),
   scale(childPeriod2Data$MOM_HELPS_CH_LEARN_ALPHABET, center=F, scale=T),
   scale(childPeriod2Data$MOM_HELPS_CH_LEARN_COLORS, center=F, scale=T),
   scale(childPeriod2Data$MOM_HELPS_CH_LEARN_SHAPES, center=F, scale=T),
   scale(childPeriod2Data$HOW_OFT_CH_TAKEN_TO_MUSEUM, center=F, scale=T),
   scale(childPeriod2Data$MUSIC_INSTMT_CH, center=F, scale=T),
   scale(childPeriod2Data$CH_GET_SPEC_LESSON, center=F, scale=T),
   scale(childPeriod2Data$HOW_OFT_CH_TAKEN_TO_PERFORMANCE, center=F, scale=T),
   scale(childPeriod2Data$HOW_OFT_CH_W_DAD_OUTDOORS, center=F, scale=T),
   scale(childPeriod2Data$HOW_OFT_CH_SPEND_TIME_W_DAD, center=F, scale=T),
   #scale(childPeriod2Data$DO_PARS_DISCUSS_TV, center=F, scale=T),
   #scale(childPeriod2Data$MOM_HELPS_CH_W_NONE, center=F, scale=T),
   #scale(childPeriod2Data$OWN_A_HOME_COMPUTER, center=F, scale=T),
   scale(childPeriod2Data$PARS_HELP_W_HOMEWORK, center=F, scale=T))
pinvest4 <- cbind(scale(childPeriod3Data$HOW_OFTEN_MOM_READS, center=F, scale=T),
   scale(childPeriod3Data$HOW_MANY_BOOKS, center=F, scale=T),
   scale(childPeriod3Data$HOW_OFT_CH_EATS_W, center=F, scale=T),
   scale(childPeriod3Data$MOM_HELPS_CH_LEARN_NUMBERS, center=F, scale=T),
   scale(childPeriod3Data$MOM_HELPS_CH_LEARN_ALPHABET, center=F, scale=T),
   scale(childPeriod3Data$MOM_HELPS_CH_LEARN_COLORS, center=F, scale=T),
   scale(childPeriod3Data$MOM_HELPS_CH_LEARN_SHAPES, center=F, scale=T),
   scale(childPeriod3Data$HOW_OFT_CH_TAKEN_TO_MUSEUM, center=F, scale=T),
   scale(childPeriod3Data$MUSIC_INSTMT_CH, center=F, scale=T),
   scale(childPeriod3Data$CH_GET_SPEC_LESSON, center=F, scale=T),
   scale(childPeriod3Data$HOW_OFT_CH_TAKEN_TO_PERFORMANCE, center=F, scale=T),
   scale(childPeriod3Data$HOW_OFT_CH_W_DAD_OUTDOORS, center=F, scale=T),
   scale(childPeriod3Data$HOW_OFT_CH_SPEND_TIME_W_DAD, center=F, scale=T),
   #scale(childPeriod3Data$DO_PARS_DISCUSS_TV, center=F, scale=T),
   #scale(childPeriod3Data$MOM_HELPS_CH_W_NONE, center=F, scale=T),
   #scale(childPeriod3Data$OWN_A_HOME_COMPUTER, center=F, scale=T),
   scale(childPeriod3Data$PARS_HELP_W_HOMEWORK, center=F, scale=T))

govinvest1 <- scale(childPeriod0Data$HOW_LONG_CHILD_WAS_IN_HEAD, center=F, scale=T)
govinvest2 <- scale(childPeriod1Data$TCURELSC_per_student, center=F, scale=T)
govinvest3 <- scale(childPeriod2Data$TCURELSC_per_student, center=F, scale=T)
govinvest4 <- scale(childPeriod3Data$TCURELSC_per_student, center=F, scale=T)
peducation <- scale(childPeriod0Data$HGC_OF_MOTHER_AS_OF_MAY_1_R, center=F, scale=T)

income1 <- ifelse(childPeriod0Data$TNFI_TRUNC > 0, log(childPeriod0Data$TNFI_TRUNC), NA)
income1 <- scale(income1, center=TRUE, scale=TRUE)
income2 <- ifelse(childPeriod1Data$TNFI_TRUNC > 0, log(childPeriod1Data$TNFI_TRUNC), NA)
income2 <- scale(income2, center=TRUE, scale=TRUE)
income3 <- ifelse(childPeriod2Data$TNFI_TRUNC > 0, log(childPeriod2Data$TNFI_TRUNC), NA)
income3 <- scale(income3, center=TRUE, scale=TRUE)
income4 <- ifelse(childPeriod3Data$TNFI_TRUNC > 0, log(childPeriod3Data$TNFI_TRUNC), NA)
income4 <- scale(income4, center=TRUE, scale=TRUE)

prices1 <- cbind(scale(childPeriod0Data$Weighted_Center_Price, center=T, scale=T),
   scale(childPeriod0Data$Weighted_Family_Care_Price, center=T, scale=T))
prices2 <- cbind(scale(childPeriod1Data$Weighted_Center_Price, center=T, scale=T),
   scale(childPeriod1Data$Weighted_Family_Care_Price, center=T, scale=T))
prices3 <- cbind(scale(childPeriod2Data$Weighted_Center_Price, center=T, scale=T),
   scale(childPeriod2Data$Weighted_Family_Care_Price, center=T, scale=T))
prices4 <- cbind(scale(childPeriod3Data$Weighted_Center_Price, center=T, scale=T),
   scale(childPeriod3Data$Weighted_Family_Care_Price, center=T, scale=T))


Nhc1 <- ncol(hc1)
Nhc2 <- ncol(hc2)
Nhc3 <- ncol(hc3)
Nhc4 <- ncol(hc4)
Npinvest1 <- ncol(pinvest1)
Npinvest2 <- ncol(pinvest2)
Npinvest3 <- ncol(pinvest3)
Npinvest4 <- ncol(pinvest4)
Ngovinvest1 <- ncol(govinvest1)
Ngovinvest2 <- ncol(govinvest2)
Ngovinvest3 <- ncol(govinvest3)
Ngovinvest4 <- ncol(govinvest4)
Npeducation <- ncol(peducation)
Nincome1 <- ncol(income1)
Nincome2 <- ncol(income2)
Nincome3 <- ncol(income3)
Nincome4 <- ncol(income4)
Nprices1 <- ncol(prices1)
Nprices2 <- ncol(prices2)
Nprices3 <- ncol(prices3)
Nprices4 <- ncol(prices4)


y <- data.matrix(cbind(hc1, hc2, hc3, hc4, 
   pinvest1, pinvest2, pinvest3, pinvest4,
   govinvest1, govinvest2, govinvest3, govinvest4,
   peducation,
   income1, income2, income3, income4,
   prices1, prices2, prices3, prices4))
y <- y[complete.cases(y), ]
write.csv(y, file = "y_output.csv", row.names = FALSE)

nZ <- ncol(y)       # nZ : number of measurements

nobs <- nrow(y)     # nObs : number of observations

ninst <- Nprices1+Nprices2+Nprices3+Nprices4

nZinst <- nZ-ninst
skill<-c(1)
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
# Number of skills
nS <- 1
# Number of factors 
nF <-17

# Convergence criterion            
conv <- 1e-05

# Configuration of factor loadings  (2 = free param, 1 = normalized to 1, 0 = fixed to 0) 
startSeq <- c(1,
    1+Nhc1,
    1+Nhc1 + Nhc2,
    1+Nhc1 + Nhc2 + Nhc3,
    1+Nhc1 + Nhc2 + Nhc3 + Nhc4,
    1+Nhc1 + Nhc2 + Nhc3 + Nhc4 + Npinvest1,
    1+Nhc1 + Nhc2 + Nhc3 + Nhc4 + Npinvest1 + Npinvest2,
    1+Nhc1 + Nhc2 + Nhc3 + Nhc4 + Npinvest1 + Npinvest2 + Npinvest3,
    1+Nhc1 + Nhc2 + Nhc3 + Nhc4 + Npinvest1 + Npinvest2 + Npinvest3 + Npinvest4,
    1+Nhc1 + Nhc2 + Nhc3 + Nhc4 + Npinvest1 + Npinvest2 + Npinvest3 + Npinvest4 + Ngovinvest1,
    1+Nhc1 + Nhc2 + Nhc3 + Nhc4 + Npinvest1 + Npinvest2 + Npinvest3 + Npinvest4 + Ngovinvest1 + Ngovinvest2,
    1+Nhc1 + Nhc2 + Nhc3 + Nhc4 + Npinvest1 + Npinvest2 + Npinvest3 + Npinvest4 + Ngovinvest1 + Ngovinvest2 + Ngovinvest3,
    1+Nhc1 + Nhc2 + Nhc3 + Nhc4 + Npinvest1 + Npinvest2 + Npinvest3 + Npinvest4 + Ngovinvest1 + Ngovinvest2 + Ngovinvest3 + Ngovinvest4,
    1+Nhc1 + Nhc2 + Nhc3 + Nhc4 + Npinvest1 + Npinvest2 + Npinvest3 + Npinvest4 + Ngovinvest1 + Ngovinvest2 + Ngovinvest3 + Ngovinvest4 + Npeducation,
    1+Nhc1 + Nhc2 + Nhc3 + Nhc4 + Npinvest1 + Npinvest2 + Npinvest3 + Npinvest4 + Ngovinvest1 + Ngovinvest2 + Ngovinvest3 + Ngovinvest4 + Npeducation + Nincome1,
    1+Nhc1 + Nhc2 + Nhc3 + Nhc4 + Npinvest1 + Npinvest2 + Npinvest3 + Npinvest4 + Ngovinvest1 + Ngovinvest2 + Ngovinvest3 + Ngovinvest4 + Npeducation + Nincome1 + Nincome2,
    1+Nhc1 + Nhc2 + Nhc3 + Nhc4 + Npinvest1 + Npinvest2 + Npinvest3 + Npinvest4 + Ngovinvest1 + Ngovinvest2 + Ngovinvest3 + Ngovinvest4 + Npeducation + Nincome1 + Nincome2 + Nincome3,
    1+Nhc1 + Nhc2 + Nhc3 + Nhc4 + Npinvest1 + Npinvest2 + Npinvest3 + Npinvest4 + Ngovinvest1 + Ngovinvest2 + Ngovinvest3 + Ngovinvest4 + Npeducation + Nincome1 + Nincome2 + Nincome3 + Nincome4)


endSeq <- c(startSeq[2:(nF)]-1,nZ-ninst)

freelambda <- matrix(0, nZinst, nF) 
for (i in 1:nF) freelambda[startSeq[i]:endSeq[i], i]     <- 2 
for (i in 1:nF) freelambda[startSeq[i], i]               <- 1

#Configuration of constants (2 = free param, otherwise constant is set according to normalized measure in base period)
constparam <- matrix(0,nZinst,1)
meanbase<-colMeans(y,na.rm=TRUE)

#Set constants for baseline measures and age-invariant measures to true values
for (i in 1:nF) constparam[startSeq[i]:endSeq[i]]     <- 2 
#age invariant/normalized
for (i in 1:4) constparam[startSeq[i]]               <- meanbase[startSeq[1]]
for (i in 5:8) constparam[startSeq[i]]               <- meanbase[startSeq[5]]
for (i in 9:12) constparam[startSeq[i]]               <- meanbase[startSeq[9]]
for (i in 13) constparam[startSeq[i]]               <- meanbase[startSeq[13]]
for (i in 14:17) constparam[startSeq[i]]               <- meanbase[startSeq[14]]

#remaining first period
baseline<-c(1:Nhc1,
(Nhc1+Nhc2+Nhc3+Nhc4+1):(Nhc1+Nhc2+Nhc3+Nhc4+Npinvest1),
(Nhc1+Nhc2+Nhc3+Nhc4+Npinvest1+Npinvest2+Npinvest3+Npinvest4+1):(Nhc1+Nhc2+Nhc3+Nhc4+Npinvest1+Npinvest2+Npinvest3+Npinvest4+Ngovinvest1),
(Nhc1+Nhc2+Nhc3+Nhc4+Npinvest1+Npinvest2+Npinvest3+Npinvest4+Ngovinvest1+Ngovinvest2+Ngovinvest3+Ngovinvest4+1):(Nhc1+Nhc2+Nhc3+Nhc4+Npinvest1+Npinvest2+Npinvest3+Npinvest4+Ngovinvest1+Ngovinvest2+Ngovinvest3+Ngovinvest4+Npeducation),
(Nhc1+Nhc2+Nhc3+Nhc4+Npinvest1+Npinvest2+Npinvest3+Npinvest4+Ngovinvest1+Ngovinvest2+Ngovinvest3+Ngovinvest4+Npeducation+1):(Nhc1+Nhc2+Nhc3+Nhc4+Npinvest1+Npinvest2+Npinvest3+Npinvest4+Ngovinvest1+Ngovinvest2+Ngovinvest3+Ngovinvest4+Npeducation+Nincome1+Nincome2+Nincome3+Nincome4))


for (i in (baseline)) constparam[i] <- meanbase[i]

freeconstant<-constparam
freeconstant[which(freeconstant!=2)]  <- 1

#lambda baseline
baselinelambda<-c(1, 5, 9, 13, 14)
baselinemean<-c(1, 5, 9)

#Construct matrix to pull values for income covariances from EM to add to LS step

instcovpull <- c(1,
    1+Nhc1,
    1+Nhc1 + Nhc2,
    1+Nhc1 + Nhc2 + Nhc3,
    1+Nhc1 + Nhc2 + Nhc3 + Nhc4,
    1+Nhc1 + Nhc2 + Nhc3 + Nhc4 + Npinvest1,
    1+Nhc1 + Nhc2 + Nhc3 + Nhc4 + Npinvest1 + Npinvest2,
    1+Nhc1 + Nhc2 + Nhc3 + Nhc4 + Npinvest1 + Npinvest2 + Npinvest3,
    1+Nhc1 + Nhc2 + Nhc3 + Nhc4 + Npinvest1 + Npinvest2 + Npinvest3 + Npinvest4,
    1+Nhc1 + Nhc2 + Nhc3 + Nhc4 + Npinvest1 + Npinvest2 + Npinvest3 + Npinvest4 + Ngovinvest1,
    1+Nhc1 + Nhc2 + Nhc3 + Nhc4 + Npinvest1 + Npinvest2 + Npinvest3 + Npinvest4 + Ngovinvest1 + Ngovinvest2,
    1+Nhc1 + Nhc2 + Nhc3 + Nhc4 + Npinvest1 + Npinvest2 + Npinvest3 + Npinvest4 + Ngovinvest1 + Ngovinvest2 + Ngovinvest3,
    1+Nhc1 + Nhc2 + Nhc3 + Nhc4 + Npinvest1 + Npinvest2 + Npinvest3 + Npinvest4 + Ngovinvest1 + Ngovinvest2 + Ngovinvest3 + Ngovinvest4,
    1+Nhc1 + Nhc2 + Nhc3 + Nhc4 + Npinvest1 + Npinvest2 + Npinvest3 + Npinvest4 + Ngovinvest1 + Ngovinvest2 + Ngovinvest3 + Ngovinvest4 + Npeducation,
    1+Nhc1 + Nhc2 + Nhc3 + Nhc4 + Npinvest1 + Npinvest2 + Npinvest3 + Npinvest4 + Ngovinvest1 + Ngovinvest2 + Ngovinvest3 + Ngovinvest4 + Npeducation + Nincome1,
    1+Nhc1 + Nhc2 + Nhc3 + Nhc4 + Npinvest1 + Npinvest2 + Npinvest3 + Npinvest4 + Ngovinvest1 + Ngovinvest2 + Ngovinvest3 + Ngovinvest4 + Npeducation + Nincome1 + Nincome2,
    1+Nhc1 + Nhc2 + Nhc3 + Nhc4 + Npinvest1 + Npinvest2 + Npinvest3 + Npinvest4 + Ngovinvest1 + Ngovinvest2 + Ngovinvest3 + Ngovinvest4 + Npeducation + Nincome1 + Nincome2 + Nincome3,
    (1+Nhc1 + Nhc2 + Nhc3 + Nhc4 + Npinvest1 + Npinvest2 + Npinvest3 + Npinvest4 + Ngovinvest1 + Ngovinvest2 + Ngovinvest3 + Ngovinvest4 + Npeducation + Nincome1 + Nincome2 + Nincome3 + Nincome4):nZ)

# Initial values of parameters for the minimum distance step (if choose measures for latent factors normalized to 1, don't have to divide by lambda^2)        
param.start <- c(rep(1,nZinst), rep(1, nM*0.5*nF*(nF+1)), rep(1,nF*2-length(baselinemean)), rep(1, length(which(freelambda==2))), rep(1, length(which(freeconstant==2))) ) 

#Reset directory and run the estimation algorithm
dir<-("C:/Users/kahna/Downloads/rdaa026_supplementary_data/Supplementary/Datafortheweb/")
setwd(dir)
source("OurCode/OurR_estim_meas_model5rev.R")

result <- estim.meas.model(y, nM, nF, freelambda, mstep.start, param.start, conv, freeconstant, constparam, baseline, baselinelambda, baselinemean)
# Extract specific elements from the result
out <- result[[1]]
est <- result[[2]]
prob <- result[[2]]$prob
eps <- result[[2]]$eps
cov <- result[[2]]$cov
mean <- result[[2]]$mean
lambda <- result[[2]]$lambda
constant <- result[[2]]$constant

#Save everything to different sheets in an Excel file
print(out)

# Source the Excel export function and save results
source("C:/Users/kahna/Downloads/rdaa026_supplementary_data/Supplementary/Datafortheweb/OurCode/save_results_to_excel.R")
save_estimation_results(out, est, "estimation_results.xlsx")
