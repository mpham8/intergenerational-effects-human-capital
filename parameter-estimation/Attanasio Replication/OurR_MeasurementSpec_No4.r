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
childPeriodData <- read.csv('Fake_Merged_Data.csv', header=TRUE)

#Normalize the data
childPeriodData <- transform(childPeriodData,
    PIAT_MATH = scale(PIAT_MATH, center=F, scale=T),
    PIAT_READ_REC = scale(PIAT_READ_REC, center=F, scale=T),
    PIAT_READ_COMP = scale(PIAT_READ_COMP, center=F, scale=T),
    PPVT = scale(PPVT, center=F, scale=T),
    HOW_OFTEN_MOM_READS = scale(HOW_OFTEN_MOM_READS, center=F, scale=T),
    MUSIC_INSTMT_CH = scale(MUSIC_INSTMT_CH, center=F, scale=T),
    CH_GET_SPEC_LESSON = scale(CH_GET_SPEC_LESSON, center=F, scale=T),
    HOW_OFT_CH_TAKEN_TO_PERFORMANCE = scale(HOW_OFT_CH_TAKEN_TO_PERFORMANCE, center=F, scale=T),
    HOW_OFT_CH_W_DAD_OUTDOORS = scale(HOW_OFT_CH_W_DAD_OUTDOORS, center=F, scale=T),
    HOW_OFT_CH_SPEND_TIME_W_DAD = scale(HOW_OFT_CH_SPEND_TIME_W_DAD, center=F, scale=T),
    HOW_LONG_CHILD_WAS_IN_HEAD = scale(HOW_LONG_CHILD_WAS_IN_HEAD, center=T, scale=T),
    TCURELSC_per_student = scale(ifelse(TCURELSC_per_student > 0, log(TCURELSC_per_student), NA), center=T, scale=T),
    HGC_OF_MOTHER_AS_OF_MAY_1_R = scale(HGC_OF_MOTHER_AS_OF_MAY_1_R, center=T, scale=T),
    TNFI_TRUNC = scale(ifelse(TNFI_TRUNC > 0, log(TNFI_TRUNC), NA), center=T, scale=T),
    Center_Care_Price = scale(Center_Care_Price, center=T, scale=T),
    Family_Care_Price = scale(Family_Care_Price, center=T, scale=T)
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


pinvest1 <- cbind(childPeriod0Data$HOW_OFTEN_MOM_READS, 
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
pinvest2 <- cbind(childPeriod1Data$HOW_OFTEN_MOM_READS,
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
pinvest3 <- cbind(##scale(childPeriod2Data$HOW_OFTEN_MOM_READS, center=F, scale=T),
   #scale(childPeriod2Data$HOW_MANY_BOOKS, center=F, scale=T),
   #scale(childPeriod2Data$HOW_OFT_CH_EATS_W, center=F, scale=T),
   ##scale(childPeriod2Data$MOM_HELPS_CH_LEARN_NUMBERS, center=F, scale=T),
   ##scale(childPeriod2Data$MOM_HELPS_CH_LEARN_ALPHABET, center=F, scale=T),
   ##scale(childPeriod2Data$MOM_HELPS_CH_LEARN_COLORS, center=F, scale=T),
   ##scale(childPeriod2Data$MOM_HELPS_CH_LEARN_SHAPES, center=F, scale=T),
   #scale(childPeriod2Data$HOW_OFT_CH_TAKEN_TO_MUSEUM, center=F, scale=T),
   childPeriod2Data$MUSIC_INSTMT_CH,
   childPeriod2Data$CH_GET_SPEC_LESSON,
   childPeriod2Data$HOW_OFT_CH_TAKEN_TO_PERFORMANCE,
   childPeriod2Data$HOW_OFT_CH_W_DAD_OUTDOORS,
   childPeriod2Data$HOW_OFT_CH_SPEND_TIME_W_DAD
   #scale(childPeriod2Data$DO_PARS_DISCUSS_TV, center=F, scale=T),
   #scale(childPeriod2Data$MOM_HELPS_CH_W_NONE, center=F, scale=T),
   #scale(childPeriod2Data$OWN_A_HOME_COMPUTER, center=F, scale=T),
   #scale(childPeriod2Data$PARS_HELP_W_HOMEWORK, center=F, scale=T)
   )


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

Nhc2 <- ncol(hc2)
Nhc3 <- ncol(hc3)
Nhc4 <- ncol(hc4)
Npinvest1 <- ncol(pinvest1)
Npinvest2 <- ncol(pinvest2)
Npinvest3 <- ncol(pinvest3)
Ngovinvest1 <- ncol(govinvest1)
Ngovinvest2 <- ncol(govinvest2)
Ngovinvest3 <- ncol(govinvest3)
Npeducation <- ncol(peducation)
Nincome1 <- ncol(income1)
Nincome2 <- ncol(income2)
Nincome3 <- ncol(income3)
Nprices1 <- ncol(prices1)
Nprices2 <- ncol(prices2)
Nprices3 <- ncol(prices3)


y <- data.matrix(cbind(hc2, hc3, hc4,
   pinvest1, pinvest2, pinvest3,
   govinvest1, govinvest2, govinvest3,
   peducation,
   income1, income2, income3,
   prices1, prices2, prices3))
y <- y[complete.cases(y), ]
write.csv(summary(y), "y_summary.csv")

nZ <- ncol(y)       # nZ : number of measurements

nobs <- nrow(y)     # nObs : number of observations

ninst <- Ngovinvest1+Ngovinvest2+Ngovinvest3+Npeducation+Nincome1+Nincome2+Nincome3+Nprices1+Nprices2+Nprices3

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
            1+Nhc2 + Nhc3 + Nhc4 + Npinvest1,
            1+Nhc2 + Nhc3 + Nhc4 + Npinvest1 + Npinvest2,
            1+Nhc2 + Nhc3 + Nhc4 + Npinvest1 + Npinvest2 + Npinvest3)


endSeq <- c(startSeq[2:(nF)]-1,nZ-ninst)

freelambda <- matrix(0, nZinst, nF) 
for (i in 1:nF) freelambda[startSeq[i]:endSeq[i], i]     <- 2 
for (i in 1:nF) freelambda[startSeq[i], i]               <- 1

#Configuration of constants (2 = free param, otherwise constant is set according to normalized measure in base period)
constparam <- matrix(0,nZinst,1)
meanbase<-colMeans(y,na.rm=TRUE)

#Set constants for baseline measures and age-invariant measures to true values
for (i in 1:nF) constparam[startSeq[i]:endSeq[i]] <- 2
#age invariant/normalized
#the intercept for the first measurement of each factor is set to the mean of the first measurement in the first period
for (i in 1:3) constparam[startSeq[i]]               <- meanbase[startSeq[1]]
for (i in 4:6) constparam[startSeq[i]]               <- meanbase[startSeq[4]]


#remaining first period
baseline<-c(1:Nhc2, (Nhc2+Nhc3+Nhc4+1):(Nhc2+Nhc3+Nhc4+Npinvest1))


for (i in (baseline)) constparam[i] <- meanbase[i]

freeconstant<-constparam
freeconstant[which(freeconstant!=2)]  <- 1


baselinemean<-c(1,4)

#Construct matrix to pull values for income covariances from EM to add to LS step

instcovpull <- c(1,
            1+Nhc2,
            1+Nhc2 + Nhc3,
            1+Nhc2 + Nhc3 + Nhc4,
            1+Nhc2 + Nhc3 + Nhc4 + Npinvest1,
            1+Nhc2 + Nhc3 + Nhc4 + Npinvest1 + Npinvest2,
            (1+Nhc2 + Nhc3 + Nhc4 + Npinvest1 + Npinvest2 + Npinvest3):nZ)

# Initial values of parameters for the minimum distance step (if choose measures for latent factors normalized to 1, don't have to divide by lambda^2)        
param.start <- c(rep(1,nZinst), rep(1, nM*0.5*nF*(nF+1)), rep(1,nF*2-length(baselinemean)), rep(1, length(which(freelambda==2))), rep(1, length(which(freeconstant==2))) ) 

EMstart1 <- list(mstep.start, param.start, y, nM, nF, freelambda, constparam, instcovpull, ninst, nobs, conv)
save(EMstart1, file="EMstart1.Rdata")



namef<- c("hc2", "hc3", "hc4",
          "pinvest1", "pinvest2", "pinvest3",
          "govinvest1", "govinvest2", "govinvest3",
          "peducation",
          "income1", "income2", "income3",
          "centerprice1", "familyprice1", "centerprice2", "familyprice2",
          "centerprice3", "familyprice3")  