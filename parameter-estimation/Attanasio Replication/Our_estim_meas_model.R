# estim.meas.model_func.R
# Function that estimates the measurement model (using EM algorithm and minimum distance estimator)
# Authors: Orazio Attanasio, Costas Meghir, and Emily Nix

#library("corpcor")
library("Matrix")
library("gdata")
###############################################################
############################################################### 
# DEFINE INPUTS OF THE FUNCTION 
############################################################### 

# y                 Data 
# nM                Number of mixtures 
# nF                Number of factors (including all periods) 
# nI                Number of instruments 
# freelambda        Matrix indicating normalized and free factor loadings (1=normalized, 2=free)
# mstep.start       Starting values for EM algorithm (M-step in particular) 
# param.start       Starting values for minimum distance estimator 
# conv              Convergence criterion for EM algorithm 


###############################################################
# DEFINITION OF FUNCTION STARTS HERE
############################################################### 
# Use exactly the same factor-mean restriction when fitting and exporting.
# Only explicitly normalized factors have weighted mean zero; log activity factors
# retain their fitted levels. Matrix dimensions also work with several factors.
reconstruct_factor_means <- function(free_means, probabilities, nF, baselinemean) {
  nM <- length(probabilities)
  stopifnot(nM >= 2, all(is.finite(probabilities)), all(probabilities > 0),
            abs(sum(probabilities) - 1) < 1e-8,
            all(baselinemean %in% seq_len(nF)))
  fixed <- matrix(FALSE, nM, nF)
  fixed[nM, baselinemean] <- TRUE
  stopifnot(length(free_means) == sum(!fixed))
  means <- matrix(0, nM, nF)
  means[!fixed] <- free_means
  if (length(baselinemean)) {
    means[nM, baselinemean] <- -colSums(sweep(
      means[seq_len(nM - 1), baselinemean, drop=FALSE], 1,
      probabilities[seq_len(nM - 1)], "*")) / probabilities[nM]
  }
  means
}

estim.meas.model <- function(y, nM, nF, freelambda, mstep.start, param.start, conv, freeconstant, constparam, baseline, baselinelambda, baselinemean, preserve_mean_indices=integer(0), loading_ties=list(), constant_ties=list()){

  
###############################################################
###############################################################
# DEFINE PARAMETERS THAT WILL BE USED THROUGHOUT 
# nZ                Number of measurements
nZ <- ncol(y)       

# nobs              Number of observations in dataset y
nobs <- nrow(y)     

###############################################################
###############################################################
# DEFINE SUBFUNCTIONS
  # scanNA 
  # estepfn 
  # mstepfn

###############################################################
# Function to scan missing values 
###############################################################
scanNA <- function(x) c(which(complete.cases(x)==TRUE))


###############################################################
# ESTEP FUNCTION 
###############################################################
estepfn<-function(mstep, y){
mean      <- mstep[[1]]  
sigma     <- mstep[[2]]
prop      <- mstep[[3]]
 
probi <- matrix(0, nobs, nM)   # Probability of being in each mixture 

invsigma_noNA  <- list()       # Compute these objects for people with complete data 

#for (m in 1:nM) invsigma_noNA[[m]] <- solve(sigma[[m]])

invsigma_noNA  <- list()
for (m in 1:nM) invsigma_noNA[[m]] <- solve(sigma[[m]])

Q_noNA <- list()
for (m in 1:nM) Q_noNA[[m]] <- (1/(((2*pi)^(nZ/2))*((det(sigma[[m]]))^.5)))

# Loop over people
for (i in 1:nobs){
for (m in 1:nM){
    
  # If they have zero NA: 
  if (length(comp[[i]])==nZ){
  probi[i,m]  <-  prop[m]*Q_noNA[[m]]*exp(-.5*t(y[i,1:nZ]- mean[m,])%*%invsigma_noNA[[m]]%*%(y[i,1:nZ]-mean[m,]))
      
  # If they have some NA, do the step only over the non-missing values - need to adjust the variances and means   
   
  } else if (length(comp[[i]])<nZ){ 
    Q <- ((1/(2*pi))^(length(comp[[i]])/2))*((1/det(make.positive.definite(sigma[[m]][comp[[i]],comp[[i]], drop=FALSE])))^.5)
    invsigma <- solve(make.positive.definite(sigma[[m]][comp[[i]],comp[[i]], drop=FALSE]))
    probi[i,m]  <-  prop[m]*Q*exp(-.5*t(y[i,comp[[i]]] - mean[m,comp[[i]]])%*%invsigma%*%(y[i,comp[[i]]] - mean[m,comp[[i]]]))
  }
  } 
} 

# Calculate probability of each observation in each mixture 
probi[is.na(probi)]    <-0	#replace NAs with zeros. NAs occur if the pdf is zero

probiden               <-rowSums(probi*1)
prob                   <-(probi*1)/(probiden)

prob[is.na(prob)]      <-.5    #replace NAs with zeros. NAs occur if both pdfs are zero

estep                  <- prob


loglikt                 <- (log(rowSums(probi)))
loglikt[is.infinite(loglikt)] = -700
loglik                 <- sum(loglikt)

return(list(estep, loglik))
}


###############################################################
# MSTEP FUNCTION 
############################################################### 
mstepfn<- function(estep,y) {
prob     <- estep[[1]]

test1=colSums(estep[[1]])
test1=abs(test1[1]-test1[2])
if (test1>.99*nobs) {
print("invoked")
mean0 <- rbind(rep(-.00005, nZ), rep(.00005, nZ))       
cov.start <- make.positive.definite(cov(y, use ="complete.obs"))
sigma0 <- list(cov.start, cov.start)
prop0 <- c(.5,.5)
mstep <- list(mean0, sigma0, prop0)} else {
# Update means 
meani     <-matrix(0, nM, nZ)
mean      <- matrix(0, nM, nZ)
propimean <-matrix(0,nM,nZ)

for (m in 1:nM){ 
  for (i in 1:nobs){
    if (length(comp[[i]])==nZ){   
      meani[m,] <- meani[m,] + prob[i,m]*(y[i,1:nZ])  
	   propimean[m,] <- propimean[m,]+prob[i,m]  
    } else if (length(comp[[i]])<nZ){ 
      temp <- matrix(0, nM, nZ)
	temp2 <- matrix(0, nM, nZ)
      temp[m,comp[[i]]] <- prob[i,m]*(y[i,comp[[i]]])
      meani[m,] <- meani[m,] + temp[m,]
	temp2[m,comp[[i]]] <- prob[i,m]
	propimean[m,] <- propimean[m,]+temp2[m,]
	}
  mean[m,] <- meani[m,]/propimean[m,]
  } 
} 

# Update variance covariance matrix   
sigmai <- list()
sigma  <- list()

for (m in 1:nM){
sigmai[[m]] <- mat.or.vec(nZ, nZ)

  for (i in 1:nobs){
    if (length(comp[[i]])==nZ){  
     
      sigmai[[m]]<-sigmai[[m]] + prob[i,m]*((y[i,1:nZ]-mean[m,])%*%t(y[i,1:nZ]-mean[m,]))
  
    } else if (length(comp[[i]])<nZ){ 
      temp <- matrix(0,nZ,nZ)
      temp[comp[[i]], comp[[i]]] <- prob[i,m]*((y[i,comp[[i]]]-mean[m,comp[[i]]])%*%t(y[i,comp[[i]]]-mean[m,comp[[i]]]))
      sigmai[[m]] <- sigmai[[m]] + temp 
    } 


sigma[[m]] <- sigmai[[m]]/propimean[m,]
#write.csv(propimean[m,], file = paste0("propimean_", m, ".csv"), row.names = FALSE)
#write.csv(sigma[[m]], file = paste0("sigma_", m, ".csv"), row.names = FALSE)
}  
}   

#To deal with the positive definite issues
for (m in 1:nM) {
sigma[[m]]<-make.positive.definite(sigma[[m]])
}


#To deal with symmetric issues
for (m in 1:nM) {
sigma[[m]]<-as.matrix(forceSymmetric(sigma[[m]]))
}

# Update weight 
prop <- colSums(prob)/sum(colSums(prob))

mstep<-list(mean, sigma, prop)
}
return(mstep)
}

###############################################################
###############################################################
# IMPLEMENT E-M ALGORITHM 

###############################################################
# 1.1. Scan missing values 
###############################################################
# Keep one index vector per household, even when all rows have the same NA pattern.
comp <- lapply(seq_len(nrow(y)), function(i) which(!is.na(y[i, ])))

###############################################################
# 1.2. Initialize loop with mstep.start values 
###############################################################
estep     <- estepfn(mstep.start,y)
mstep     <- mstepfn(estep,y)
loglik    <- estep[[2]]

estep     <- estepfn(mstep,y)
mstep     <- mstepfn(estep,y)
loglik    <- rbind(loglik, estep[[2]])

iter      <- 1
###############################################################
# 1.3. Loop over the E and M steps until convergence 
###############################################################
while (abs((loglik[iter+1]-loglik[iter])/loglik[iter])>conv){
iter      <- iter + 1
estep     <- estepfn(mstep,y)
mstep     <- mstepfn(estep,y)
loglik    <- rbind(loglik, estep[[2]])
print(paste("Iteration:", iter,sep=" ")) 
print(paste("Convergence criterion:", abs(loglik[iter+1]-loglik[iter])/loglik[iter], sep=" "))
} 

###############################################################
###############################################################
# IMPLEMENT MINIMUM DISTANCE ESTIMATOR TO ESTIMATE 
# PARAMETERS OF THE FACTOR JOINT DISTRIBUTION
# FACTOR LOADINGS AND VARIANCE OF MEASUREMENT ERROR 


##############################################################
# 2.1. Define objects to fill in the "param" vector properly 
##############################################################
# Param is the vector of parameter solution of the min dist estimator 
# It contains the following (in this order)
  # Variance and covariance of the measurement error terms 
  # 1/2 * nF*(nF+1)*nM elements of the variance-covariance matrixes of the mixtures 
  # nF*(nM-1) elements of the vectors of means for the nM-1 first mixtures 
  # Factor loadings of the measurement system 

nZinst <- nZ-ninst
startMean <- nZinst + nM*0.5*nF*(nF+1) + 1
endMean   <- startMean - 1 + nF *(nM)-length(baselinemean)

nLambda   <- length(which(freelambda==2))
  
startLambda <- endMean + 1 
endLambda   <- startLambda - 1 + nLambda
lambdalong  <- rep(0, nZinst*nF)
lambdalong[which(freelambda==1)]  <- 1 

nConst   <- length(which(freeconstant==2))
startConst <- endLambda+1
endConst   <- startConst - 1 + nConst



##############################################################
# 2.2. Define function to optimize over 
##############################################################
mindistance      <-function(param) {

# 2.2.1 Fill in various objects with elements of param  
##############################################################

# Variances of the uniquenesses 
eps_parvec       <- param[1:nZinst]

# Variance-covariance matrices of the nM mixtures 
Lcovfactor_par    <- list()
covfactor_par    <- list()
for (m in 1:nM){
Lcovfactor_par[[m]] <- matrix(0, nF, nF)
lowerTriangle(Lcovfactor_par[[m]], diag=TRUE) <- param[(nZinst+1 + (m-1)*0.5*nF*(nF+1)):(nZinst + m*0.5*nF*(nF+1))]
covfactor_par[[m]] <- Lcovfactor_par[[m]] %*% t(Lcovfactor_par[[m]])
}

# Means of the nM-1 mixtures 


meanfactor_par <- reconstruct_factor_means(
  param[startMean:endMean], mstep[[3]], nF, baselinemean)

# Factor loadings : this is tricky because we want to impose certain restrictions on the matrix of factor laodings 
# To do this simply, we have a matrix that indicates 1 if the parameter is fixed to 1, 2 if the parameter is to be estimated, 0 otherwise

lambdalong[which(freelambda==2)]  <- param[startLambda:endLambda]
lambda_par  <- matrix(lambdalong, nZinst, nF)
for (tie in loading_ties) lambda_par[tie[3], tie[4]] <- lambda_par[tie[1], tie[2]]


# Constants : this is also tricky because we want to impose certain restrictions on the vector of constants 
#- baseline constants are equal to the mean of the measure normalized to 1, and later age invariant constants are imposed for the same measures
# To do this simply, we have a matrix that indicates the fixed constant where applicable and 2 if the parameter is to be estimated
const_par <- constparam
const_par[which(freeconstant==2)]  <- param[startConst:endConst]
for (tie in constant_ties) const_par[tie[2]] <- const_par[tie[1]]


############################################################## 
# 2.2.2 Extract output of the EM alrogithm and compute estimated 
# means and variance-covariances 
############################################################## 
mean_hat1      <- mstep[[1]]
cov_hat1       <- mstep[[2]]
prop_hat       <- mstep[[3]]

mean_hat      <- mean_hat1[,1:nZinst]
cov_hat	  <- list()
for (m in 1:nM){
cov_hat1a	  <- cov_hat1[[m]]
cov_hat[[m]]  <- cov_hat1a[1:nZinst,1:nZinst]
}

# Measurement means implied by the same factors that will be exported.
mean_par      <- matrix(0, nM, nZinst)
for (m in 1:(nM)){ 
mean_par[m,] <- prop_hat[m]*const_par + prop_hat[m]*(lambda_par %*% as.matrix(meanfactor_par[m,]))
}





# Squared distance between output of EM and functional form 
mean  <- sum((mean_hat[1,]-(mean_par[1,]/prop_hat[1]))^2)
for (m in 2:(nM)){ 
mean  <- mean + sum((mean_hat[m,]-(mean_par[m,]/prop_hat[m]))^2)
} 


# Variance-covariance 
eps_par  <-diag(eps_parvec)
cov_par  <- list()
for (m in 1:nM){
cov_par[[m]] <-lambda_par%*%covfactor_par[[m]]%*%t(lambda_par) + eps_par
} 

cov     <- sum(sum((cov_hat[[1]]-cov_par[[1]])^2))
for (m in 2:nM){
cov    <- cov + sum(sum((cov_hat[[m]]-cov_par[[m]])^2))
}

# Sum of least squares 
sos<-mean+cov
return(sos)
}


##############################################################
# 3. Perform minimum distance estimator  
##############################################################

lower_bounds <- c(rep(0, nZinst), rep(-Inf, length(param.start)-nZinst))
# Fix the bridge orientation and avoid division by a zero proxy loading.
for (tie in loading_ties) {
  position <- match(tie[1] + (tie[2]-1L)*nZinst, which(freelambda==2))
  lower_bounds[startLambda + position - 1L] <- 1e-6
}
out <- optim(param.start,mindistance, method=c("L-BFGS-B"), control = list(maxit = 20000),
             lower=lower_bounds, upper=rep(Inf, length(param.start)))
if (out$convergence != 0L || !is.finite(out$value)) stop("Measurement minimum-distance fit did not converge; no factors exported.")

##############################################################
# 4. Organize output   
##############################################################
# Organize the output so that each component is easily isolated
  eps_est          <-out$par[1:nZinst]
  
  Lcovfactor_est    <- list()
  covfactor_est     <- list()
  for (m in 1:nM){
    Lcovfactor_est[[m]] <- matrix(0, nF, nF)
    lowerTriangle(Lcovfactor_est[[m]], diag=TRUE) <- out$par[(nZinst+1 + (m-1)*0.5*nF*(nF+1)):(nZinst + m*0.5*nF*(nF+1))]
    covfactor_est[[m]] <- Lcovfactor_est[[m]] %*% t(Lcovfactor_est[[m]])
  }  
  
  mean_est <- out$par[startMean:endMean]

  constant_est <- constparam
  constant_est[which(freeconstant==2)]  <- out$par[startConst:endConst]
  for (tie in constant_ties) constant_est[tie[2]] <- constant_est[tie[1]]

  lambdalong[which(freelambda==2)]  <- out$par[startLambda:endLambda]
  lambda_est  <- matrix(lambdalong, nZinst, nF)
  for (tie in loading_ties) lambda_est[tie[3], tie[4]] <- lambda_est[tie[1], tie[2]]
  
  meanall_est1 <- mstep[[1]]
  covinst_hat1 <- mstep[[2]] 
  prop_hat       <- mstep[[3]]
  
  meanfactor_est <- reconstruct_factor_means(mean_est, prop_hat, nF, baselinemean)

  meanall_est <- matrix(0,nM,nF+ninst)
  for (m in 1:nM){
  meanall_est[m,1:nF]  <- meanfactor_est[m,1:nF]
  meanall_est[m,(nF+1):(nF+ninst)] <- meanall_est1[m,(nZ-ninst+1):nZ]
  }
  # Log-dollar inputs have identified levels; centering them destroys their units.
  # Retain the existing restrictions only for the other observed controls.
  for (j in setdiff((nF+1):(nF+ninst), preserve_mean_indices)){
  meanall_est[nM,j]  <- - 1 * sum(prop_hat[1:(nM-1)] * meanall_est[1:(nM-1),j])/prop_hat[nM]
  }


  covall_est<-list()
  covall_est2 <- matrix(0,(nF+ninst),(nF+ninst))
  for (m in 1:nM){
  covall_est1a <- covinst_hat1[[m]]
  covall_est1b <- covfactor_est[[m]]
  covall_est2[1:nF,1:nF] <- covall_est1b
  for (z in 1:(nF+ninst)) {
  for (i in 1:ninst){
  # The period-3 proxy now has an estimated, not unit, loading.
  proxy_loading <- if (z <= nF) lambda_est[instcovpull[z], z] else 1
  covall_est2[(nF+i),z] <- covall_est1a[(nZ-ninst+i),(instcovpull[z])] / proxy_loading
  covall_est2[z,(nF+i)] <- covall_est2[(nF+i),z]
  }
  }
  covall_est[[m]]  <- covall_est2
  }
  
##############################################################
# 5. Group output to save it
##############################################################
  est <- list(prob=mstep[[3]], eps=eps_est, cov=covall_est, mean=meanall_est, lambda=lambda_est, constant=constant_est, covboot=mstep[[2]], meanboot=mstep[[1]])
all <- list(out, est)

return(all)  
} 

