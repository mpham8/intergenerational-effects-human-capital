# Period 3
# Inputs: period 3 human capital, period 3 parental investments, period 3 governmental investments
# Output: period 4 human capital

CES3_nocf <- function(alldata) {
  out <- log(alldata$hc4)
  # Debug CES term
  s1 <- 0.3; s2 <- 0.2; rho <- -2
  ces_term <- s1 * alldata$hc3^rho + s2 * alldata$govinvest3^rho + (1-s1-s2) * alldata$pinvest3^rho
  print("CES term summary at start:")
  print(summary(ces_term))
  cesout <- try(nlsLM(out ~ (1/rho)*log(s1 * hc3^rho + s2 * govinvest3^rho + (1-s1-s2) * pinvest3^rho) 
                      + delta + alpha3 * peducation, data = alldata,
                      start=c(delta=0.3, alpha3=0.05, s1=0.3, s2=0.2, rho=0.5),
                      lower=c(-Inf, -Inf, 0, 0, -5),
                      upper=c(Inf, Inf, 1, 1, 5),
                      control=nls.lm.control(maxiter=1000, ftol=1e-8, ptol=1e-8, gtol=1e-8)), silent=FALSE)
  if (length(summary(cesout)) == 11) {  
    npar <- nrow(summary(cesout)[[10]])
    elast <- 1 / (1 - (summary(cesout)[[10]][npar,1]))
    s3 <- 1 - sum(summary(cesout)[[10]][3:4,1])
    residual_sd <- summary(cesout)[[3]]
    estim <- c(summary(cesout)[[10]][1:(npar-1),1], s3, summary(cesout)[[10]][npar,1], elast, residual_sd)
    names(estim) <- c("delta", "peducation", "hc3", "govinvest3", "pinvest3", "rho", "elast", "residual sd")
  } else {
    print("nlsLM failed with error:")
    print(cesout)
    estim <- rep(NA, 8)
  }
  return(estim)
}

CES3_cf <- function(alldata){
  out <- log(alldata$hc4)
  inst3 <- lm(log(pinvest3) ~ log(hc3) + log(peducation) + log(income3), data=alldata)
  cf <- inst3$residuals
  # Extract readable summary statistics
  inst3_summary <- data.frame(
    coefficients = summary(inst3)$coefficients[,1],
    std_errors = summary(inst3)$coefficients[,2],
    t_values = summary(inst3)$coefficients[,3],
    p_values = summary(inst3)$coefficients[,4],
    r_squared = summary(inst3)$r.squared,
    adj_r_squared = summary(inst3)$adj.r.squared,
    f_statistic = summary(inst3)$fstatistic[1],
    f_pvalue = pf(summary(inst3)$fstatistic[1], 
                  summary(inst3)$fstatistic[2], 
                  summary(inst3)$fstatistic[3], 
                  lower.tail = FALSE),
    row.names = names(summary(inst3)$coefficients[,1])
  )
  cesout <- try(nlsLM(out ~ (1/rho)* log(s1 * hc3^rho + s2*govinvest3^rho + (1-s1-s2)* pinvest3^rho) 
                      + delta + alpha*cf + alpha3*peducation, data = alldata,
                      start=c(delta=0.3, alpha=0, alpha3=.05, s1=0.3, s2=0.2, rho=0.5),
                      lower=c(-Inf, -Inf, -Inf, 0, 0, -5),
                      upper=c(Inf, Inf, Inf, 1, 1, 5),
                      control=nls.lm.control(maxiter=1000)), silent=FALSE)
  if (length(summary(cesout))==11) {  
    npar <- nrow(summary(cesout)[[10]])
    elast <- 1 / (1 - (summary(cesout)[[10]][npar,1]))
    s3 <- 1 - sum(summary(cesout)[[10]][4:5,1])
    residual_sd <- summary(cesout)[[3]]
    estim <- c(summary(cesout)[[10]][1:(npar-1),1], s3, summary(cesout)[[10]][npar,1], elast, residual_sd)
    names(estim) <- c("delta", "controlfn", "peducation", "hc3", "govinvest3", "pinvest3", "rho", "elast", "residual sd")
    gamma = summary(cesout)[[10]][2,1]
    structural_res = residuals(cesout) + gamma*cf
    reg <- lm(structural_res ~ log(hc3) + log(peducation) + log(income3), data=alldata)
    coef <- as.vector(summary.lm(reg)[[4]][,1])
    se <- as.vector(summary.lm(reg)[[4]][,2])
    vcov <- as.matrix(vcov(reg))
  } else {
    estim <- rep(NA, 9)
    coef <- rep(NA, 4)
  }
  return(list(estim, summary(inst3)[[4]][,1], coef, inst3_summary))
}

# Period 2

CES2_nocf <- function(alldata){
  out <- log(alldata$hc3)
  cesout <- try(nlsLM(out ~ (1/rho)* log(s1 * hc2^rho + s2*govinvest2^rho + (1-s1-s2)* pinvest2^rho) 
                      + delta + alpha3*peducation, data = alldata,
                      start=c(delta=0.3, alpha3=.05, s1=0.3, s2=0.2, rho=0.5),
                      lower=c(-Inf, -Inf, 0, 0, -5),
                      upper=c(Inf, Inf, 1, 1, 5),
                      control=nls.lm.control(maxiter=1000)), silent=FALSE)
  if (length(summary(cesout))==11) {  
    npar <- nrow(summary(cesout)[[10]])
    elast <- 1 / (1 - (summary(cesout)[[10]][npar,1]))
    s3 <- 1 - sum(summary(cesout)[[10]][3:4,1])
    residual_sd <- summary(cesout)[[3]]
    estim <- c(summary(cesout)[[10]][1:(npar-1),1], s3, summary(cesout)[[10]][npar,1], elast, residual_sd)
    names(estim) <- c("delta", "peducation", "hc2", "govinvest2", "pinvest2", "rho", "elast", "residual sd")
  } else {
    estim <- rep(NA, 8)
  }
  return(estim)
}

CES2_cf <- function(alldata){
  out <- log(alldata$hc3)
  inst2 <- lm(log(pinvest2) ~ log(hc2) + log(peducation) + log(income2), data=alldata)
  cf <- inst2$residuals
  # Extract readable summary statistics
  inst2_summary <- data.frame(
    coefficients = summary(inst2)$coefficients[,1],
    std_errors = summary(inst2)$coefficients[,2],
    t_values = summary(inst2)$coefficients[,3],
    p_values = summary(inst2)$coefficients[,4],
    r_squared = summary(inst2)$r.squared,
    adj_r_squared = summary(inst2)$adj.r.squared,
    f_statistic = summary(inst2)$fstatistic[1],
    f_pvalue = pf(summary(inst2)$fstatistic[1], 
                  summary(inst2)$fstatistic[2], 
                  summary(inst2)$fstatistic[3], 
                  lower.tail = FALSE),
    row.names = names(summary(inst2)$coefficients[,1])
  )
  cesout <- try(nlsLM(out ~ (1/rho)* log(s1 * hc2^rho + s2*govinvest2^rho + (1-s1-s2)* pinvest2^rho) 
                      + delta + alpha*cf + alpha3*peducation, data = alldata,
                      start=c(delta=0.3, alpha=0, alpha3=.05, s1=0.3, s2=0.2, rho=0.5),
                      lower=c(-Inf, -Inf, -Inf, 0, 0, -5),
                      upper=c(Inf, Inf, Inf, 1, 1, 5),
                      control=nls.lm.control(maxiter=1000)), silent=FALSE)
  if (length(summary(cesout))==11) {  
    npar <- nrow(summary(cesout)[[10]])
    elast <- 1 / (1 - (summary(cesout)[[10]][npar,1]))
    s3 <- 1 - sum(summary(cesout)[[10]][4:5,1])
    residual_sd <- summary(cesout)[[3]]
    estim <- c(summary(cesout)[[10]][1:(npar-1),1], s3, summary(cesout)[[10]][npar,1], elast, residual_sd)
    names(estim) <- c("delta", "controlfn", "peducation", "hc2", "govinvest2", "pinvest2", "rho", "elast", "residual sd")
    gamma = summary(cesout)[[10]][2,1]
    structural_res = residuals(cesout) + gamma*cf
    reg <- lm(structural_res ~ log(hc2) + log(peducation) + log(income2), data=alldata)
    coef <- as.vector(summary.lm(reg)[[4]][,1])
    se <- as.vector(summary.lm(reg)[[4]][,2])
    vcov <- as.matrix(vcov(reg))
  } else {
    estim <- rep(NA, 9)
    coef <- rep(NA, 4)
  }
  return(list(estim, summary(inst2)[[4]][,1], coef, inst2_summary))
}

# Period 1

CES1_nocf <- function(alldata){
  out <- log(alldata$hc2)
  cesout <- try(nlsLM(out ~ (1/rho)* log(s1 * hc1^rho + s2*govinvest1^rho + (1-s1-s2)* pinvest1^rho) 
                      + delta + alpha3*peducation, data = alldata,
                      start=c(delta=0.3, alpha3=.05, s1=0.3, s2=0.2, rho=0.5),
                      lower=c(-Inf, -Inf, 0, 0, -5),
                      upper=c(Inf, Inf, 1, 1, 5),
                      control=nls.lm.control(maxiter=1000)), silent=FALSE)
  if (length(summary(cesout))==11) {  
    npar <- nrow(summary(cesout)[[10]])
    elast <- 1 / (1 - (summary(cesout)[[10]][npar,1]))
    s3 <- 1 - sum(summary(cesout)[[10]][3:4,1])
    residual_sd <- summary(cesout)[[3]]
    estim <- c(summary(cesout)[[10]][1:(npar-1),1], s3, summary(cesout)[[10]][npar,1], elast, residual_sd)
    names(estim) <- c("delta", "peducation", "hc1", "govinvest1", "pinvest1", "rho", "elast", "residual sd")
  } else {
    estim <- rep(NA, 8)
  }
  return(estim)
}

CES1_cf <- function(alldata){
  out <- log(alldata$hc2)
  inst1 <- lm(log(pinvest1) ~ log(peducation) + log(income1), data=alldata)
  cf <- inst1$residuals
  # Extract readable summary statistics
  inst1_summary <- data.frame(
    coefficients = summary(inst1)$coefficients[,1],
    std_errors = summary(inst1)$coefficients[,2],
    t_values = summary(inst1)$coefficients[,3],
    p_values = summary(inst1)$coefficients[,4],
    r_squared = summary(inst1)$r.squared,
    adj_r_squared = summary(inst1)$adj.r.squared,
    f_statistic = summary(inst1)$fstatistic[1],
    f_pvalue = pf(summary(inst1)$fstatistic[1], 
                  summary(inst1)$fstatistic[2], 
                  summary(inst1)$fstatistic[3], 
                  lower.tail = FALSE),
    row.names = names(summary(inst1)$coefficients[,1])
  )
  cesout <- try(nlsLM(out ~ (1/rho)* log(s1 * hc1^rho + s2*govinvest1^rho + (1-s1-s2)* pinvest1^rho) 
                      + delta + alpha*cf + alpha3*peducation, data = alldata,
                      start=c(delta=0.3, alpha=0, alpha3=.05, s1=0.3, s2=0.2, rho=0.5),
                      lower=c(-Inf, -Inf, -Inf, 0, 0, -5),
                      upper=c(Inf, Inf, Inf, 1, 1, 5),
                      control=nls.lm.control(maxiter=1000)), silent=FALSE)
  if (length(summary(cesout))==11) {  
    npar <- nrow(summary(cesout)[[10]])
    elast <- 1 / (1 - (summary(cesout)[[10]][npar,1]))
    s3 <- 1 - sum(summary(cesout)[[10]][4:5,1])
    residual_sd <- summary(cesout)[[3]]
    estim <- c(summary(cesout)[[10]][1:(npar-1),1], s3, summary(cesout)[[10]][npar,1], elast, residual_sd)
    names(estim) <- c("delta", "controlfn", "peducation", "hc1", "govinvest1", "pinvest1", "rho", "elast", "residual sd")
    gamma = summary(cesout)[[10]][2,1]
    structural_res = residuals(cesout) + gamma*cf
    reg <- lm(structural_res ~ log(peducation) + log(income1), data=alldata)
    coef <- as.vector(summary.lm(reg)[[4]][,1])
    se <- as.vector(summary.lm(reg)[[4]][,2])
    vcov <- as.matrix(vcov(reg))
  } else {
    estim <- rep(NA, 9)
    coef <- rep(NA, 3)
  }
  return(list(estim, summary(inst1)[[4]][,1], coef, inst1_summary))
}

##ESTIMATION##
for (boot in 0:1){
  # Estimation on true data
  if (boot==0){
    setwd(dir_output)
    load("trueFM.R")
    data <- drawfactor(mean.mix, cov.mix, prob.mix)
    alldata <- data[[1]]
    # CES period 3, without control functions
    out_ces3_nocf <- CES3_nocf(alldata)
    # CES period 3, with control functions
    out3 <- CES3_cf(alldata)
    out_ces3_cf <- out3[[1]]
    coef_ces3 <- out3[[3]]
    # Investment in period 3
    trueInvest3 <- out3[[2]]
    # CES period 2, without control functions
    out_ces2_nocf <- CES2_nocf(alldata)
    # CES period 2, with control functions
    out2 <- CES2_cf(alldata)
    out_ces2_cf <- out2[[1]]
    coef_ces2 <- out2[[3]]
    # Investment in period 2
    trueInvest2 <- out2[[2]]
    # CES period 1, without control functions
    out_ces1_nocf <- CES1_nocf(alldata)
    # CES period 1, with control functions
    out1 <- CES1_cf(alldata)
    out_ces1_cf <- out1[[1]]
    coef_ces1 <- out1[[3]]
    # Investment in period 1
    trueInvest1 <- out1[[2]]
    # Put estimates of production functions with and without control functions together
    npari <- length(out_ces3_nocf)
    nocf3_vector <- c(out_ces3_nocf[1], rep(0,1), out_ces3_nocf[2:npari])
    out_ces3_nocf <- matrix(nocf3_vector, ncol=1)
    out_ces3_cf <- matrix(out_ces3_cf, ncol=1)
    trueCESperiod3 <- cbind(out_ces3_nocf, out_ces3_cf)
    nocf2_vector <- c(out_ces2_nocf[1], rep(0,1), out_ces2_nocf[2:npari])
    out_ces2_nocf <- matrix(nocf2_vector, ncol=1)
    out_ces2_cf <- matrix(out_ces2_cf, ncol=1)
    trueCESperiod2 <- cbind(out_ces2_nocf, out_ces2_cf)
    nocf1_vector <- c(out_ces1_nocf[1], rep(0,1), out_ces1_nocf[2:npari])
    out_ces1_nocf <- matrix(nocf1_vector, ncol=1)
    out_ces1_cf <- matrix(out_ces1_cf, ncol=1)
    trueCESperiod1 <- cbind(out_ces1_nocf, out_ces1_cf)
    rownames(trueCESperiod3) <- c("delta", "control function", "peducation", "hc3", "govinvest3", "pinvest3", "rho", "elast", "residual sd")
    colnames(trueCESperiod3) <- c("Human Capital without Control Function", "Human Capital with Control Function")
    rownames(trueCESperiod2) <- c("delta", "control function", "peducation", "hc2", "govinvest2", "pinvest2", "rho", "elast", "residual sd")
    colnames(trueCESperiod2) <- c("Human Capital without Control Function", "Human Capital with Control Function")
    rownames(trueCESperiod1) <- c("delta", "control function", "peducation", "hc1", "govinvest1", "pinvest1", "rho", "elast", "residual sd")
    colnames(trueCESperiod1) <- c("Human Capital without Control Function", "Human Capital with Control Function")
    # Save output in the proper directory 
    setwd(dir_output)
    save(trueCESperiod3, file="trueCESperiod3.R")
    save(trueCESperiod2, file="trueCESperiod2.R")
    save(trueCESperiod1, file="trueCESperiod1.R")
    save(trueInvest3, file="trueInvest3.R")
    save(trueInvest2, file="trueInvest2.R")
    save(trueInvest1, file="trueInvest1.R")
    save(coef_ces3, file="coef_ces3.R")
    save(coef_ces2, file="coef_ces2.R")
    save(coef_ces1, file="coef_ces1.R")
  }
  # Estimation on bootstrap data
  else if (boot==1){
    setwd(dir_output)
    load("allbootFM.R")
    # Define a function that runs the estimation on each bootstrap sample
    bootprod <- function(b){  
      prob.boot <- bootFM[[b]]$prob
      mean.boot <- bootFM[[b]]$mean
      cov.boot <- rbind(make.positive.definite(bootFM[[b]]$cov[[1]]), make.positive.definite(bootFM[[b]]$cov[[2]]))
      alldatab <- drawfactor(mean.boot, cov.boot, prob.boot)[[1]]
      print(summary(alldatab[, c("hc3", "govinvest3", "pinvest3", "peducation")]))
      # CES period 3, without control functions
      out_ces3_nocf <- CES3_nocf(alldatab)
      # CES period 3, with control functions
      out3 <- CES3_cf(alldatab)
      out_ces3_cf <- out3[[1]]
      coef_ces3 <- out3[[3]]
      # Investment in period 3
      outInvest3 <- out3[[2]]
      # CES period 2, without control functions
      out_ces2_nocf <- CES2_nocf(alldatab)
      # CES period 2, with control functions
      out2 <- CES2_cf(alldatab)
      out_ces2_cf <- out2[[1]]
      coef_ces2 <- out2[[3]]
      # Investment in period 2
      outInvest2 <- out2[[2]]
      # CES period 1, without control functions
      out_ces1_nocf <- CES1_nocf(alldatab)
      # CES period 1, with control functions
      out1 <- CES1_cf(alldatab)
      out_ces1_cf <- out1[[1]]
      coef_ces1 <- out1[[3]]
      # Investment in period 1
      outInvest1 <- out1[[2]]
      estim <- list(out_ces3_nocf, out_ces3_cf, 
                    out_ces2_nocf, out_ces2_cf, 
                    out_ces1_nocf, out_ces1_cf,
                    outInvest3, outInvest2, outInvest1,
                    coef_ces3, coef_ces2, coef_ces1)
      return(estim)
    }
    # Create array to save estimates from each bootstrap
    bootCES3 <- array(0, dim=c(9, 2, bsample))
    bootCES2 <- array(0, dim=c(9, 2, bsample))
    bootCES1 <- array(0, dim=c(9, 2, bsample))
    bootInvest3 <- array(0, dim=c(4, 1, bsample))
    bootInvest2 <- array(0, dim=c(4, 1, bsample))
    bootInvest1 <- array(0, dim=c(3, 1, bsample))
    bootcoef_ces3 <- array(0, dim=c(4, bsample))
    bootcoef_ces2 <- array(0, dim=c(4, bsample))
    bootcoef_ces1 <- array(0, dim=c(3, bsample))
    npar <- 9
    npar1 <- npar - 1
    # Run the estimation on each bootstrap sample 
    for (b in 1:bsample) {
      print(b)      
      estim <- bootprod(b)
      nocf_vector <- c(estim[[1]][1], rep(0,1), estim[[1]][2:npar1])
      bootCES3[,,b] <- cbind(nocf_vector, estim[[2]][1:npar])
      nocf_vector <- c(estim[[3]][1], rep(0,1), estim[[3]][2:npar1])
      bootCES2[,,b] <- cbind(nocf_vector, estim[[4]][1:npar])
      nocf_vector <- c(estim[[5]][1], rep(0,1), estim[[5]][2:npar1])
      bootCES1[,,b] <- cbind(nocf_vector, estim[[6]][1:npar])
      bootInvest3[,,b] <- matrix(estim[[7]], nrow=4, ncol=1)
      bootInvest2[,,b] <- matrix(estim[[8]], nrow=4, ncol=1)
      bootInvest1[,,b] <- matrix(estim[[9]], nrow=3, ncol=1)
      bootcoef_ces3[,b] <- estim[[10]]
      bootcoef_ces2[,b] <- estim[[11]]
      bootcoef_ces1[,b] <- estim[[12]]
    }
    # Save output
    setwd(dir_output)
    save(bootCES3, file="bootCES3.R")
    save(bootCES2, file="bootCES2.R")
    save(bootCES1, file="bootCES1.R")
    save(bootInvest3, file="bootInvest3.R")
    save(bootInvest2, file="bootInvest2.R")
    save(bootInvest1, file="bootInvest1.R")
    save(bootcoef_ces3, file="bootcoef_ces3.R")
    save(bootcoef_ces2, file="bootcoef_ces2.R")
    save(bootcoef_ces1, file="bootcoef_ces1.R")
  }
}

# MAKE TABLES OF RESULTS WITH BOOTSTRAPPED STANDARD ERRORS

# Estimates of the production functions:
npar <- 8

cestable_3 <- matrix(0, (npar*3), 2)
cestable_2 <- matrix(0, (npar*3), 2)
cestable_1 <- matrix(0, (npar*3), 2)

# Estimates of the production functions (Period 3):
for (i in 1:npar) { 
  cestable_3[(i*3-2), ] <- round(trueCESperiod3[i,1:2],3)       # Point estimates                                      
  for (l in 1:2) {
    cestable_3[(i*3-1), l] <- round(sd(bootCES3[i,l,], na.rm=TRUE), 3)   # Standard errors 
    cestable_3[(i*3), l] <- paste("[", round(quantile(bootCES3[i,l,], 0.05, na.rm=TRUE), 3), ",", 
                                  round(quantile(bootCES3[i,l,], 0.95, na.rm=TRUE), 3), "]", sep="") 
  } 
}
colnames(cestable_3) <- c("Human Capital without Control Function", "Human Capital with Control Function")
rownames(cestable_3) <- c("delta","","",
                          "control function","","",
                          "peducation","","",
                          "human capital 3","","",
                          "government investment 3","","",
                          "parental investment 3","","",
                          "rho","","",
                          "elasticity of substitution","","")

# Estimates of the investment functions (Period 3)
investtable_3 <- matrix(0, (4*3), 1)
for (i in 1:4) {
  investtable_3[(i*3-2), 1] <- round(trueInvest3[i],3)
  investtable_3[(i*3-1), 1] <- round(sd(bootInvest3[i,1,], na.rm=TRUE),3)
  investtable_3[(i*3), 1] <- paste("[", round(quantile(bootInvest3[i,1,], 0.05, na.rm=TRUE),3), ",", 
                                   round(quantile(bootInvest3[i,1,], 0.95, na.rm=TRUE),3), "]", sep="")  
}
colnames(investtable_3) <- "Period 3"
rownames(investtable_3) <- c("Intercept","","",
                             "Human Capital 3","","",
                             "Highest Grade Completed Mother","","",
                             "Income 3","","")

# Estimates of the production functions (Period 2):
for (i in 1:npar) { 
  cestable_2[(i*3-2), ] <- round(trueCESperiod2[i,1:2],3)       # Point estimates                                      
  for (l in 1:2) {
    cestable_2[(i*3-1), l] <- round(sd(bootCES2[i,l,], na.rm=TRUE), 3)   # Standard errors 
    cestable_2[(i*3), l] <- paste("[", round(quantile(bootCES2[i,l,], 0.05, na.rm=TRUE), 3), ",", 
                                  round(quantile(bootCES2[i,l,], 0.95, na.rm=TRUE), 3), "]", sep="") 
  } 
}
colnames(cestable_2) <- c("Human Capital without Control Function", "Human Capital with Control Function")
rownames(cestable_2) <- c("delta","","",
                          "control function","","",
                          "peducation","","",
                          "human capital 2","","",
                          "government investment 2","","",
                          "parental investment 2","","",
                          "rho","","",
                          "elasticity of substitution","","")

# Estimates of the investment functions (Period 2)
investtable_2 <- matrix(0, (4*3), 1)
for (i in 1:4) {
  investtable_2[(i*3-2), 1] <- round(trueInvest2[i],3)
  investtable_2[(i*3-1), 1] <- round(sd(bootInvest2[i,1,], na.rm=TRUE),3)
  investtable_2[(i*3), 1] <- paste("[", round(quantile(bootInvest2[i,1,], 0.05, na.rm=TRUE),3), ",", 
                                   round(quantile(bootInvest2[i,1,], 0.95, na.rm=TRUE),3), "]", sep="")  
}
colnames(investtable_2) <- "Period 2"
rownames(investtable_2) <- c("Intercept","","",
                             "Human Capital 2","","",
                             "Highest Grade Completed Mother","","",
                             "Income 2","","")

# Estimates of the production functions (Period 1):
for (i in 1:npar) { 
  cestable_1[(i*3-2), ] <- round(trueCESperiod1[i,1:2],3)       # Point estimates                                      
  for (l in 1:2) {
    cestable_1[(i*3-1), l] <- round(sd(bootCES1[i,l,], na.rm=TRUE), 3)   # Standard errors 
    cestable_1[(i*3), l] <- paste("[", round(quantile(bootCES1[i,l,], 0.05, na.rm=TRUE), 3), ",", 
                                  round(quantile(bootCES1[i,l,], 0.95, na.rm=TRUE), 3), "]", sep="") 
  } 
}
colnames(cestable_1) <- c("Human Capital without Control Function", "Human Capital with Control Function")
rownames(cestable_1) <- c("delta","","",
                          "control function","","",
                          "peducation","","",
                          "human capital 1","","",
                          "government investment 1","","",
                          "parental investment 1","","",
                          "rho","","",
                          "elasticity of substitution","","")

# Estimates of the investment functions (Period 1)
investtable_1 <- matrix(0, (3*3), 1)
for (i in 1:3) {
  investtable_1[(i*3-2), 1] <- round(trueInvest1[i],3)
  investtable_1[(i*3-1), 1] <- round(sd(bootInvest1[i,1,], na.rm=TRUE),3)
  investtable_1[(i*3), 1] <- paste("[", round(quantile(bootInvest1[i,1,], 0.05, na.rm=TRUE),3), ",", 
                                   round(quantile(bootInvest1[i,1,], 0.95, na.rm=TRUE),3), "]", sep="")  
}
colnames(investtable_1) <- "Period 1"
rownames(investtable_1) <- c("Intercept","","",
                             "Highest Grade Completed Mother","","",
                             "Income 1","","")

# Save tables:
write.csv(cestable_3, file="cestable_3.csv")
write.csv(cestable_2, file="cestable_2.csv")
write.csv(cestable_1, file="cestable_1.csv")
write.csv(investtable_3, file="investtable3.csv")
write.csv(investtable_2, file="investtable2.csv")
write.csv(investtable_1, file="investtable1.csv")

# Save summary of first regression
inst3_summary <- out3[[4]]
write.csv(inst3_summary, "inst3_regression_summary.csv", row.names = TRUE)
inst2_summary <- out2[[4]]
write.csv(inst2_summary, "inst2_regression_summary.csv", row.names = TRUE)  
inst1_summary <- out1[[4]]
write.csv(inst1_summary, "inst1_regression_summary.csv", row.names = TRUE)