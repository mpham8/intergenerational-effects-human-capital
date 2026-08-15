# Estimation of measurement system on true data and bootstrap, also draws synth dataset
# Authors: Orazio Attanasio, Costas Meghir, and Emily Nix


setwd(dir_output)

for (boot in 0:Bootstrap){
  
  if (boot==0 & onlyboot==0){
    
  # Estimate distribution of latent factors
    all <- estim.meas.model(y, nM, nF, freelambda, mstep.start, param.start, conv, freeconstant, constparam, baseline, baselinelambda, baselinemean) 

  # save estimates to use as starting values in bootstrap
  param.startnew  <- all[[1]]$par
  save(param.startnew, file="param_startnew.R")
    
    est <- all[[2]]
    
    save(est, file="est.Rdata")

    lambda<- est$lambda      
    loadings<- est$lambda 
    save(loadings, file="loadings.Rdata")
    constants<- est$constant 
    save(constants, file="constants.Rdata")

    
    covariances<- est$cov  
    save(covariances, file="covariances.Rdata")
    
    means<- est$mean  
    save(means, file="means.Rdata")
    
    
    eps<- est$eps 
    save(eps, file="eps.Rdata")
    
    ##########################################################
    # Draw and save factors from the estimation distribution 
    ##########################################################
    prob.mix <- as.vector(est$prob)
    mean.mix <- rbind(est$mean)
    cov.mix  <- rbind(make.positive.definite(est$cov[[1]]), make.positive.definite(est$cov[[2]]))

    print_g_given_h_parameters <- function(period, h_index, g_index) {
      cond_means <- c()
      cond_sds <- c()
      cond_weights <- c()
      for (m in 1:nM) {
        cov_m <- cov.mix[((m - 1) * ncol(mean.mix) + 1):(m * ncol(mean.mix)), ]
        mu_h <- mean.mix[m, h_index]
        mu_g <- mean.mix[m, g_index]
        var_h <- cov_m[h_index, h_index]
        var_g <- cov_m[g_index, g_index]
        cov_gh <- cov_m[g_index, h_index]
        slope <- cov_gh / var_h
        intercept <- mu_g - slope * mu_h
        cond_sd <- sqrt(var_g - cov_gh^2 / var_h)
        cond_means[m] <- paste0(intercept, " + ", slope, " * h")
        cond_sds[m] <- cond_sd
        cond_weights[m] <- paste0(prob.mix[m], " * dnorm(h, ", mu_h, ", ", sqrt(var_h), ")")
      }
      cat("Mean of g", period, "|h", period, ": ", paste(cond_means, collapse="; "), "\n", sep="")
      cat("Standard deviation of g", period, "|h", period, ": ", paste(cond_sds, collapse=", "), "\n", sep="")
      cat("Weighting of g", period, "|h", period, ": normalize(", paste(cond_weights, collapse=", "), ")\n", sep="")
    }

    print_g_given_h_parameters(1, 1, nF + 1)
    print_g_given_h_parameters(2, 2, nF + 2)

    mstep.startboot<-list(est$meanboot, est$covboot, prob.mix)
    save(mstep.startboot, file="mstep_startboot.R")
    save(prob.mix, mean.mix, cov.mix, lambda, eps, nM, nF, bsample, file="trueFM.R")
  }
  if (boot==1  & onlyboot==0){ 

  load("param_startnew.R")
  load("mstep_startboot.R")
# Specify measurement system 
  
   bootfactor   <- function(b){  
      
      # construct bootstrapped dataset 
      bootindex <- sample(nrow(y), nrow(y), replace=TRUE)
      bootsample <- y[bootindex,]
      # Estimate factor model 
      allboot <- estim.meas.model(bootsample, nM, nF, freelambda, mstep.startboot, param.startnew, conv, freeconstant, constparam, baseline, baselinelambda,baselinemean) 

      return(allboot[[2]])
    } 
    
    # Run the function defined above 
    bootFM <- list()
    for (b in 1:bsample){
      
      print(b)
      bootFM[[b]] <- bootfactor(b)
    }
    
    # Now we have a measurement system for each bootstrapped sample of measures.
    # Save estimates along with indexes for estimation of production functions in next step 
    
    save(bootFM, file="allbootFM.R")
    # from these measurement systems, we will draw the latent factors to use them to estimate the pf.
  }
} 
