# Procedures 
# Authors: Orazio Attanasio, Costas Meghir, and Emily Nix


#######################################################################################
drawfactor <- function(mean.mix, cov.mix, prob.mix){
  f <- rmvnorm.mixt(10000, mus=mean.mix, cov.mix, prob.mix)
  
  # Add hc1 = 1 as first column
  f_with_hc1 <- cbind(hc1 = rep(0, nrow(f)), f)       # log(1) = 0
  fC <- data.frame(exp(f_with_hc1))                   # now hc1 = exp(0) = 1
  colnames(fC) <- c("hc1", namef)                     # prepend name
  
  alldata <- data.frame(fC)
  lnalldata <- data.frame(f_with_hc1)
  
  colnames(alldata) <- colnames(lnalldata) <- c("hc1", namef)
  
  data <- list(alldata, lnalldata)
  return(data)
}