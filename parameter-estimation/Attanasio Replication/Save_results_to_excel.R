# Function I created (Aaron) to save estimation results to Excel with separate sheets
library(openxlsx)

save_estimation_results <- function(out, est, filename = "estimation_results.xlsx") {
  
  # Helper function to check if object is valid for writing
  is_valid_for_excel <- function(obj) {
    return(!is.null(obj) && length(obj) > 0 && !all(is.na(obj)))
  }
  
  # Create workbook
  wb <- createWorkbook()
  
  # Add optimization results (out object)
  if(is_valid_for_excel(out$par)) {
    addWorksheet(wb, "optimization_par")
    writeData(wb, "optimization_par", data.frame(Parameter = out$par), rowNames = TRUE)
  }
  
  if(is_valid_for_excel(out$value) && is_valid_for_excel(out$convergence) && is_valid_for_excel(out$counts)) {
    addWorksheet(wb, "optimization_summary")
    opt_summary <- data.frame(
      Component = c("Final_Value", "Convergence_Code", "Function_Evaluations", "Gradient_Evaluations"),
      Value = c(out$value, out$convergence, out$counts[1], out$counts[2])
    )
    writeData(wb, "optimization_summary", opt_summary)
  }
  
  # Add convergence message if it exists
  if(!is.null(out$message) && is_valid_for_excel(out$message)) {
    addWorksheet(wb, "optimization_message")
    writeData(wb, "optimization_message", data.frame(Message = out$message))
  }
  
  # Add probabilities
  if(is_valid_for_excel(est$prob)) {
    addWorksheet(wb, "probabilities")
    writeData(wb, "probabilities", data.frame(Probability = est$prob), rowNames = TRUE)
  }
  
  # Add eps (error variances)
  if(is_valid_for_excel(est$eps)) {
    addWorksheet(wb, "eps")
    writeData(wb, "eps", data.frame(Error_Variance = est$eps), rowNames = TRUE)
  }
  
  # Add lambda (factor loadings matrix)
  if(is_valid_for_excel(est$lambda)) {
    addWorksheet(wb, "lambda")
    writeData(wb, "lambda", est$lambda, rowNames = TRUE)
  }
  
  # Add mean matrix
  if(is_valid_for_excel(est$mean)) {
    addWorksheet(wb, "mean")
    writeData(wb, "mean", est$mean, rowNames = TRUE)
  }
  
  # Add constants
  if(is_valid_for_excel(est$constant)) {
    addWorksheet(wb, "constants")
    writeData(wb, "constants", data.frame(Constant = est$constant), rowNames = TRUE)
  }
  
  # Add covariance matrices (each as separate sheet)
  if(!is.null(est$cov) && length(est$cov) > 0) {
    for(i in seq_along(est$cov)) {
      if(is_valid_for_excel(est$cov[[i]])) {
        addWorksheet(wb, paste0("cov_matrix_", i))
        writeData(wb, paste0("cov_matrix_", i), est$cov[[i]], rowNames = TRUE)
      }
    }
  }
  
  # Add covboot matrices (from EM algorithm)
  if(!is.null(est$covboot) && length(est$covboot) > 0) {
    for(i in seq_along(est$covboot)) {
      if(is_valid_for_excel(est$covboot[[i]])) {
        addWorksheet(wb, paste0("covboot_matrix_", i))
        writeData(wb, paste0("covboot_matrix_", i), est$covboot[[i]], rowNames = TRUE)
      }
    }
  }
  
  # Add meanboot matrix
  if(is_valid_for_excel(est$meanboot)) {
    addWorksheet(wb, "meanboot")
    writeData(wb, "meanboot", est$meanboot, rowNames = TRUE)
  }
  
  # Save workbook
  saveWorkbook(wb, filename, overwrite = TRUE)
  
  cat("Results saved to", filename, "\n")
  cat("Sheets created:\n")
  if(is_valid_for_excel(out$par)) cat("- optimization_par: Optimal parameters from optim()\n")
  if(is_valid_for_excel(out$value)) cat("- optimization_summary: Convergence info and function evaluations\n")
  if(!is.null(out$message) && is_valid_for_excel(out$message)) cat("- optimization_message: Convergence message\n")
  if(is_valid_for_excel(est$prob)) cat("- probabilities: Mixture probabilities\n")
  if(is_valid_for_excel(est$eps)) cat("- eps: Error variances\n")
  if(is_valid_for_excel(est$lambda)) cat("- lambda: Factor loadings matrix\n")
  if(is_valid_for_excel(est$mean)) cat("- mean: Mean estimates\n")
  if(is_valid_for_excel(est$constant)) cat("- constants: Constant parameters\n")
  if(!is.null(est$cov) && length(est$cov) > 0) {
    for(i in seq_along(est$cov)) {
      if(is_valid_for_excel(est$cov[[i]])) {
        cat("- cov_matrix_", i, ": Covariance matrix for mixture ", i, "\n", sep="")
      }
    }
  }
  if(!is.null(est$covboot) && length(est$covboot) > 0) {
    for(i in seq_along(est$covboot)) {
      if(is_valid_for_excel(est$covboot[[i]])) {
        cat("- covboot_matrix_", i, ": Bootstrap covariance matrix for mixture ", i, "\n", sep="")
      }
    }
  }
  if(is_valid_for_excel(est$meanboot)) cat("- meanboot: Bootstrap means\n")
}

# Example usage (add this to your main script after running the estimation):
# save_estimation_results(out, est, "my_estimation_results.xlsx")
