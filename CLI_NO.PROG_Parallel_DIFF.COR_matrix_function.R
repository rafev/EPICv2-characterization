################################################################################
##** TESTING PARALLEL COR FUNCTION **
################################################################################

parallel_matrix_function = function() {
  args = commandArgs(trailingOnly = TRUE)
  
  ## --------------
  # Read in matrices
  ## --------------
  matrix1 = readRDS(args[1])
  matrix2 = readRDS(args[2])
  if(length(args) > 2) {
    troubleshoot = as.logical(args[3])
  } else { troubleshoot = FALSE }
  
  ## --------------
  # Load in libraries
  ## --------------
  library(doFuture)
  library(data.table)
  library(stringr)
  
  ## --------------
  # Parallell and Progress setup
  ## --------------
  cat("Setting up parallel backend\n")
  registerDoFuture()
  plan(multicore)
  
  options(future.globals.maxSize = 2350 * 1024^2) # changes the maxsize for globals to be 1.8 GiB for future objects
  options(progressr.enable = TRUE)
  
  
  ## --------------
  # Launch!
  ## --------------
  cat("Launching...\n")
  if(troubleshoot) {
    chunks = seq_len(floor(ncol(matrix1)/100))
  } else {
    chunks = seq_len(ncol(matrix1))
  }
  
  working_dir = getwd()
  
  foreach(j = chunks) %dopar% {
    
    cpg = colnames(matrix1)[j]
    
    ## Difference calculations
    diff_output = colMeans(matrix1[,j] - matrix2)
    
    diff_output1 = head(diff_output[order(abs(diff_output))], 10000)
    diff_output2 = diff_output[which(abs(diff_output) <= 0.05)]
    
    if (length(diff_output1) > length(diff_output2)) {
      diff_output = diff_output1
    } else {diff_output = diff_output2}
    
    
    ## Correlation calculations
    cor_output = cor(matrix1[,j], matrix2, method = 'pearson')
    
    cor_output1 = head(cor_output[,order(cor_output, decreasing = T)], 10000)
    cor_output2 = cor_output[,which(apply(cor_output, MARGIN = 2, max)>0.9)]

    if (length(cor_output1) > length(cor_output2)) {
      cor_output = cor_output1
    } else {cor_output = cor_output2}
    
    
    ## Overlap of diff and cor
    diff_output = diff_output[names(diff_output) %in% names(cor_output)]
    diff_output = diff_output[order(abs(diff_output))]
    
    
    ## Save final result
    cat("writting matrix",j,"to",working_dir,"\n")
    saveRDS(diff_output, paste0(working_dir,"/diff.cor_outputs/",cpg,".rds"))

    rm(diff_output, cor_output, diff_output2, diff_output1, cor_output2, cor_output1)
    gc()
    
  }
  cat("DONE!!")
}

parallel_matrix_function()