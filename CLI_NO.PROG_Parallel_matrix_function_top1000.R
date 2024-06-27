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
    
    cor_output = cor(matrix1[,j], matrix2, method = 'pearson')
    
    cor_output1 = head(cor_output[,order(cor_output, decreasing = T)], 10000)
    cor_output2 = cor_output[,which(apply(cor_output, MARGIN = 2, max)>0.9)]
    
    if (length(cor_output1) > length(cor_output2)) {
      cor_output = cor_output1
    } else {cor_output = cor_output2}
    
    cat("writting matrix",j,"to",working_dir,"\n")
    saveRDS(cor_output, paste0(working_dir,"/cor_outputs/",cpg,".rds"))
    #write.table(cor_output, paste0(working_dir,"/cor_col",j))
    
    rm(cor_output)
    gc()
    
  }
  cat("DONE!!")
}

parallel_matrix_function()