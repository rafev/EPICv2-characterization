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
  library(progressr)
  
  ## --------------
  # Parallell and Progress setup
  ## --------------
  registerDoFuture()
  plan(multicore)
  
  options(future.globals.maxSize = 2350 * 1024^2) # changes the maxsize for globals to be 1.8 GiB for future objects
  options(progressr.enable = TRUE)
  
  #Set up progress bar
  handlers(global = TRUE)
  #handlers("cli")
  #handlers("progress")
  handlers(handler_progress(
  	enable = TRUE,
  	format = ":spin :current/:total [:bar] (:message) :percent in :elapsed ETA: :eta",
  	width = 150,
  	complete = "=")
  )
  
  
  ## --------------
  # Launch!
  ## --------------
  
  if(troubleshoot) {
  	chunks = seq_len(floor(ncol(matrix1)/100))
  } else {
  	chunks = seq_len(ncol(matrix1))
  }
  
  working_dir = getwd()
  
  p = progressor(along = chunks)
  
    foreach(j = chunks) %dopar% {
    	
              cor_output = cor(matrix1[,j], matrix2, method = 'pearson')
              
              write.table(cor_output, paste0(working_dir,"/cor_col",j))
              
              p(sprintf("i=%s", j))
              rm(cor_output)
              gc()
              
            }
  }

parallel_matrix_function()