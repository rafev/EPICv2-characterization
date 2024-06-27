################################################################################
##** TESTING PARALLEL COR FUNCTION **
################################################################################

parallel_matrix_function = function() {
  args = commandArgs(trailingOnly = TRUE)
  
  ## --------------
  # Read in matrices
  ## --------------
  cat("Reading in files...")
  diff_list_filenames = list.files(args[1])
  EPICv2_remainder_gene_map = readRDS(args[2])
  EHv1_remainder_gene_map = readRDS(args[3])
  
  if(length(args) > 3) {
    cat("troubleshoot mode enabled\n")
    troubleshoot = as.logical(args[4])
  } else { cat("troubleshoot mode disabled\n"); troubleshoot = FALSE }
  
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
    chunks = seq_len(floor(length(diff_list_filenames)/1000))
  } else {
    chunks = seq_len(length(diff_list_filenames))
  }
  
  working_dir = getwd()
  
  foreach(j = chunks) %dopar% {
    
    EHv1.name = gsub(".rds", "", diff_list_filenames[j])
    
    res = data.frame(diffs = readRDS(paste0(args[1],diff_list_filenames[j])))
    res = merge(res, EPICv2_remainder_gene_map, by.x = "row.names", by.y = "EPICv2_IlmnID", all.x = T)
    res = res[apply(res, 1, function(df){
      
      any(unlist(strsplit(df["EPICv2_UCSC_RefGene_Name"], split = ";")) %in%
            EHv1_remainder_gene_map[EHv1_remainder_gene_map$EHv1_ID==EHv1.name,
                                    "EHv1_UCSC_RefGene_Name"])
    }), ]
    
    cat("writting output",names(diff_list_filenames)[j],"to",working_dir,"/gene.match_outputs","\n")
    saveRDS(res, paste0(working_dir,"/gene.match_outputs/",diff_list_filenames[j]))
    
    rm(res)
    gc()
    
  }
  cat("DONE!!\n")
}

parallel_matrix_function()