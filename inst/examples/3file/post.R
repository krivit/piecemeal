#!/usr/bin/env Rscript

source("setup.R")

saveRDS(sim$result_df(), file = file.path(sys_tempdir, "piecemeal_results.rds"))
