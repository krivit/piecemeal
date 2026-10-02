#!/usr/bin/env Rscript

source("sim.R") # Note that Piecemeal$autorun() won't run if source()d.

saveRDS(sim$result_df(), file = file.path(sys_tempdir, "piecemeal_results.rds"))
