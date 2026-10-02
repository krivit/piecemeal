Running simulations on a cluster and monitoring their progress
------------------------------------------------------------------------

The two subdirectories, `setup_combined` and `setup_separate`
demonstrate two common patterns for using {piecemeal} on a cluster. In
both cases, we want to run the code on the worker nodes, monitor the
simulation on the head node, and collate the results on the head node
(though if it turns out to take a long time, we want to be able to
collate inside a batch job instead). We also want to minimise code
duplication.

The three-file pattern
----------------------

In the three-file pattern, we create the following files:

`setup.R`: Load {piecemeal}, initialise the `Piecemeal` object (here,
  `sim`) and configure the simulation, but don't reset it or run
  it. Also, if run directly (not `source()`d), consolidate the results
  into an SQLite database and print the simulation status.

`run.R`: `source()` the `setup.R` file and call `sim$run()`.

`post.R`: `source()` the `setup.R` file and call `sim$result_list()`
  or `sim$result_df()` and/or perform whatever collation,
  summarisation, and/or postprocessing is desired.

Now, we can have SLURM (or whatever job scheduler our cluster uses)
run `Rscript run.R` or `R CMD BATCH run.R` to start the
job. (Alternatively, `run.R` can be made executable and run directly.)
Don't forget to make sure the working directory is set so that the
`source()` call can find `setup.R`.

Once one or more jobs are running, we can check how far along they are
and the estimated completion time (particularly how likely they are to
finish before running out of time), by starting R on the head node and
running `source("setup.R")` and then `sim$status()`. If we
want to just check the status and exit, we can even run `Rscript
setup.R` from the shell or make `setup.R` executable and run it
directly. Note that `source()`-ing `setup.R` from either `run.R` or
`post.R` will not result in `sim$status()` being called, because the
code will not be run from the top level (`Rscript` or `R CMD BATCH` in
the system shell as opposed to `source()` within R).

Once the simulation has completed or there are enough results to start
the preliminary analysis, either `source("post.R")` from inside R,
`Rscript post.R` from the system shell, or `post.R` made executable
and run directly will (in this example) perform the collation and
save the results in an RDS file.

The two-file pattern
--------------------

The two-file pattern combines `setup.R` and `run.R` into a single R
script, `sim.R`. This script first sets up the simulation as in
`setup.R`, then calls the `sim$autorun()` method, which runs the
simulation only if called from the top level (that is, from `Rscript
sim.R` or `R CMD BATCH sim.R` as opposed to `source("sim.R")` in R).

The cost of this automation is that this might fail on some unusual
system configurations and that running `Rscript sim.R` to get the
status or begin consolidation will no longer work, since `autorun()`
will get in the way. These will therefore need to be done interactively.
