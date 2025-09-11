Running simulations on a cluster and monitoring their progress
------------------------------------------------------------------------

Files `setup.R`, `run.R`, and `post.R` demonstrate a common pattern
for using {piecemeal} on a cluster. Here, we want to run the code on
the worker nodes, monitor the simulation on the head node, and collate
the results on the head node.

Thus, we create three files:

`setup.R`: Load {piecemeal}, initialise the `Piecemeal` object (here,
  `sim`) and configure the simulation, but don't reset it or run it.

`run.R`: `source()` the `setup.R` file and call `sim$run()`.

`post.R`: `source()` the `setup.R` file and call `sim$result_list()`
  or `sim$result_df()` and/or perform whatever collation,
  summarisation, and/or postprocessing is desired.

Now, we can have SLURM (or whatever job scheduler your cluster uses)
run `Rscript run.R` to start the job. (Don't forget to make sure the
working directory is set so that the `source()` call can find
`setup.R`.)

Once one or more jobs are running, we can check how far along they are
and the estimated completion time (particularly how likely they are to
finish before running out of time), by starting R on the head node and
running `source("setup.R")` and subsequently `sim$status()`. If we
want to just check the status and exit, we can even run `Rscript
"setup.R"` from the shell. Note that `source()`-ing `setup.R` from
either `run.R` or `post.R` will not result in `sim$status()` being
called, because the call will be neither interactive nor top-level.

Once the simulation has completed or there are enough results to start
the preliminary analysis, either `source("post.R")` from inside R or
`Rscript post.R` will (in this example) perform the collation and save
the results in an RDS file.
