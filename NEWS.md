# piecemeal 0.2.0

* Documentation improvements.

* Time-consuming interactive commands now show progress bars via
  {cli}.

* Calling `sim$status()` also includes the ETA, since there is no
  additional cost.

* New method, `sim$consolidate()`, to consolidate successful run
  results into an SQLite database, which other methods can access
  transparently. Among other things, it helps prevent inode exhaustion
  and significantly speeds up `sim$status()` and
  `sim$eta()`. Consolidation can take place while the simulation is
  running.

* Default cluster settings now allow for a longer timeout.

* A file descriptor leak in {filelock} has been mitigated.

# piecemeal 0.1.0

* Initial CRAN submission.
