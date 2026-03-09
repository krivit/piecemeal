# piecemeal 0.2.0

* Time-consuming interactive commands now show progress bars via
  {cli}.

* Calling `Piecemeal$status()` also includes the ETA, since there is
  no additional cost.

* New method, `Piecemeal$consolidate()`, to consolidate successful run
  results into an SQLite database, which other methods can access
  transparently. Among other things, it helps prevent inode exhaustion
  and significantly speeds up `Piecemeal$status()` and
  `Piecemeal$eta()`. Consolidation can take place while the simulation
  is running.

* Default cluster settings now allow for a longer timeout.

* A file descriptor leak in {filelock} has been mitigated.

* Documentation improvements.

# piecemeal 0.1.0

* Initial CRAN submission.
