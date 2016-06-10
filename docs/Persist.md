# Persist

The `persist` and `cache` commands allow you to store the current
dataset on disk or in memory to avoid redundant computation and
improve the performance of Hail pipelines.

`persist` command line option:
 - `-s | --storage-level <storage level>` -- how to store the current dataset.

Storage level can be one of:
 - NONE
 - DISK_ONLY
 - DISK_ONLY_2
 - MEMORY_ONLY
 - MEMORY_ONLY_2
 - MEMORY_ONLY_SER
 - MEMORY_ONLY_SER_2
 - MEMORY_AND_DISK
 - MEMORY_AND_DISK_2
 - MEMORY_AND_DISK_SER
 - MEMORY_AND_DISK_SER_2
 - OFF_HEAP

`cache` is an alias for `persist -s MEMORY_ONLY`.  Most users will
want `MEMORY_AND_DISK`.
