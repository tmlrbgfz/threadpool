# Threadpool
## Summary
This library intends to provide an easy way of creating a thread pool and
feeding tasks to it. There are no performance optimizations done yet.
This library is still work in progress.

## Features
The following features are provided:
* Adding tasks to the thread pool. These can have arbitrary arguments (which
  must be provided when adding the task to the thread pool) and return values
  (which will be provided via a std::future object).
* Specifying dependencies between tasks. A task can depend on a set of other,
  already created tasks and will only be run if all those tasks completed.

## Known limitations
* Things might get weird after 2^64 tasks were created.