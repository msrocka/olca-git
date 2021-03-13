loaded DB tree  in:   1.058 sec
synced the tree in: 258.322 sec

loaded DB tree in 1.582 sec
synced the tree in 452.319 sec

loaded DB tree in 1.492 sec
synced the tree in 430.702 sec

loaded DB tree in 2.012 sec
synced the tree in 249.232 sec

loaded DB tree in 1.516 sec
synced the tree in 267.347 sec

loaded DB tree in 1.545 sec
synced the tree in 298.523 sec

loaded DB tree in 1.741 sec
synced the tree in 299.584 sec

using a cached thread pool:
loaded DB tree in 1.508 sec
synced the tree in 269.724 sec

using the pack inserter
loaded DB tree in 1.453 sec
synced the tree in 180.076 sec

loaded DB tree in 1.473 sec
synced the tree in 177.578 sec

build a tree: https://stackoverflow.com/questions/22320996/jgit-bare-commit-tree-construction
traverse a tree: https://stackoverflow.com/questions/19941597/use-jgit-treewalk-to-list-files-and-folders

https://stackoverflow.com/questions/37323534/why-is-jgits-add-command-very-slow

* using a shared `ObjectInserter` does not make things faster, it is even slower
* one thread for loading and converting and one thread for writing per sub-tree
  seems to be the fastest configuration
* using a fixed size thread pool is slower than creating new threads for loading
  and writing per sub-tree
* using a cached thread pool seems to be fine
* the packed inserter can be reused and is much faster; we can even disable the
  check for existence here
