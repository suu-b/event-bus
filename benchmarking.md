# Benchmarking

Roughly, benchmarking is the process of measuring and comparing the performance of different implementations of the same system or feature under different workloads.

For event bus, historically, I have tried to implement the system using two ways - one with redis pub/sub and the other using python multiprocessing library. I want to benchmark these two implementations to compare their performance.

Now, to do so - I may first need to have an alternate implementation of the app using these two.
Followed by this, I may need to collect timestamps of each of these at various stages of the request lifecycle.
Then, I may make use of pandas or maybe just regular python to calculate metrics for the comparison.

## Two alternate implementations
