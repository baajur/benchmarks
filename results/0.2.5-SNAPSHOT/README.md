# Testing notes

- Spark results are with default config
- Reduced Ballista csv reader batch sizes from 1MM to 1000 in Rust and JVM to keep memory down. Spark is row based and has no equivalent config.
- Memory was unconstrained for all tests so ignore memory amount in filenames



