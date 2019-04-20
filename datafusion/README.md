# DataFusion Benchmarks

- Executes aggregate query in parallel using a separate context per partition/thread
- Results from each partition are then loaded into a MemTable and a secondary query is executed on another context

Roadmap:

- [x] Manual parallel aggregate query
- [ ] DataFusion implementing parallel query
- [ ] Distributed query execution using Kubernetes
