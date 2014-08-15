
Benchmark all the S3 regions
============================

```bash
# benchmark a 5MB payload, max time of 60s per test
go test -v -bench=. -benchtime=60s -bench.payload=5000 
```
