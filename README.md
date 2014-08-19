
Benchmark all the S3 regions
============================

```bash
go install github.com/lox/s3-bench
s3-bench -payload=5000 -runs=3 
```

To cleanup previously failed runs:

```bash
aws s3 ls | awk '{print $3}' | grep s3-bench | xargs -n1 -p -I {} aws s3 rm --recursive s3://{}/
aws s3 ls | awk '{print $3}' | grep s3-bench | xargs -n1 -p -I {} aws s3 rb s3://{}/
```

The above will prompt before each delete. Obviously any buckets that contain `s3-bench` will be deleted, so be careful!