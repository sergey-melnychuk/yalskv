YALSKV
======

# Yet Another Log-Structured Key-Value storage

```
$ cargo clean && cargo build --release && ./target/release/main
<snip>
N=1000000 limit=33554432
insert: ok (ms=5847 op=171027 kb=25386)
reduce: ok (ms=14600 op=68493 kb=10166)
lookup: ok (ms=1775 op=563380 kb=83626)
sorted: ok (ms=1565 op=638977 kb=94848)
remove: ok (ms=4110 op=243309 kb=36116)
reduce: ok (ms=15332 op=65223 kb=9681)
```
