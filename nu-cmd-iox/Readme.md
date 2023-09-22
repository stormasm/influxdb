
This crate contains the commands that interact with
[influxdb-iox](https://github.com/influxdata/influxdb_iox)

The commands include

* ioxnamespace
* ioxsql
* ioxwrite
* ioxwritefile

To see more details simply type this command:

```rust
help --find iox
```

To get up and running

```rust
cd influxdb_iox
cd nushell
cargo build
cd influxdb_iox
./target/debug/nu
```
