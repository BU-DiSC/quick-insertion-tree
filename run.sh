cp configs/config1.toml config.toml
./workload.sh "$1"
cp configs/config2.toml config.toml
./workload.sh "$1"
cp configs/config4.toml config.toml
./workload.sh "$1"
cp configs/config8.toml config.toml
./workload.sh "$1"
cp configs/config16.toml config.toml
./workload.sh "$1"
