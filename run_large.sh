for config in $(ls -v configs); do
  cp configs/"$config" config.toml
  ./workload_large.sh "$1"
done
