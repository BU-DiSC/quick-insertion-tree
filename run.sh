for config in $(ls -v configs); do
  cp configs/"$config" config.toml
  ./workload.sh "$1"
done
