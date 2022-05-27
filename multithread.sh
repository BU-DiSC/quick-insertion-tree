for num_threads in 1 2 4 8 16 32 48; do
  "$@" --num_threads $num_threads
done
