#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <random>

#include "bench_bptree.h"
#include "bptree/config.h"
#include "index_bench.h"
#include "spdlog/spdlog.h"

using key_type = unsigned;
using value_type = unsigned;

using namespace std;

std::vector<int> generateRandomNumbers(int n, bool shuffle = true, int seed = -1)
{
    std::vector<int> numbers;
    for (int i = 0; i < n; i++)
    {
        numbers.push_back(i);
    }

    if (shuffle)
    {
        spdlog::info("shuffling input");
        if (seed >= 0)
        {
            std::mt19937 g(seed);
            std::shuffle(numbers.begin(), numbers.end(), g);
        }
        else
        {
            std::random_device rd;
            std::mt19937 g(rd());
            std::shuffle(numbers.begin(), numbers.end(), g);
        }
    }

    return numbers;
}

std::vector<int> generateRandomLookups(int n, int num_lookups, int seed)
{
    std::vector<int> lookups;
    std::mt19937 rng(seed); // Random number generator with seed
    std::uniform_int_distribution<int> dist(0,
                                            n - 1); // Distribution from 1 to n

    std::cout << "Random numbers: ";
    for (int i = 0; i < num_lookups; ++i)
    {
        lookups.push_back(dist(rng));
    }
    return lookups;
}

std::vector<int> sorted_indices(const std::vector<int> &v)
{
    std::vector<int> idx(v.size());
    std::iota(idx.begin(), idx.end(), 0);

    std::sort(idx.begin(), idx.end(), [&v](size_t i1, size_t i2) { return v[i1] < v[i2]; });

    return idx;
}

std::size_t cmp(const key_type &max, const key_type &min)
{
    return max - min;
}

unsigned long long do_experiment(vector<int> &input_stream, vector<int> lookups, string results_file, int n,
                                 int num_trials, int &depth)
{
    spdlog::info("Executing btree analysis on with n = {}, num_lookups = {}, "
                 "and "
                 "num_trials = {}",
                 n, lookups.size(), num_trials);
    auto config_file = "config.toml";
    auto tree_dat = "tree.dat";

    Config conf(config_file);
    BlockManager manager(tree_dat, conf.blocks_in_memory);

    manager.reset();
    index_bench::BPTreeIndex<key_type, value_type> tree(cmp, manager);

    spdlog::info("Inserting {} elements into the tree", n);
    for (int i = 0; i < n; i++)
    {
        tree.insert(input_stream[i], i);
    }

    depth = tree._tree.get_depth();
    spdlog::info("Depth of the tree: {}", depth);

    spdlog::info("Executing {} trials with {} lookups each", num_trials, lookups.size());
    unsigned long long time_taken_btree = 0;
    for (int i = 0; i < num_trials; i++)
    {
        unsigned long long time_taken_for_trial = 0;
        int c = 0;
        for (int j = 0; j < lookups.size(); j++)
        {
            auto start = std::chrono::high_resolution_clock::now();
            bool res = tree.contains(lookups[j]);
            auto stop = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
            time_taken_for_trial += duration.count();
            if (!res)
                c++;
            spdlog::info("Time taken for trial {} - lookup {}, looking up {}: {} ns", i, j, lookups[j],
                         duration.count());
        }
        spdlog::info("Trial {} - {} lookups, {} not found", i, lookups.size(), c);
        // report time taken by btree per lookup for this trial
        time_taken_btree += (time_taken_for_trial / lookups.size());
    }

    // first get time per trial
    time_taken_btree /= (double)num_trials;

    spdlog::info("Avg. time taken per lookup per experiment: {} nanoseconds", time_taken_btree);

    return time_taken_btree;
}

int main(int argc, char **argv)
{
    int n = atoi(argv[1]);

    // get num_trials from arguments
    int num_trials = atoi(argv[2]);

    string results_file = argv[3];

    int num_lookups = atoi(argv[4]);

    int seed = atoi(argv[5]);

    bool random_input = atoi(argv[6]);

    std::cout << "Writing results to: " << results_file << std::endl;

    try
    {
        vector<int> input_stream = generateRandomNumbers(n, random_input, seed);
        vector<int> lookups = generateRandomLookups(n, num_lookups, seed); // generate random
                                                                           // lookups
        unsigned long long time = 0;
        string tree_name = "";
        int depth = 0;
        char c;
        uint64_t size = 0;

        long long duration_wt = 0;
        long num_access_calls = 0;

        // bp_tree<key_type, value_type> tree(cmp, manager);

        time = do_experiment(input_stream, lookups, results_file, n, num_trials, depth);

        tree_name = "btree";
        spdlog::info("size = {}", size);
        spdlog::info("time received=  {}", time);

        fstream results(results_file, std::fstream::in | std::fstream::out | std::fstream::app);
        // results.open(results_file, ios::app);
        if (!results.is_open())
        {
            cout << "No such file found";
        }
        results << n << ", " << tree_name << ", " << time << ", " << depth << endl;
        results.close();
    }
    catch (exception &e)
    {
        std::cout << "Exception caught: ";
        cout << e.what() << endl;
        return 1;
    }

    return 0;
}
