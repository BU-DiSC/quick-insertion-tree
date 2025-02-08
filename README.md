# Quick Insertion Tree
Source code for EDBT 2025 paper: "QuIT your B+-tree for the Quick Insertion Tree".

You can cite our paper using:
```
@inproceedings{Raman2025QuITYourBT,
 author = {Aneesh Raman and Konstantinos Karatsenidis and Shaolin Xie and Matthaios Olma and Subhadeep Sarkar and Manos Athanassoulis},
 booktitle = {Proceedings of the International Conference on Extending Database Technology (EDBT)},
 doi = {10.48786/EDBT.2025.36},
 pages = {451--463},
 title = {QuIT your B+-tree for the Quick Insertion Tree},
 url = {https://doi.org/10.48786/edbt.2025.36},
 year = {2025}
}
```

## About
This repository contains the source code for the prototype B+-tree and Quick Insertion Tree (QuIT) implementations. 
In the current version, both prototypes are generic, but the supporting application files only support integer data type. 
At present, the application files use the same value for both key and value of each entry but can be extended as needed. 

The prototypes can work on disk, as well as purely in memory, when allocated enough memory to the bufferpool. 
The buffer pool allocation is given in terms of number of blocks where each block is 4KB. 
For example, if you use an allocation of 1M blocks, then you are allocating 1M*4KB = 4GB of memory for the tree data structure.
These settings can be changed in the `config.toml` file. 

## How To Run
Below are the steps to run a basic test for the prototypes 

### Generating Ingestion Workload
Use the sortedness data generator from this repo: https://github.com/BU-DiSC/bods to generate ingestion keys (can specify payload size=0 to generate only keys).
As mentioned above, the application files use the same value for both key and value of each entry (K,V pair). Note the path to the generated workload. Remember to generate 
the data as a binary file using the `--binary` flag. 

### Compiling
We use CMAKE to compile the code. 
1. Compile the code using the command `cmake -S . -B build`. This should create a build folder where the executables will be stored.
2. `cd build` to change to the build directory.
3. Use the `make <tree_type>` command to compile the code. `<tree_type>` can be either `simple` for the textbook B+-tree, `tail` for tail B+-tree,
   `lil` for the lil-B+-tree, or `quit` for the Quick Insertion Tree.
4. Run the executable with the command formatted like: `<executable> <output_file> <input_file>`
   
   For example, to run the B+-tree with a file called `sorted` stored in the same directory and print the output to a file called `results.csv`, we can use the command:

   `./simple results.csv sorted`

By default, all statistics and timing results are printed to the file specified in the `output_file` argument. 
