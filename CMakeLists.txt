cmake_minimum_required(VERSION 3.22)
project(tree_analysis)

set(CMAKE_CXX_STANDARD 17)

add_executable(tree_analysis src/tree_analysis.cpp)
target_include_directories(tree_analysis PUBLIC src/bamboofilters src/bptree)
target_compile_options(tree_analysis PUBLIC "-mavx2")
