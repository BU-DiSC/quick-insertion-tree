cmake_minimum_required(VERSION 3.20)
project(tree_analysis)

set(CMAKE_CXX_STANDARD 17)

add_executable(vanilla src/tree_analysis.cpp)
target_include_directories(vanilla PUBLIC src/bptree)

add_executable(profile src/tree_analysis.cpp)
target_include_directories(profile PUBLIC src/bptree)
target_compile_definitions(profile PRIVATE BENCHMARK)
