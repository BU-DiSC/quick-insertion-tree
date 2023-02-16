cmake_minimum_required(VERSION 3.22)
project(tree_analysis)

set(CMAKE_CXX_STANDARD 17)

add_executable(vanilla src/tree_analysis.cpp)
target_include_directories(vanilla PUBLIC src/bptree)

add_executable(outofplace src/tree_analysis.cpp)
target_include_directories(outofplace PUBLIC src/bamboofilters src/bptree)
target_compile_options(outofplace PUBLIC "-mavx2")
target_compile_definitions(outofplace PRIVATE DUAL_FILTERS=1)

add_executable(inplace src/tree_analysis.cpp)
target_include_directories(inplace PUBLIC src/bamboofilters src/bptree)
target_compile_options(inplace PUBLIC "-mavx2")
target_compile_definitions(inplace PRIVATE DUAL_FILTERS=2)

add_executable(bamboo src/example.cpp)
target_include_directories(bamboo PUBLIC src/bamboofilters)
target_compile_options(bamboo PUBLIC "-mavx2")
