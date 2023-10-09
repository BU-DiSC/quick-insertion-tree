cmake_minimum_required(VERSION 3.20)
project(tree_analysis)

set(CMAKE_CXX_STANDARD 17)

add_executable(simple src/tree_analysis.cpp)
target_compile_definitions(simple PRIVATE INMEMORY)
target_compile_definitions(simple PRIVATE BENCHMARK)

add_executable(tail src/tree_analysis.cpp)
target_compile_definitions(tail PRIVATE TAIL_FAT)
target_compile_definitions(tail PRIVATE INMEMORY)
target_compile_definitions(tail PRIVATE BENCHMARK)

add_executable(lil src/tree_analysis.cpp)
target_compile_definitions(lil PRIVATE LIL_FAT)
target_compile_definitions(lil PRIVATE INMEMORY)
target_compile_definitions(lil PRIVATE BENCHMARK)

add_executable(plain_lol src/tree_analysis.cpp)
target_compile_definitions(plain_lol PRIVATE LOL_FAT)
target_compile_definitions(plain_lol PRIVATE INMEMORY)
target_compile_definitions(plain_lol PRIVATE LOL_RESET)
target_compile_definitions(plain_lol PRIVATE BENCHMARK)

add_executable(lol src/tree_analysis.cpp)
target_compile_definitions(lol PRIVATE LOL_FAT)
target_compile_definitions(lol PRIVATE INMEMORY)
target_compile_definitions(lol PRIVATE VARIABLE_SPLIT)
target_compile_definitions(lol PRIVATE REDISTRIBUTE)
target_compile_definitions(lol PRIVATE DOUBLE_IQR)
target_compile_definitions(lol PRIVATE LOL_RESET)
target_compile_definitions(lol PRIVATE BENCHMARK)
