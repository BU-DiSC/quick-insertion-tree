cmake_minimum_required(VERSION 3.20)
project(tree_analysis)

set(CMAKE_CXX_STANDARD 17)

add_executable(simple src/tree_analysis.cpp)
target_include_directories(simple PUBLIC src/bptree)

add_executable(lil src/tree_analysis.cpp)
target_include_directories(lil PUBLIC src/bptree)
target_compile_definitions(lil PRIVATE LIL_FAT)

add_executable(lol src/tree_analysis.cpp)
target_include_directories(lol PUBLIC src/bptree)
target_compile_definitions(lol PRIVATE LOL_FAT)

add_executable(fast src/tree_analysis.cpp)
target_include_directories(fast PUBLIC src/bptree)
target_compile_definitions(fast PRIVATE LOL_FAT)
target_compile_definitions(fast PRIVATE LIL_FAT)
