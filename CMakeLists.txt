cmake_minimum_required(VERSION 3.20)
project(tree_analysis)

set(CMAKE_CXX_STANDARD 17)

add_executable(simple src/tree_analysis.cpp)
target_compile_definitions(simple PRIVATE INMEMORY)

add_executable(tail src/tree_analysis.cpp)
target_compile_definitions(tail PRIVATE LOL_FAT)
target_compile_definitions(tail PRIVATE LIL_FAT)
target_compile_definitions(tail PRIVATE TAIL_FAT)
target_compile_definitions(tail PRIVATE INMEMORY)

add_executable(lil src/tree_analysis.cpp)
target_compile_definitions(lil PRIVATE LIL_FAT)
target_compile_definitions(lil PRIVATE INMEMORY)

add_executable(lol src/tree_analysis.cpp)
target_compile_definitions(lol PRIVATE LOL_FAT)
target_compile_definitions(lol PRIVATE INMEMORY)

add_executable(lollipop src/tree_analysis.cpp)
target_compile_definitions(lollipop PRIVATE LOL_FAT)
target_compile_definitions(lollipop PRIVATE LIL_FAT)
target_compile_definitions(lollipop PRIVATE INMEMORY)
