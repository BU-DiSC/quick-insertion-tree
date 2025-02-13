cmake_minimum_required(VERSION 3.20)
project(tree_analysis)

set(CMAKE_CXX_STANDARD 17)

add_executable(simple src/tree_analysis.cpp)
target_compile_definitions(simple PRIVATE INMEMORY)

add_executable(tail src/tree_analysis.cpp)
target_compile_definitions(tail PRIVATE TAIL_FAT)
target_compile_definitions(tail PRIVATE INMEMORY)

add_executable(lil src/tree_analysis.cpp)
target_compile_definitions(lil PRIVATE LIL_FAT)
target_compile_definitions(lil PRIVATE INMEMORY)

add_executable(lol src/tree_analysis.cpp)
target_compile_definitions(lol PRIVATE LOL_FAT)
target_compile_definitions(lol PRIVATE INMEMORY)

add_executable(lol_r src/tree_analysis.cpp)
target_compile_definitions(lol_r PRIVATE LOL_FAT)
target_compile_definitions(lol_r PRIVATE LOL_RESET)
target_compile_definitions(lol_r PRIVATE INMEMORY)

add_executable(lol_v src/tree_analysis.cpp)
target_compile_definitions(lol_v PRIVATE LOL_FAT)
target_compile_definitions(lol_v PRIVATE VARIABLE_SPLIT)
target_compile_definitions(lol_v PRIVATE INMEMORY)

add_executable(lol_vr src/tree_analysis.cpp)
target_compile_definitions(lol_vr PRIVATE LOL_FAT)
target_compile_definitions(lol_vr PRIVATE VARIABLE_SPLIT)
target_compile_definitions(lol_vr PRIVATE REDISTRIBUTE)
target_compile_definitions(lol_vr PRIVATE INMEMORY)

add_executable(quit src/tree_analysis.cpp)
target_compile_definitions(quit PRIVATE LOL_FAT)
target_compile_definitions(quit PRIVATE VARIABLE_SPLIT)
target_compile_definitions(quit PRIVATE REDISTRIBUTE)
target_compile_definitions(quit PRIVATE LOL_RESET)
target_compile_definitions(quit PRIVATE INMEMORY)
