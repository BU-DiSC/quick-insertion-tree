cmake_minimum_required(VERSION 3.20)
project(tree_analysis)

set(CMAKE_CXX_STANDARD 17)

add_executable(tree_analysis
        src/dual_tree.h
        src/dual_tree_knob.h
        src/collection.h
        src/betree.h
        src/betree_utils.h
        src/block_manager.h
        src/dist_detector.h
        src/dual_tree_knob.cpp
        src/lru_cache.h
        src/outlier_detector.h
        src/stdev_detector.h
        src/tree_analysis.cpp
        src/FastAppendBeTree.h
        )

target_link_libraries(tree_analysis stdc++fs)
