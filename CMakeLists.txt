cmake_minimum_required(VERSION 3.22)
project(tree_analysis)

set(CMAKE_CXX_STANDARD 17)

add_compile_definitions(BPLUS)
add_compile_definitions(COUNTER)

add_executable(tree_analysis
        include/dual_btree/dual_tree.h
        include/dual_btree/dual_tree_knob.h
        include/collection.h
        src/betree.h
        src/block_manager.h
        src/dist_detector.h
        src/dual_tree_knob.cpp
        src/lru_cache.h
        src/outlier_detector.h
        src/stdev_detector.h
        src/tree_analysis.cpp
        )

target_include_directories(tree_analysis PRIVATE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include/dual_btree>
        $<INSTALL_INTERFACE:include/dual_btree>
        )
target_include_directories(tree_analysis PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
        )
