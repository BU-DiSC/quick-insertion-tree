cmake_minimum_required(VERSION 3.20)
project(tree_analysis)

set(CMAKE_CXX_STANDARD 17)

option(DEBUG "Debug mode on or off" OFF)
if(${DEBUG})
    set(CMAKE_BUILD_TYPE "Debug")
else()
    set(CMAKE_BUILD_TYPE "Release")
endif()

find_package(Git QUIET)
if(GIT_FOUND AND EXISTS "${PROJECT_SOURCE_DIR}/.git")
# Update submodules as needed
    option(GIT_SUBMODULE "Check submodules during build" ON)
    if(GIT_SUBMODULE)
        message(STATUS "Submodule update")
        execute_process(COMMAND ${GIT_EXECUTABLE} submodule update --init --recursive
                        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
                        RESULT_VARIABLE GIT_SUBMOD_RESULT)
        if(NOT GIT_SUBMOD_RESULT EQUAL "0")
            message(FATAL_ERROR "git submodule update --init --recursive failed with ${GIT_SUBMOD_RESULT}, please checkout submodules")
        endif()
    endif()
endif()

if(NOT EXISTS "${PROJECT_SOURCE_DIR}/external/utilities/CMakeLists.txt")
    message(FATAL_ERROR "Utilities submodules were not downloaded! GIT_SUBMODULE was turned off or failed. Please update submodules and try again.")
endif()

add_subdirectory(external)

add_subdirectory(external/utilities)

add_executable(simple src/tree_analysis.cpp)
target_compile_definitions(simple PRIVATE INMEMORY)
#target_compile_definitions(simple PRIVATE BENCHMARK)

add_executable(tail src/tree_analysis.cpp)
target_compile_definitions(tail PRIVATE TAIL_FAT)
target_compile_definitions(tail PRIVATE INMEMORY)
#target_compile_definitions(tail PRIVATE BENCHMARK)

add_executable(lil src/tree_analysis.cpp)
target_compile_definitions(lil PRIVATE LIL_FAT)
target_compile_definitions(lil PRIVATE INMEMORY)
#target_compile_definitions(lil PRIVATE BENCHMARK)

add_executable(lol src/tree_analysis.cpp)
target_compile_definitions(lol PRIVATE LOL_FAT)
target_compile_definitions(lol PRIVATE INMEMORY)
#target_compile_definitions(lol PRIVATE BENCHMARK)

add_executable(lol_v src/tree_analysis.cpp)
target_compile_definitions(lol_v PRIVATE LOL_FAT)
target_compile_definitions(lol_v PRIVATE VARIABLE_SPLIT)
target_compile_definitions(lol_v PRIVATE INMEMORY)
#target_compile_definitions(lol_v PRIVATE BENCHMARK)

add_executable(lol_vi src/tree_analysis.cpp)
target_compile_definitions(lol_vi PRIVATE LOL_FAT)
target_compile_definitions(lol_vi PRIVATE VARIABLE_SPLIT)
target_compile_definitions(lol_vi PRIVATE INMEMORY)
#target_compile_definitions(lol_vi PRIVATE BENCHMARK)

add_executable(lol_vir src/tree_analysis.cpp)
target_compile_definitions(lol_vir PRIVATE LOL_FAT)
target_compile_definitions(lol_vir PRIVATE VARIABLE_SPLIT)
target_compile_definitions(lol_vir PRIVATE REDISTRIBUTE)
target_compile_definitions(lol_vir PRIVATE INMEMORY)
#target_compile_definitions(lol_vir PRIVATE BENCHMARK)

add_executable(quit src/tree_analysis.cpp)
target_compile_definitions(quit PRIVATE LOL_FAT)
target_compile_definitions(quit PRIVATE VARIABLE_SPLIT)
target_compile_definitions(quit PRIVATE REDISTRIBUTE)
target_compile_definitions(quit PRIVATE LOL_RESET)
target_compile_definitions(quit PRIVATE INMEMORY)
#target_compile_definitions(quit PRIVATE BENCHMARK)

add_executable(sabtree src/test_satree.cpp)
target_compile_definitions(sabtree PRIVATE INMEMORY)
target_compile_definitions(sabtree PRIVATE OSMTIMER)
target_compile_definitions(sabtree PRIVATE SPLIT80)
target_compile_definitions(sabtree PRIVATE ZONES)
target_compile_definitions(sabtree PRIVATE GLOBALBF)
target_compile_definitions(sabtree PRIVATE SUBBFS)
target_compile_definitions(sabtree PRIVATE SPDLOG_STATS)
target_compile_definitions(sabtree PRIVATE SPDLOG_COUNTERS)

target_compile_options(sabtree PUBLIC 
$<$<BOOL:${DEBUG}>:-g>)

target_compile_options(quit PUBLIC 
$<$<BOOL:${DEBUG}>:-g>)

target_include_directories(sabtree PUBLIC
    ${PROJECT_SOURCE_DIR}/external
)

target_link_libraries(sabtree PUBLIC
    spdlog::spdlog
    cxxopts
    tabulate
    utilities
)
