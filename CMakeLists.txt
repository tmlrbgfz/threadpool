project(threadpool)
cmake_minimum_required(VERSION 3.20)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED TRUE)

add_library(threadpool INTERFACE)
cmake_path(SET THREADPOOL_INCLUDE_ROOT ".." )
cmake_path(ABSOLUTE_PATH THREADPOOL_INCLUDE_ROOT)
target_include_directories(threadpool INTERFACE ${THREADPOOL_INCLUDE_ROOT})

target_sources(threadpool
    INTERFACE
        ThreadPool.hpp
        ThreadPoolPolicies.hpp)

if(PROJECT_IS_TOP_LEVEL)
  add_executable(tests ThreadPoolTest.cpp)
  add_test(NAME ThreadPoolTest COMMAND tests)
endif()
