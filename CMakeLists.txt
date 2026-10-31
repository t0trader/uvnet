cmake_minimum_required(VERSION 3.20)

project(uvnet VERSION 0.1.0 LANGUAGES CXX)

option(UVNET_BUILD_TESTS "构建 uvnet 测试" ON)
option(UVNET_BUILD_BENCHMARKS "构建 uvnet 性能基准" OFF)
option(UVNET_BUILD_SAMPLES "构建 uvnet 示例" OFF)

add_library(uvnet STATIC
  src/address.cpp
  src/async.cpp
  src/error.cpp
  src/loop.cpp
  src/resolver.cpp
)

if(WIN32)
  enable_language(C)
  target_sources(uvnet PRIVATE
    src/loop_win.cpp
    src/poller_win_ready.cpp
    src/tcp_win.cpp
    src/tcp_win_ready.cpp
    src/udp_win.cpp
    src/udp_win_ready.cpp
    src/detail/wepoll/wepoll.c
  )
  # 第三方 wepoll 源码不在本项目的警告策略内
  if(MSVC)
    set_source_files_properties(src/detail/wepoll/wepoll.c PROPERTIES COMPILE_OPTIONS "/w")
  endif()
  target_compile_definitions(uvnet PRIVATE
    WIN32_LEAN_AND_MEAN
    NOMINMAX
    _WIN32_WINNT=0x0A00
  )
  target_link_libraries(uvnet PUBLIC ws2_32 mswsock)
elseif(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  target_sources(uvnet PRIVATE
    src/loop_linux.cpp
    src/tcp_linux.cpp
    src/udp_linux.cpp
  )
  target_compile_definitions(uvnet PRIVATE _GNU_SOURCE)
else()
  message(FATAL_ERROR "uvnet 目前仅支持 Windows 与 Linux")
endif()

target_include_directories(uvnet
  PUBLIC  ${CMAKE_CURRENT_SOURCE_DIR}/include
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src
)

target_compile_features(uvnet PUBLIC cxx_std_23)

set_target_properties(uvnet PROPERTIES
  CXX_EXTENSIONS OFF
)

find_package(Threads REQUIRED)
target_link_libraries(uvnet PUBLIC Threads::Threads)

if(MSVC)
  target_compile_options(uvnet PRIVATE
    /W4
    /permissive-
    /utf-8
    /EHsc
    /Zc:__cplusplus
  )
else()
  target_compile_options(uvnet PRIVATE
    -Wall
    -Wextra
    -Wpedantic
  )
endif()

if(UVNET_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()

if(UVNET_BUILD_BENCHMARKS)
  add_subdirectory(benchmarks)
endif()

if(UVNET_BUILD_SAMPLES)
  add_subdirectory(samples)
endif()
