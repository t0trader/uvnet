cmake_minimum_required(VERSION 3.20)

project(client_app VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(CLIENT_APP_BUILD_TESTS "构建 client_app 测试" ON)

# ---------------------------------------------------------------------------
# 依赖：uvnet（源码引入）
#
# 默认取 uvnet 仓库根目录（samples/client_app 的上两级）；uvnet 放在别处时用
# -DUVNET_ROOT=<路径> 指定。
# ---------------------------------------------------------------------------
set(UVNET_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../.." CACHE PATH "uvnet 源码目录")
if(NOT EXISTS "${UVNET_ROOT}/CMakeLists.txt")
  message(FATAL_ERROR "找不到 uvnet：${UVNET_ROOT}（用 -DUVNET_ROOT=<路径> 指定）")
endif()

# 只构建库本体，跳过它自己的测试/基准/示例
set(UVNET_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(UVNET_BUILD_BENCHMARKS OFF CACHE BOOL "" FORCE)
set(UVNET_BUILD_SAMPLES OFF CACHE BOOL "" FORCE)
add_subdirectory("${UVNET_ROOT}" "${CMAKE_BINARY_DIR}/uvnet")

# ---------------------------------------------------------------------------
# 核心库：配置 / 日志 / 协议 / 平台适配 / 会话 / 回显服务器
# ---------------------------------------------------------------------------
add_library(client_core STATIC
  src/config.cpp
  src/echo_server.cpp
  src/logger.cpp
  src/platform.cpp
  src/protocol.cpp
  src/session.cpp
)

target_include_directories(client_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(client_core PUBLIC uvnet)
target_compile_features(client_core PUBLIC cxx_std_23)

# ---------------------------------------------------------------------------
# 可执行文件
# ---------------------------------------------------------------------------
add_executable(client_app src/main.cpp)
target_link_libraries(client_app PRIVATE client_core)

add_executable(echo_server tools/echo_server.cpp)
target_link_libraries(echo_server PRIVATE client_core)

if(MSVC)
  set(APP_WARNINGS /W4 /permissive- /utf-8 /EHsc /Zc:__cplusplus)
else()
  set(APP_WARNINGS -Wall -Wextra -Wpedantic)
endif()

foreach(app_target IN ITEMS client_core client_app echo_server)
  target_compile_options(${app_target} PRIVATE ${APP_WARNINGS})
endforeach()

if(CLIENT_APP_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()
