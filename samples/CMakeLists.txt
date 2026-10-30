add_executable(uvnet_demo_client demo_client.cpp)
target_link_libraries(uvnet_demo_client PRIVATE uvnet)

if(MSVC)
  target_compile_options(uvnet_demo_client PRIVATE /W4 /permissive- /utf-8 /EHsc)
else()
  target_compile_options(uvnet_demo_client PRIVATE -Wall -Wextra -Wpedantic)
endif()
