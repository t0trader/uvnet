add_executable(app_test_protocol test_protocol.cpp)
add_executable(app_test_session test_session.cpp)

foreach(app_test IN ITEMS app_test_protocol app_test_session)
  target_link_libraries(${app_test} PRIVATE client_core)
  target_include_directories(${app_test} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
  target_compile_options(${app_test} PRIVATE ${APP_WARNINGS})
endforeach()

add_test(NAME app_protocol COMMAND app_test_protocol)
add_test(NAME app_session COMMAND app_test_session)
