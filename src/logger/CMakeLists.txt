set(CMAKE_POSITION_INDEPENDENT_CODE ON)

add_library(acquire-logger
        logger.hh
        logger.cpp
)

target_include_directories(acquire-logger
        INTERFACE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)

set_target_properties(acquire-logger PROPERTIES
        MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>"
)

install(TARGETS acquire-logger
        LIBRARY DESTINATION lib
)