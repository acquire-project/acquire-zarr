add_library(acquire-logger-obj OBJECT
        logger.hh
        logger.cpp
)

set_target_properties(acquire-logger-obj PROPERTIES
        POSITION_INDEPENDENT_CODE ON
        MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>"
)

target_include_directories(acquire-logger-obj
        PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
)