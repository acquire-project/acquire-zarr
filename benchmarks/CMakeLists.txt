set(project acquire-zarr)

set(tgt acquire-zarr-benchmark)
add_executable(${tgt} benchmark.cpp)
target_compile_definitions(${tgt} PUBLIC "TEST=\"${tgt}\"")
set_target_properties(${tgt} PROPERTIES
        MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>"
)
target_include_directories(${tgt} PRIVATE
        ${PROJECT_SOURCE_DIR}/include
        ${PROJECT_SOURCE_DIR}/src/logger
)
target_link_libraries(${tgt} PRIVATE
        acquire-zarr
        nlohmann_json::nlohmann_json
        miniocpp::miniocpp
)
