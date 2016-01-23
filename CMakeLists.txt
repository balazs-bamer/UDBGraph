cmake_minimum_required(VERSION 2.8)

project( udbgraph_debug )

# configure a header file to pass some of the CMake settings
# to the source code

set(USE_NVWA "OFF" CACHE BOOL "If 1, use NVWA library to catch new-delete memory leaks.")
set(DEBUG_STDOUT "OFF" CACHE BOOL "If 1, output goes to stdout, if 0, into DEBUG_LOC.")
set(DEBUG_LOC "/tmp/diag.log" CACHE STRING "Debug output location.")
set(UPSCALEDB_INCLUDE "/usr/local/src/upscaledb-2.1.12/include" CACHE STRING "UpscaleDB header files location.")

add_subdirectory("src")

set(DEBUG_BINARY_DIR "${CMAKE_CURRENT_LIST_DIR}/bin")
set(UDBGRAPH_INCLUDE_DIR "${CMAKE_CURRENT_LIST_DIR}/src")
set(DEBUG_SOURCE_DIR "${CMAKE_CURRENT_LIST_DIR}/debug")

configure_file (
    "${DEBUG_SOURCE_DIR}/debug_config.h.in"
    "${DEBUG_BINARY_DIR}/debug_config.h"
)

set(include_dirs
	"${DEBUG_BINARY_DIR}"
	"${UDBGRAPH_INCLUDE_DIR}"
	"${UPSCALEDB_INCLUDE}"
)

if(USE_NVWA)
	set(include_dirs ${include_dirs} "${DEBUG_SOURCE_DIR}/../src/nvwa")
endif()

include_directories( ${include_dirs} )

set(debug_hdrs
    "${DEBUG_BINARY_DIR}/debug_config.h"
)

set(debug1_srcs "${CMAKE_CURRENT_LIST_DIR}/debug/debug1.cpp")
set(debug2_srcs "${CMAKE_CURRENT_LIST_DIR}/debug/debug2.cpp")
set(dumpdb_srcs "${CMAKE_CURRENT_LIST_DIR}/debug/dumpdb.cpp")

if(USE_NVWA)
	set(debug1_srcs ${debug1_srcs} "${DEBUG_SOURCE_DIR}/../src/nvwa/debug_new.cpp")
	set(debug2_srcs ${debug2_srcs} "${DEBUG_SOURCE_DIR}/../src/nvwa/debug_new.cpp")
	set(dumpdb_srcs ${dumpdb_srcs} "${DEBUG_SOURCE_DIR}/../src/nvwa/debug_new.cpp")
endif()
	
set(debug_libraries "/usr/local/lib/libupscaledb.so.1.0.0" udbgraph_static
)

add_executable(debug1 ${debug_hdrs} ${debug1_srcs} )
add_executable(debug2 ${debug_hdrs} ${debug2_srcs} )
add_executable(dumpdb ${debug_hdrs} ${dumpdb_srcs} )

target_link_libraries(debug1 ${debug_libraries} )
target_link_libraries(debug2 ${debug_libraries} )
target_link_libraries(dumpdb ${debug_libraries} )

set_target_properties(debug1 PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${DEBUG_BINARY_DIR} )
set_target_properties(debug2 PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${DEBUG_BINARY_DIR} )
set_target_properties(dumpdb PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${DEBUG_BINARY_DIR} )

