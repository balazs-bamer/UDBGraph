cmake_minimum_required(VERSION 3.0)

project( udbgraph )

# The version number.
set (UDBGRAPH_VERSION_MAJOR 0)
set (UDBGRAPH_VERSION_MINOR 1)
 
# configure a header file to pass some of the CMake settings
# to the source code

set(USE_NVWA "OFF" CACHE BOOL "If 1, use NVWA library to catch new-delete memory leaks.")
set(UPSCALEDB_INCLUDE "/usr/local/src/upscaledb-2.1.12/include" CACHE STRING "UpscaleDB header files location.")

set(UDBGRAPH_BINARY_DIR "${CMAKE_CURRENT_LIST_DIR}/../bin")
set(UDBGRAPH_SOURCE_DIR "${CMAKE_CURRENT_LIST_DIR}")

configure_file (
        "${UDBGRAPH_SOURCE_DIR}/udbgraph_config.h.in"
        "${UDBGRAPH_BINARY_DIR}/udbgraph_config.h"
)

set(include_dirs
	"${UDBGRAPH_BINARY_DIR}"
	"${UPSCALEDB_INCLUDE}"
)

if(USE_NVWA)
	set(include_dirs ${include_dirs} "${UDBGRAPH_SOURCE_DIR}/nvwa")
endif()

include_directories( ${include_dirs} )

set(udbgraph_hdrs
    "${UDBGRAPH_BINARY_DIR}/udbgraph_config.h"
)

set(udbgraph_srcs
    "${UDBGRAPH_SOURCE_DIR}/util.cpp"
    "${UDBGRAPH_SOURCE_DIR}/udbgraph.cpp"
    "${UDBGRAPH_SOURCE_DIR}/exception.cpp"
    "${UDBGRAPH_SOURCE_DIR}/serializer.cpp"
)

add_library(udbgraph_static STATIC ${udbgraph_srcs} ${udbgraph_hdrs})
add_library(udbgraph_shared SHARED ${udbgraph_srcs} ${udbgraph_hdrs})

set_target_properties(udbgraph_static udbgraph_shared PROPERTIES 
LIBRARY_OUTPUT_DIRECTORY ${UDBGRAPH_BINARY_DIR} 
ARCHIVE_OUTPUT_DIRECTORY ${UDBGRAPH_BINARY_DIR} 
OUTPUT_NAME "udbgraph"
)
