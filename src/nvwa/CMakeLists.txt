set(the_description "Nvwa")

set(nvwa_hdrs
    ${CMAKE_CURRENT_LIST_DIR}/debug_new.h
    ${CMAKE_CURRENT_LIST_DIR}/_nvwa.h
    ${CMAKE_CURRENT_LIST_DIR}/c++11.h
    ${CMAKE_CURRENT_LIST_DIR}/fast_mutex.h
    ${CMAKE_CURRENT_LIST_DIR}/static_assert.h
    )

set(nvwa_srcs
    ${CMAKE_CURRENT_LIST_DIR}/debug_new.cpp
)

add_library(nvwa ${nvwa_srcs} ${nvwa_hdrs})

