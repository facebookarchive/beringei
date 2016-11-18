#
# - Try to find Facebook zstd library
# This will define
# ZSTD_FOUND
# ZSTD_INCLUDE_DIR
# ZSTD_LIBRARIES
#

find_path(
    ZSTD_INCLUDE_DIR
    NAMES "zstd.h"
    HINTS
        "/usr/local/facebook/include"
)

find_library(
    ZSTD_LIBRARY
    NAMES zstd
    HINTS
        "/usr/local/facebook/lib"
)

set(ZSTD_LIBRARIES ${ZSTD_LIBRARY})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
    ZSTD DEFAULT_MSG ZSTD_INCLUDE_DIR ZSTD_LIBRARIES)

mark_as_advanced(ZSTD_INCLUDE_DIR ZSTD_LIBRARIES ZSTD_FOUND)

if(ZSTD_FOUND AND NOT ZSTD_FIND_QUIETLY)
    message(STATUS "ZSTD: ${ZSTD_INCLUDE_DIR}")
endif()
