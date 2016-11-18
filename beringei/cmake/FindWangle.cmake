#
# - Try to find Facebook wangle library
# This will define
# WANGLE_FOUND
# WANGLE_INCLUDE_DIR
# WANGLE_LIBRARIES
#

find_package(Folly REQUIRED)

find_path(
    WANGLE_INCLUDE_DIR
    NAMES "wangle/channel/Pipeline.h"
    HINTS
        "/usr/local/facebook/include"
)

find_library(
    WANGLE_LIBRARY
    NAMES wangle
    HINTS
        "/usr/local/facebook/lib"
)

set(WANGLE_LIBRARIES ${WANGLE_LIBRARY} ${FOLLY_LIBRARIES})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
    WANGLE DEFAULT_MSG WANGLE_INCLUDE_DIR WANGLE_LIBRARIES)

mark_as_advanced(WANGLE_INCLUDE_DIR WANGLE_LIBRARIES WANGLE_FOUND)

if(WANGLE_FOUND AND NOT WANGLE_FIND_QUIETLY)
    message(STATUS "WANGLE: ${WANGLE_INCLUDE_DIR}")
endif()
