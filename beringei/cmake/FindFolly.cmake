#
# - Try to find Facebook folly library
# This will define
# FOLLY_FOUND
# FOLLY_INCLUDE_DIR
# FOLLY_LIBRARIES
#

find_package(DoubleConversion REQUIRED)

find_path(
    FOLLY_INCLUDE_DIR
    NAMES "folly/String.h"
    HINTS
        "/usr/local/facebook/include"
)

find_library(
    FOLLY_LIBRARY
    NAMES folly
    HINTS
        "/usr/local/facebook/lib"
)

set(FOLLY_LIBRARIES ${FOLLY_LIBRARY} ${DOUBLE_CONVERSION_LIBRARY})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
    FOLLY DEFAULT_MSG FOLLY_INCLUDE_DIR FOLLY_LIBRARIES)

mark_as_advanced(FOLLY_INCLUDE_DIR FOLLY_LIBRARIES FOLLY_FOUND)

if(FOLLY_FOUND AND NOT FOLLY_FIND_QUIETLY)
    message(STATUS "FOLLY: ${FOLLY_INCLUDE_DIR}")
endif()
