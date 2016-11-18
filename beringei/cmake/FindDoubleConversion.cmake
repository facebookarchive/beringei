#
# Finds libdouble-conversion.
#
# This module defines:
# DOUBLE_CONVERSION_INCLUDE_DIR
# DOUBLE_CONVERSION_LIBRARY
#

find_path(
    DOUBLE_CONVERSION_INCLUDE_DIR
    NAMES double-conversion.h
    HINTS
        /usr/include/double-conversion
        /usr/local/include/double-conversion
)
find_library(
    DOUBLE_CONVERSION_LIBRARY
    NAMES double-conversion
    HINTS
        /usr/local/double-conversion
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
    DOUBLE_CONVERSION DEFAULT_MSG
    DOUBLE_CONVERSION_INCLUDE_DIR DOUBLE_CONVERSION_LIBRARY)

mark_as_advanced(
    DOUBLE_CONVERSION_INCLUDE_DIR
    DOUBLE_CONVERSION_LIBRARY
    DOUBLE_CONVERSION_FOUND
)

if(DOUBLE_CONVERSION_FOUND AND NOT DOUBLE_CONVERSION_FIND_QUIETLY)
    message(STATUS "DOUBLE_CONVERSION: ${DOUBLE_CONVERSION_INCLUDE_DIR}")
endif()