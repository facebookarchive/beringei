#
# - Try to find GFlags
# This will define
# GFLAGS_FOUND
# GFLAGS_INCLUDE_DIR
# GFLAGS_LIBRARIES
#

find_path(GFLAGS_INCLUDE_DIR "gflags/gflags.h")
find_library(GFLAGS_LIBRARY gflags)

set(GFLAGS_LIBRARIES ${GFLAGS_LIBRARY})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
    GFLAGS DEFAULT_MSG GFLAGS_INCLUDE_DIR GFLAGS_LIBRARIES)

mark_as_advanced(GFLAGS_INCLUDE_DIR GFLAGS_LIBRARIES GFLAGS_FOUND)

if(GFLAGS_FOUND AND NOT GFLAGS_FIND_QUIETLY)
    message(STATUS "GFLAGS: ${GFLAGS_INCLUDE_DIR}")
endif()