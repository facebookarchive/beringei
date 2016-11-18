#
# Find libglog
#
#  LIBGLOG_INCLUDE_DIR - where to find glog/logging.h, etc.
#  LIBGLOG_LIBRARY     - List of libraries when using libglog.
#  LIBGLOG_FOUND       - True if libglog found.
#

find_path(LIBGLOG_INCLUDE_DIR glog/logging.h)
find_library(LIBGLOG_LIBRARY glog)

set(LIBGLOG_LIBRARIES ${LIBGLOG_LIBRARY})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
    LIBGLOG DEFAULT_MSG LIBGLOG_INCLUDE_DIR LIBGLOG_LIBRARIES)

mark_as_advanced(LIBGLOG_INCLUDE_DIR LIBGLOG_LIBRARIES LIBGLOG_FOUND)

if(LIBGLOG_FOUND AND NOT LIBGLOG_FIND_QUIETLY)
    message(STATUS "LIBGLOG: ${LIBGLOG_INCLUDE_DIR}")
endif()