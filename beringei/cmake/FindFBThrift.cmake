#
# - Try to find Facebook fbthrift library
# This will define
# FBTHRIFT_FOUND
# FBTHRIFT_INCLUDE_DIR
# FBTHRIFT_LIBRARIES
#

find_package(OpenSSL REQUIRED)

find_path(
    FBTHRIFT_INCLUDE_DIR
    NAMES "thrift/lib/cpp2/Thrift.h"
    HINTS
        "/usr/local/facebook/include"
)

find_library(
    FBTHRIFT_CORE_LIBRARY
    NAMES thrift
    HINTS
        "/usr/local/facebook/lib"
)

find_library(
    FBTHRIFT_CPP2_LIBRARY
    NAMES thriftcpp2
    HINTS
        "/usr/local/facebook/lib"
)

find_library(
    FBTHRIFT_PROTOCOL_LIBRARY
    NAMES thriftprotocol
    HINTS
        "/usr/local/facebook/lib"
)

find_library(
    FBTHRIFT_Z_LIBRARY
    NAMES thriftz
    HINTS
        "/usr/local/facebook/lib"
)

set(FBTHRIFT_LIBRARIES
    ${FBTHRIFT_CORE_LIBRARY}
    ${FBTHRIFT_CPP2_LIBRARY}
    ${FBTHRIFT_PROTOCOL_LIBRARY}
    ${FBTHRIFT_Z_LIBRARY}
    ${OPENSSL_LIBRARIES}
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
    FBTHRIFT DEFAULT_MSG FBTHRIFT_INCLUDE_DIR FBTHRIFT_LIBRARIES)

mark_as_advanced(FBTHRIFT_INCLUDE_DIR FBTHRIFT_LIBRARIES FBTHRIFT_FOUND)

if(FBTHRIFT_FOUND AND NOT FBTHRIFT_FIND_QUIETLY)
    message(STATUS "FBTHRIFT: ${FBTHRIFT_INCLUDE_DIR}")
endif()
