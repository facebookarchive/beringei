#
# - Try to find Facebook proxygen library
# This will define
# PROXYFGEN_FOUND
# PROXYGEN_INCLUDE_DIR
# PROXYGEN_LIBRARIES
#

find_package(Folly REQUIRED)
find_package(Wangle REQUIRED)

find_path(
  PROXYGEN_INCLUDE_DIR
  NAMES "proxygen/httpserver/HTTPServer.h"
  HINTS
    "/usr/local/facebook/include"
)

find_library(
  PROXYGEN_LIBRARY
  NAMES proxygenlib
  HINTS
    "/usr/local/facebook/lib"
  )

find_library(
  PROXYGEN_HTTP_SERVER
  NAMES proxygenhttpserver
  HINTS
    "/usr/local/facebook/lib"
)

set(PROXYGEN_LIBRARIES
  ${PROXYGEN_LIBRARY}
  ${PROXYGEN_HTTP_SERVER}
)

set(PROXYGEN_LIBRARIES ${PROXYGEN_LIBRARIES} ${FOLLY_LIBRARIES} ${WANGLE_LIBRARIES})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
  PROXYGEN DEFAULT_MSG PROXYGEN_INCLUDE_DIR PROXYGEN_LIBRARIES)

mark_as_advanced(PROXYGEN_INCLUDE_DIR PROXYGEN_LIBRARIES PROXYGEN_FOUND)

if(PROXYGEN_FOUND AND NOT PROXYGEN_FIND_QUIETLY)
  message(STATUS "PROXYGEN: ${PROXYGEN_INCLUDE_DIR}")
endif()
