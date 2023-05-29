### TBB ###

IF (TBB_INCLUDE_DIR)
    SET (TBB_FIND_QUIETLY TRUE)
ENDIF (TBB_INCLUDE_DIR)

message(${TBB_ROOT})

# find includes
FIND_PATH (TBB_INCLUDE_DIR
    NAMES tbb.h
    PATHS ${TBB_ROOT}/include/tbb
)

# find lib
SET(TBB_NAME tbb)

FIND_LIBRARY(TBB_LIBRARIES
    NAMES ${TBB_NAME}
    PATHS ${TBB_ROOT}/lib/x86_64-linux-gnu/
    NO_DEFAULT_PATH
)

include ("FindPackageHandleStandardArgs")
find_package_handle_standard_args ("TBB" DEFAULT_MSG TBB_INCLUDE_DIR TBB_LIBRARIES)

mark_as_advanced (TBB_INCLUDE_DIR TBB_LIBRARIES)
