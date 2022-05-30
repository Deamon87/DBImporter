cmake_minimum_required(VERSION 3.13)
project(DBImporter)

set(CMAKE_CXX_STANDARD 17)
message("CMAKE_GENERATOR = ${CMAKE_GENERATOR}")

if (MSVC)
	include(CheckCXXCompilerFlag)
	CHECK_CXX_COMPILER_FLAG("/std:c++17" _cpp_17_flag_supported)
	message("MSVC Is on")
	if (_cpp_17_flag_supported)
		message("/std:c++17 is supported")
		#target_compile_options(AWebWoWViewerCpp "/std:c++17")
		set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} /std:c++17")
		set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} /std:c++17")
		set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} /std:c++17")
		#target_compile_options(AWebWoWViewerCpp /std:c++17)
	endif() 
endif()

set(SQLITECPP_RUN_CPPLINT OFF CACHE BOOL "")
if (WIN32 AND NOT MSVC)
    add_compile_options(-fstack-protector)
endif()

add_subdirectory(${PROJECT_SOURCE_DIR}/3rdparty/SQLiteCpp EXCLUDE_FROM_ALL)

include_directories(
        ${CMAKE_CURRENT_LIST_DIR}/3rdparty/SQLiteCpp/include
) 

add_executable(DBImporter main.cpp DBDFile.cpp DBDFile.h
        WDC2/DB2Base.cpp
        WDC3/DB2Base.cpp
        CSQLLiteImporter.cpp CSQLLiteImporter.h)

if (NOT MSVC)
    find_library(DL_EXISTS dl)
    message("DL_EXISTS = ${DL_EXISTS}")
    if (NOT DL_EXISTS STREQUAL "DL_EXISTS-NOTFOUND")
        message("Linking against DL")
        target_link_libraries(DBImporter dl)
    else()
       find_library(SSP_EXISTS ssp)
       message("SSP_EXISTS = ${SSP_EXISTS}")
       if (SSP_EXISTS STREQUAL "SSP_EXISTS-NOTFOUND")
           error( "Neither DL nor SSP was found" )
       endif()
       
       message("Linking against SSP")

       target_link_libraries(DBImporter ssp)

    endif()
endif(NOT MSVC)

target_link_libraries(DBImporter
        SQLiteCpp
        sqlite3
        )

if (NOT MSVC)
    target_link_libraries(DBImporter pthread)
    if (NOT CMAKE_CXX_COMPILER_ID MATCHES "AppleClang")
      target_link_libraries(DBImporter stdc++fs)
    endif()
endif()

#add_custom_command(TARGET DBImporter POST_BUILD
#        COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_RUNTIME_DLLS:DBImporter> $<TARGET_FILE_DIR:DBImporter>
#        COMMAND_EXPAND_LISTS
#        )

install(TARGETS DBImporter
        EXPORT ${PROJECT_NAME}Targets
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        COMPONENT libraries)

INSTALL(FILES ${CMAKE_INSTALL_SYSTEM_RUNTIME_LIBS} DESTINATION bin COMPONENT Libraries)

if( MINGW )
    message( STATUS "    Installing system-libraries: MinGW DLLs." )
    get_filename_component( Mingw_Path ${CMAKE_CXX_COMPILER} PATH )
    set( CMAKE_INSTALL_SYSTEM_RUNTIME_LIBS ${Mingw_Path}/libgcc_s_seh-1.dll ${Mingw_Path}/libstdc++-6.dll ${Mingw_Path}/libssp-0.dll ${Mingw_Path}/libwinpthread-1.dll)
endif( MINGW )
include( InstallRequiredSystemLibraries )


# Actually install it when make install is called.
# Note, this works with CPack
if( CMAKE_INSTALL_SYSTEM_RUNTIME_LIBS )
    install( PROGRAMS ${CMAKE_INSTALL_SYSTEM_RUNTIME_LIBS} DESTINATION bin COMPONENT System )
endif( CMAKE_INSTALL_SYSTEM_RUNTIME_LIBS )

include(CPack)

