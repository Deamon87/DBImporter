cmake_minimum_required(VERSION 3.13)
project(DBImporter)
include(CheckCXXCompilerFlag)

enable_language( C CXX ASM )
set(CMAKE_CXX_STANDARD 17)
message("CMAKE_GENERATOR = ${CMAKE_GENERATOR}")

check_cxx_compiler_flag(-std=c++17 HAVE_FLAG_STD_CXX17)
if(HAVE_FLAG_STD_CXX17)
    # Have -std=c++17, use it
    message("WOWLIB HAVE_FLAG_STD_CXX17 is supported")
    #    set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -std=c++17" )
    #    set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -std=c++17" )
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -std=c++17" )
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -std=c++17" )
else()
    check_cxx_compiler_flag(-std=c++1z HAVE_FLAG_STD_CXX1Z)
    if(HAVE_FLAG_STD_CXX1Z)
        #        set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -std=c++1z" )
        #        set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -std=c++1z" )
        set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -std=c++1z")
        set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -std=c++1z")
    else()
        message(ERROR "No supported flags")
    endif()
endif()

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

option(DBIMPORTER_LINK_SQLITE "Complie and Link SQLITE" ON)
option(DBIMPORTER_SQLITE_INCLUDE_DIR "SqliteCPP Include dir" OFF)
if (NOT DBIMPORTER_SQLITE_INCLUDE_DIR)
    set(DBIMPORTER_SQLITE_INCLUDE_DIR "${PROJECT_SOURCE_DIR}/3rdparty/SQLiteCpp/include")
endif()
if (DBIMPORTER_LINK_SQLITE)
    add_subdirectory(${PROJECT_SOURCE_DIR}/3rdparty/SQLiteCpp EXCLUDE_FROM_ALL)
endif()



set(MAIN_SOURCES
        fileReaders/DBD/DBDFile.cpp
        fileReaders/DBD/DBDFile.h
        fileReaders/WDC2/DB2Base.cpp
        fileReaders/WDC3/DB2Ver3.cpp
        fileReaders/WDC4/DB2Ver4.cpp
        exporters/sqlite/CSQLLiteExporter.cpp
        exporters/sqlite/CSQLLiteExporter.h
        fileReaders/DBD/DBDFileStorage.cpp
        fileReaders/DBD/DBDFileStorage.h
        importers/WDC3/WDC3Importer.cpp
        importers/WDC3/WDC3Importer.h
        importers/FieldInterchangeData.h
        utils/string_utils.h
        utils/string_utils.cpp )
add_library(DBImporterLib ${MAIN_SOURCES})
target_include_directories(
        DBImporterLib
        PUBLIC ${DBIMPORTER_SQLITE_INCLUDE_DIR}
)

add_executable(DBImporter main.cpp
        importers/CImporterClass.cpp
        importers/CImporterClass.h
        persistance/persistanceFile.h
        exporters/IExporter.h)

target_link_libraries(DBImporter DBImporterLib)


if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    message("This is clang")
    if (NOT CMAKE_CXX_COMPILER_ID MATCHES "AppleClang")
        #Force clang to use libc++
        set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} ")
        set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -stdlib=libstdc++")
        set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE}  ")
        set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -stdlib=libstdc++")
    endif()
    #	link_libraries(libstdc++fs)


    #set(_GLIBCXX_USE_C99_CHECK 0)
    #set(_GLIBCXX_USE_C99_DYNAMIC 0)
    #set(_GLIBCXX_USE_C99_LONG_LONG_CHECK 0)
    #set(_GLIBCXX_USE_C99_LONG_LONG_DYNAMIC 0)
    #	set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -stdlib=libc++")
    #	set(CMAKE_C_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_DEBUG} -stdlib=libc++")
    #	set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -stdlib=libc++")
endif()
if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    link_libraries(stdc++fs)
    #	set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -pthread")
    #	set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -pthread ")
    #	set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++20")
endif()

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

