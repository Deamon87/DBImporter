cmake_minimum_required(VERSION 3.13)
project(DBImporter)

set(CMAKE_CXX_STANDARD 17)

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

set(SQLITECPP_RUN_CPPLINT 0)

add_subdirectory(${PROJECT_SOURCE_DIR}/3rdparty/SQLiteCpp)

include_directories(
        ${CMAKE_CURRENT_LIST_DIR}/3rdparty/SQLiteCpp/include
) 

add_executable(DBImporter main.cpp DBDFile.cpp DBDFile.h
        WDC2/DB2Base.cpp
        WDC3/DB2Base.cpp
        CSQLLiteImporter.cpp CSQLLiteImporter.h)

find_library(SSP_EXISTS ssp)
message("SSP_EXISTS = ${SSP_EXISTS}")
if (SSP_EXISTS STREQUAL "SSP_EXISTS-NOTFOUND" )
    find_library(DL_EXISTS dl)
    message("DL_EXISTS = ${DL_EXISTS}")
    if (NOT DL_EXISTS STREQUAL "DL_EXISTS-NOTFOUND")
        message("Linking against DL")
        target_link_libraries(DBImporter dl)
    endif()
else()
    message("Linking against SSP")
        message(${SSP_EXISTS})
    target_link_libraries(DBImporter ssp)
endif()


target_link_libraries(DBImporter
        SQLiteCpp
        sqlite3
        )

if (NOT MSVC)
    target_link_libraries(DBImporter pthread stdc++fs ssp)
endif()