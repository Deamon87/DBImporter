cmake_minimum_required(VERSION 3.13)
project(DBImporter)

set(CMAKE_CXX_STANDARD 17)

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
        pthread
        stdc++fs
        ssp
        )