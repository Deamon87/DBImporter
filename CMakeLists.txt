cmake_minimum_required(VERSION 3.13)
project(DBImporter)

set(CMAKE_CXX_STANDARD 17)

add_subdirectory(${CMAKE_CURRENT_LIST_DIR}/SQLiteCpp)

include_directories(
        ${CMAKE_CURRENT_LIST_DIR}/SQLiteCpp/include
)

add_executable(DBImporter main.cpp DBDFile.cpp DBDFile.h
        WDC2/DB2Base.cpp
        WDC3/DB2Base.cpp
        )

target_link_libraries(DBImporter
        SQLiteCpp
        sqlite3
        pthread
        stdc++fs
        ssp
        )