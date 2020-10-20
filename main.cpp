#include <iostream>
#include <fstream>
#include <vector>
#include <locale>
#include <filesystem>

#include "DBDFile.h"
#include "WDC2/DB2Base.h"
#include "WDC3/DB2Base.h"

#include <SQLiteCpp/SQLiteCpp.h>
#include <csignal>
#include <exception>
//#include <sqlite3.h>


#include "CSQLLiteImporter.h"

namespace fs = std::filesystem;

void printUsage() {
    std::cout << "Usage: DBImporter -x <pathToDBDFiles> <pathToDBFiles> <sqliteFileName>" << std::endl;
    std::cout << "Options: " << std::endl;

    std::cout << "  -x                   Mandatory flag for future uses" << std::endl;
    std::cout << "  <pathToDBDFiles>     Path to folder with database definition files *.dbd" << std::endl;
    std::cout << "  <pathToDBFiles>      Path to folder with db2 files. Right now only files with WDC3 header are supported" << std::endl;
    std::cout << "  <sqliteFileName>     File name for sqlite database. File will be created if it doesnt exist" << std::endl;
}

extern "C" void my_function_to_handle_aborts(int signal_number)
{
    /*Your code goes here. You can output debugging info.
      If you return from this function, and it was called
      because abort() was called, your program will exit or crash anyway
      (with a dialog box on Windows).
     */

    std::cout << "HELLO" << std::endl;
    std::cout << "HELLO" << std::endl;
}


#ifdef _WIN32
#include <windows.h>
void beforeCrash() {
    std::cout << "HELLO" << std::endl;
    //__asm("int3");
}

static LONG WINAPI
windows_exception_handler(EXCEPTION_POINTERS
* ExceptionInfo)
{
switch(ExceptionInfo->ExceptionRecord->ExceptionCode)
{
case EXCEPTION_ACCESS_VIOLATION:
fputs("Error: EXCEPTION_ACCESS_VIOLATION\n", stderr);
break;
case EXCEPTION_ARRAY_BOUNDS_EXCEEDED:
fputs("Error: EXCEPTION_ARRAY_BOUNDS_EXCEEDED\n", stderr);
break;
case EXCEPTION_BREAKPOINT:
fputs("Error: EXCEPTION_BREAKPOINT\n", stderr);
break;
}
return 0;
}
#endif

int main(int argc, char **argv) {

#ifdef _WIN32
    SetUnhandledExceptionFilter(windows_exception_handler);
    const bool SET_TERMINATE = std::set_terminate(beforeCrash);
#ifndef _MSC_VER
    const bool SET_TERMINATE_UNEXP = std::set_unexpected(beforeCrash);
#endif
#endif
    signal(SIGABRT, &my_function_to_handle_aborts);


    if (argc < 5 || std::string(argv[1]) != "-x") {
        printUsage();
        return 1;
    }

    std::string definitionsPath = std::string(argv[2]);
    std::string DB2Folder = std::string(argv[3]);

    CSQLLiteImporter csqlLiteImporter = CSQLLiteImporter(std::string(argv[4]));

    for (const auto& entry : fs::directory_iterator(definitionsPath)) {
        const auto filenameStr = entry.path().filename().string();
        if (entry.status().type() == fs::file_type::regular) {

            std::string dbdFileName = "";
            std::string fileExtension = "";
            auto pointPos = filenameStr.find(".");
            if ( pointPos != std::string::npos) {
                dbdFileName = filenameStr.substr(0, pointPos);
                fileExtension = filenameStr.substr(pointPos+1, filenameStr.size()- pointPos);
                if (fileExtension != "dbd") continue;
            } else {
                continue;
            }

//            dbdFileName = "itembonustree";
//            version = "8.0.1.26231";

            std::string tableName = dbdFileName;

            std::string db2Name = dbdFileName;
            std::transform(db2Name.begin(), db2Name.end(), db2Name.begin(),
                           [](unsigned char c){ return std::tolower(c); });


            std::string pathToDB2 = DB2Folder + db2Name+".db2";

            csqlLiteImporter.addTable(tableName, pathToDB2, definitionsPath+dbdFileName+".dbd");
//            break;
        }
    }

    return 0;
}