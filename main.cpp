#include <iostream>
#include <fstream>
#include <vector>
#include <locale>
#include <experimental/filesystem>

#include "DBDFile.h"
#include "WDC2/DB2Base.h"
#include "WDC3/DB2Base.h"

#include <SQLiteCpp/SQLiteCpp.h>
#include <sqlite3.h>

#include "CSQLLiteImporter.h"

namespace fs = std::experimental::filesystem;

void printUsage() {
    std::cout << "Usage: DBImporter -x <pathToDBDFiles> <pathToDBFiles> <version> <sqliteFileName>" << std::endl;
    std::cout << "Options: " << std::endl;

    std::cout << "  -x                   Mandatory flag for future uses" << std::endl;
    std::cout << "  <pathToDBDFiles>     Path to folder with database definition files *.dbd" << std::endl;
    std::cout << "  <pathToDBFiles>      Path to folder with db2 files. Right now only files with WDC3 header are supported" << std::endl;
    std::cout << "  <version>            Build and version of db2 files. For example: 8.3.0.32414" << std::endl;
    std::cout << "  <sqliteFileName>     File name for sqlite database. File will be created if it doesnt exist" << std::endl;
}


int main(int argc, char **argv) {
    if (argc < 6 || std::string(argv[1]) != "-x") {
        printUsage();
        return 1;
    }

    std::string definitionsPath = std::string(argv[2]);
    std::string DB2Folder = std::string(argv[3]);
    std::string version = std::string(argv[4]);

    CSQLLiteImporter csqlLiteImporter = CSQLLiteImporter(std::string(argv[5]));

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

//            dbdFileName = "ItemDisplayInfo";

            std::string tableName = dbdFileName;

            std::string db2Name = dbdFileName;
            std::transform(db2Name.begin(), db2Name.end(), db2Name.begin(),
                           [](unsigned char c){ return std::tolower(c); });


            std::string pathToDB2 = DB2Folder + db2Name+".db2";

            csqlLiteImporter.addTable(tableName, version, pathToDB2, definitionsPath+dbdFileName+".dbd");
//            break;
        }
    }

    return 0;
}