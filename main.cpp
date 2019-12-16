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


int main() {
    std::string definitionsPath = "../3rdparty/WoWDBDefs/definitions/";

    CSQLLiteImporter csqlLiteImporter("export.db3");

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


            std::string pathToDB2 = "../db2Files/"+ db2Name+".db2";

            csqlLiteImporter.addTable(tableName, pathToDB2, definitionsPath+dbdFileName+".dbd");
//            break;
        }
    }

    return 0;
}