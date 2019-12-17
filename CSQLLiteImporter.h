//
// Created by deamon on 16.12.19.
//

#ifndef DBIMPORTER_CSQLLITEIMPORTER_H
#define DBIMPORTER_CSQLLITEIMPORTER_H

#include <string>
#include <SQLiteCpp/Database.h>
#include <memory>
#include <vector>
#include "DBDFile.h"
#include "WDC3/DB2Base.h"

class CSQLLiteImporter {
public:
    CSQLLiteImporter(const std::string &databaseFile);

    void addTable(std::string &tableName, std::string version, std::string db2File, std::string dbdFile);

private:
    SQLite::Database m_sqliteDatabase;


    void processWDC2(HFileContent fileContent){};
    void processWDC3(std::string tableName, HFileContent fileContent, std::shared_ptr<DBDFile> m_dbdFile, DBDFile::BuildConfig &buildConfig);


    std::string generateTableCreateSQL(
        std::string tableName,
        std::shared_ptr<DBDFile> m_dbdFile,
        DBDFile::BuildConfig &buildConfig,

        std::vector<std::string> &fieldNames, std::vector<std::string> &fieldDefaultValues
        );

    bool readWDC3Record(int i, std::vector<std::string> &fieldValues, WDC3::DB2Base &db2Base,
                        std::shared_ptr<DBDFile> &m_dbdFile, DBDFile::BuildConfig &buildConfig, int InlineIdIndex,
                        const std::vector<int> &columnDefFieldIndexToFieldIndex);
};


#endif //DBIMPORTER_CSQLLITEIMPORTER_H
