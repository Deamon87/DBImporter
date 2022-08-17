//
// Created by deamon on 16.12.19.
//

#ifndef DBIMPORTER_CSQLLITEIMPORTER_H
#define DBIMPORTER_CSQLLITEIMPORTER_H

#include <string>
#include <SQLiteCpp/Database.h>
#include <memory>
#include <vector>
#include "DBD/DBDFile.h"
#include "WDC3/DB2Base.h"
#include "DBD/DBDFileStorage.h"

class CSQLLiteImporter {
public:
    CSQLLiteImporter(const std::string &databaseFile);
    ~CSQLLiteImporter();
    void addTable(std::string &tableName, std::string db2File, std::shared_ptr<DBDFileStorage> fileDBDStorage);

private:
    SQLite::Database m_sqliteDatabase;
    std::string m_databaseFile;

    void processWDC2(HFileContent fileContent){};
    void processWDC3(std::string tableName, std::shared_ptr<WDC3::DB2Base> db2Base,
                     std::shared_ptr<DBDFile> m_dbdFile, DBDFile::BuildConfig *buildConfig);


    std::string generateTableCreateSQL(
        std::string tableName,
        std::shared_ptr<DBDFile> m_dbdFile,
        std::shared_ptr<WDC3::DB2Base> db2Base,
        DBDFile::BuildConfig *buildConfig,

        std::vector<std::string> &fieldNames, std::vector<std::string> &fieldDefaultValues
        );

    bool readWDC3Record(int i, int recordIdSqlIndex,
                        std::vector<std::string> &fieldValues,
                        std::shared_ptr<WDC3::DB2Base> db2Base,
                        std::shared_ptr<DBDFile> &m_dbdFile,
                        DBDFile::BuildConfig *buildConfig,

                        const std::vector<int> &db2FieldIndexToSQLIndex,
                        const std::vector<int> &dbdFieldIndexToSQLIndex
                        );


    void generateFieldsFromDBDColumns(std::shared_ptr<DBDFile> &m_dbdFile,
                                      const DBDFile::BuildConfig *buildConfig,
                                      std::vector<std::string> &fieldNames,
                                      std::vector<std::string> &sqlFieldDefaultValues,
                                      std::string &tableCreateQuery);

    void generateFieldsFromDB2Columns(
            std::shared_ptr<WDC3::DB2Base>db2Base,
            std::vector<std::string> &fieldNames,
            std::vector<std::string> &sqlFieldDefaultValues,
            std::string &tableCreateQuery);
};


#endif //DBIMPORTER_CSQLLITEIMPORTER_H
