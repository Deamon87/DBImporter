//
// Created by deamon on 16.12.19.
//

#ifndef DBIMPORTER_CSQLLITEEXPORTER_H
#define DBIMPORTER_CSQLLITEEXPORTER_H

#include <string>
#include <memory>
#include <vector>
#include <functional>

#include "SQLiteCpp/Database.h"
#include "../../importers/FieldInterchangeData.h"


class CSQLLiteExporter {
public:
    CSQLLiteExporter(const std::string &databaseFile);
    ~CSQLLiteExporter();

private:
    SQLite::Database m_sqliteDatabase;
    std::string m_databaseFile;

    void addTableData(
        std::string tableName,
        std::vector<fieldInterchangeData> &fieldDefs,
        std::function<bool(std::vector<std::string> &fieldValues)> fieldValueGenerator,
        std::function<bool(int &fromId, int &toId)> copyGenerator
    );

private:
    std::string generateCreateSQL(const std::string &tableName,
                                       const std::vector<fieldInterchangeData> &fieldDefs);

    void performInsert(const std::string &tableName,
                       const std::vector<fieldInterchangeData> &fieldDefs,
                       const std::function<bool(std::vector<std::string> &)> &fieldValueGenerator);

    void performCopy(const std::string &tableName,
                     const std::vector<fieldInterchangeData> &fieldDefs,
                     const std::function<bool(int &, int &)> &copyGenerator);
};


#endif //DBIMPORTER_CSQLLITEEXPORTER_H
