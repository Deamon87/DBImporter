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
#include "../IExporter.h"


class CSQLLiteExporter : public IExporter {
public:
    CSQLLiteExporter(const std::string &databaseFile);
    ~CSQLLiteExporter() override;

private:
    SQLite::Database m_sqliteDatabase;
    std::string m_databaseFile;

    void addTableData(
        std::string tableName,
        std::vector<fieldInterchangeData> &fieldDefs,
        const std::function<void(const std::function <void(std::vector<std::string>&)>& )> &fieldValueIterator,
        const std::function<void(const std::function <void(int, int)>& )> &copyIterator
    ) override;

private:
    std::string generateCreateSQL(const std::string &tableName,
                                       const std::vector<fieldInterchangeData> &fieldDefs);

    void performInsert(const std::string &tableName,
                       const std::vector<fieldInterchangeData> &fieldDefs,
                       const std::function<void(const std::function <void(std::vector<std::string>&)>& )> &fieldValueIterator);

    void performCopy(const std::string &tableName,
                     const std::vector<fieldInterchangeData> &fieldDefs,
                     const std::function<void(const std::function <void(int, int)>& )> &copyIterator);
};


#endif //DBIMPORTER_CSQLLITEEXPORTER_H
