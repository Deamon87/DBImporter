//
// Created by deamon on 16.12.19.
//

//#include <sqlite3.h>
#include <iostream>
#include <fstream>
#include <SQLiteCpp/Transaction.h>

#include "SQLiteCpp/Database.h"
#include "SQLiteCpp/../../sqlite3/sqlite3.h"
#include "CSQLLiteExporter.h"
#include "../../utils/string_utils.h"



CSQLLiteExporter::CSQLLiteExporter(const std::string &databaseFile) : m_databaseFile(databaseFile), m_sqliteDatabase(":memory:", SQLite::OPEN_READWRITE | SQLite::OPEN_CREATE) {
    //Tell database that often writes to disk is not necessary
    char *sErrMsg = "";
    sqlite3_exec(m_sqliteDatabase.getHandle(), "PRAGMA synchronous = OFF", NULL, NULL, &sErrMsg);
    sqlite3_exec(m_sqliteDatabase.getHandle(), "PRAGMA schema.journal_mode = MEMORY", NULL, NULL, &sErrMsg);
}

CSQLLiteExporter::~CSQLLiteExporter() {
    m_sqliteDatabase.backup(m_databaseFile.c_str(), SQLite::Database::BackupType::Save);
}

std::string CSQLLiteExporter::generateCreateSQL(const std::string &tableName,
                                         const std::vector<fieldInterchangeData> &fieldDefs) {
    std::string tableCreateQuery = "CREATE TABLE IF NOT EXISTS "+tableName+" (";

    for (int i = 0; i < fieldDefs.size(); i++) {
        auto &columnDef = fieldDefs[i];
        auto colFieldName = columnDef.fieldName;

        //Reserved words in sqlite
        if (toLowerCase(colFieldName) == "default") {
            colFieldName = "_Default";
        }

        if (toLowerCase(colFieldName) == "order") {
            colFieldName = "_Order";
        }
        if (toLowerCase(colFieldName) == "index") {
            colFieldName = "_Index";
        }

        tableCreateQuery += colFieldName;
        switch (columnDef.fieldType) {
            case FieldType::INT:
                tableCreateQuery += " INTEGER";
                break;
            case FieldType::FLOAT:
                tableCreateQuery += " REAL";
                break;
            case FieldType::STRING:
                tableCreateQuery += " TEXT";
                break;
        }
        if (columnDef.isId) {
            tableCreateQuery += " PRIMARY KEY";
        }
        tableCreateQuery += ", ";
    }

    tableCreateQuery.resize(tableCreateQuery.size()-2);
    tableCreateQuery +="); ";

    return tableCreateQuery;
}


void CSQLLiteExporter::addTableData( std::string tableName,
                                     std::vector<fieldInterchangeData> &fieldDefs,
                                     const std::function<void(const std::function <void(std::vector<std::string>&)>& )> &fieldValueIterator,
                                     const std::function<void(const std::function <void(int, int)>& )> &copyIterator) {
    //Create table if not exists
    {
        std::string createTable = this->generateCreateSQL(tableName,fieldDefs);

        SQLite::Transaction transaction(m_sqliteDatabase);

        m_sqliteDatabase.exec("DROP TABLE IF EXISTS " + tableName + ";");
        m_sqliteDatabase.exec(createTable);

        transaction.commit();
    }

    //Add index over relation/foreign keys
    {
        SQLite::Transaction transaction(m_sqliteDatabase);

        for (auto &fieldDef : fieldDefs) {
            if (fieldDef.isForeignKey) {
                m_sqliteDatabase.exec("CREATE INDEX IF NOT EXISTS "
                                      +fieldDef.fieldName+"_idx ON " + tableName + "("+fieldDef.fieldName+");");
            }
        }

        transaction.commit();
    }

    performInsert(tableName, fieldDefs, fieldValueIterator);
    performCopy(tableName, fieldDefs, copyIterator);

}

void CSQLLiteExporter::performCopy(const std::string &tableName,
                                   const std::vector<fieldInterchangeData> &fieldDefs,
                                   const std::function<void(const std::function <void(int, int)>& )> &copyIterator) {
    int sqlIdIndex = -1;
    for (int i = 0; i < fieldDefs.size(); i++) {
        if (fieldDefs[i].isId) {
            sqlIdIndex = i;
            break;
        }
    }

    //Copy records
    {
        //Prepear statement for copy
        std::string copyStatement = "INSERT INTO " + tableName + " (  ";
        for (int sqlFieldInd = 0; sqlFieldInd < fieldDefs.size() - 1; sqlFieldInd++) {
            copyStatement += fieldDefs[sqlFieldInd].fieldName + ", ";
        }
        copyStatement += fieldDefs[fieldDefs.size() - 1].fieldName + ") select ";
        for (int sqlFieldInd = 0; sqlFieldInd < fieldDefs.size(); sqlFieldInd++) {
            if (sqlFieldInd == sqlIdIndex) {
                copyStatement += +"?, ";
            } else {
                copyStatement += fieldDefs[sqlFieldInd].fieldName + ", ";
            }
        }

        copyStatement = copyStatement.substr(0, copyStatement.size() - 2);
        copyStatement += " FROM " + tableName;
        copyStatement += " where " + fieldDefs[sqlIdIndex].fieldName + " = ?";

        SQLite::Statement copyQuery(m_sqliteDatabase, copyStatement);

        {
            SQLite::Transaction transaction(m_sqliteDatabase);

            copyIterator([&copyQuery](int fromId, int toId) -> void {
                copyQuery.reset();

                copyQuery.bind(1, std::to_string(toId));
                copyQuery.bind(2, std::to_string(fromId));

                copyQuery.exec();
            });

            transaction.commit();
        }
    }
}

void CSQLLiteExporter::performInsert(const std::string &tableName,
                                     const std::vector<fieldInterchangeData> &fieldDefs,
                                     const std::function<void(const std::function <void(std::vector<std::string>&)>& )> &fieldValueIterator) {
    //Perform insert
    {
        //Prepare statement for insert
        std::string statement = "INSERT INTO " + tableName + " (  ";
        for (int sqlFieldInd = 0; sqlFieldInd < fieldDefs.size() - 1; sqlFieldInd++) {
            statement += fieldDefs[sqlFieldInd].fieldName + ", ";
        }
        statement += fieldDefs[fieldDefs.size() - 1].fieldName + ") VALUES ( ";
        for (int sqlFieldInd = 0; sqlFieldInd < fieldDefs.size() - 1; sqlFieldInd++) {
            statement += "?, ";
        }
        statement += "? );";
        SQLite::Statement query(m_sqliteDatabase, statement);


{
    SQLite::Transaction transaction(m_sqliteDatabase);

    fieldValueIterator([&query](std::vector<std::string> fieldValues) -> void {
        query.reset();

        for (int sqlFieldInd = 0; sqlFieldInd < fieldValues.size(); sqlFieldInd++) {
            query.bind(sqlFieldInd + 1, fieldValues[sqlFieldInd]);
        }

        query.exec();
    });

    // Commit transaction
    transaction.commit();
}
    }
}
