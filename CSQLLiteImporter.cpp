//
// Created by deamon on 16.12.19.
//

//#include <sqlite3.h>
#include <iostream>
#include <fstream>
#include <SQLiteCpp/Transaction.h>
#include "CSQLLiteImporter.h"
#include "WDC3/DB2Base.h"
#include "WDC2/DB2Base.h"
#include "3rdparty/SQLiteCpp/sqlite3/sqlite3.h"

CSQLLiteImporter::CSQLLiteImporter(const std::string &databaseFile) : m_sqliteDatabase(databaseFile, SQLite::OPEN_READWRITE|SQLite::OPEN_CREATE) {

    //Tell database that often writes to disk is not necessary
    char *sErrMsg = "";
    sqlite3_exec(m_sqliteDatabase.getHandle(), "PRAGMA synchronous = OFF", NULL, NULL, &sErrMsg);

}

void CSQLLiteImporter::addTable(std::string &tableName, std::string version, std::string db2File, std::string dbdFile) {
    std::shared_ptr<DBDFile> m_dbdFile = std::make_shared<DBDFile>(dbdFile);

    DBDFile::BuildConfig *buildConfig_;

    bool configFound = m_dbdFile->findBuildConfig(version, "", buildConfig_);
    if (!configFound) {
        std::cout << "Could not find config for file " + dbdFile << std::endl;
        return;
    }
    DBDFile::BuildConfig &buildConfig = *buildConfig_;

    //Read DB2 into memory
    std::ifstream cache_file(db2File, std::ios::in |std::ios::binary);
    HFileContent vec;
    if (cache_file.good()) {
        cache_file.unsetf(std::ios::skipws);

        // get its size:
        std::streampos fileSize;

        cache_file.seekg(0, std::ios::end);
        fileSize = cache_file.tellg();
        cache_file.seekg(0, std::ios::beg);


        vec = std::make_shared<FileContent>(fileSize);
        cache_file.read((char *) &(*vec.get())[0], fileSize);
        std::cout << "DB2 File opened " << db2File << std::endl;
    } else {
        std::cout << "DB2 File was not found " << db2File << std::endl;
        return;
    }

    if (*(uint32_t *)vec->data() == 'WDC2') {
        WDC2::DB2Base db2Base;
        db2Base.process(vec, db2File);


    } else if (*(uint32_t *)vec->data() == '3CDW') {
        processWDC3(tableName, vec, m_dbdFile, buildConfig);
    }
}

void CSQLLiteImporter::processWDC3(std::string tableName, HFileContent fileContent, std::shared_ptr<DBDFile> m_dbdFile, DBDFile::BuildConfig &buildConfig) {
    WDC3::DB2Base db2Base;
    db2Base.process(fileContent, "");

    bool isIdNonInline = false;
    int InlineIdIndex = -1;
    int IdIndex = -1;

    std::vector<int> columnDefFieldIndexToFieldIndex = std::vector<int>(buildConfig.columns.size(), -1);
    int sqlFieldsCount;
    {
        int sqlIndex = 0;
        int columnDB2Index = 0;
        for (auto &columnDef : buildConfig.columns) {
            if (columnDef.isId) {
                IdIndex = sqlIndex;
                if (columnDef.isNonInline) {
                    isIdNonInline = true;
                    InlineIdIndex = sqlIndex++;
                    continue;
                }
            }
            columnDefFieldIndexToFieldIndex[columnDB2Index++] = sqlIndex;

            if (columnDef.arraySize > 1) {
                sqlIndex += columnDef.arraySize;
            } else {
                sqlIndex++;
            }
        }
        sqlFieldsCount = sqlIndex;
    }

    //Indexed by sqlIndex
    std::vector<std::string> fieldNames = {};
    std::vector<std::string> fieldValues = {};
    std::vector<std::string> fieldDefaultValues = {};

    //To IndexInto fieldNames

    int totalColumnNum = 0;


    //Create table if not exists
    {
        std::string createTable = this->generateTableCreateSQL(tableName, m_dbdFile,
            buildConfig, fieldNames, fieldDefaultValues);

        SQLite::Transaction transaction(m_sqliteDatabase);

        m_sqliteDatabase.exec(createTable);

        transaction.commit();
    }


    //Prepear statement
    std::string statement = "INSERT INTO "+tableName+" (  ";
    for (int sqlFieldInd = 0; sqlFieldInd < sqlFieldsCount-1; sqlFieldInd++)  {
        statement += fieldNames[sqlFieldInd]+", ";
    }
    statement += fieldNames[sqlFieldsCount-1]+") VALUES ( ";
    for (int sqlFieldInd = 0; sqlFieldInd < sqlFieldsCount-1; sqlFieldInd++)  {
        statement += "?, ";
    }
    statement += "? );";

    SQLite::Statement   query(m_sqliteDatabase,statement);

    std::string copyStatement = "INSERT INTO "+tableName+" (  ";
    for (int sqlFieldInd = 0; sqlFieldInd < sqlFieldsCount-1; sqlFieldInd++)  {
        copyStatement += fieldNames[sqlFieldInd]+", ";
    }
    copyStatement += fieldNames[sqlFieldsCount-1]+") select ";
    for (int sqlFieldInd = 0; sqlFieldInd < sqlFieldsCount; sqlFieldInd++)  {
        if (sqlFieldInd == IdIndex) {
            copyStatement += + "?, ";
        } else {
            copyStatement += fieldNames[sqlFieldInd]+", ";
        }
    }
    copyStatement = copyStatement.substr(0, copyStatement.size()-2);
    copyStatement += " FROM "+tableName;
    copyStatement += " where " + fieldNames[IdIndex] +" = ?";

    SQLite::Statement   copyQuery(m_sqliteDatabase, copyStatement);

    for (int i = 0; i < db2Base.getRecordCount(); i++) {
        std::string newRec = "";
        //TODO: HackFix bc some fields are still not read properly
//                    fieldValues = std::vector<std::string>(sqlIndex, "");

        fieldValues = fieldDefaultValues;
        bool recordRead = readWDC3Record(i, fieldValues, db2Base, m_dbdFile, buildConfig, InlineIdIndex, columnDefFieldIndexToFieldIndex);

        if (recordRead) {
            SQLite::Transaction transaction(m_sqliteDatabase);

            query.reset();
            for (int sqlFieldInd = 0; sqlFieldInd < sqlFieldsCount; sqlFieldInd++)  {
                query.bind(sqlFieldInd+1, fieldValues[sqlFieldInd]);
            }

            query.exec();

            // Commit transaction
            transaction.commit();
        }
    }
    //Copy records
    {
        SQLite::Transaction transaction(m_sqliteDatabase);
        db2Base.iterateOverCopyRecords([&copyQuery](int oldId, int newId) -> void {
            copyQuery.reset();

            copyQuery.bind(1, std::to_string(newId));
            copyQuery.bind(2, std::to_string(oldId));

            copyQuery.exec();
        });
        transaction.commit();
    }
}

bool CSQLLiteImporter::readWDC3Record(int i, std::vector<std::string> &fieldValues, WDC3::DB2Base &db2Base,
                                      std::shared_ptr<DBDFile> &m_dbdFile, DBDFile::BuildConfig &buildConfig,
                                      int InlineIdIndex, const std::vector<int> &columnDefFieldIndexToFieldIndex) {

    int recordIndex = i;
    int recordId = -1;
    bool recordRead = db2Base.readRecordByIndex(i, 0, -1,
        [&buildConfig, &recordId, &m_dbdFile, &db2Base, InlineIdIndex, &fieldValues, columnDefFieldIndexToFieldIndex, recordIndex]
        (uint32_t &id, int fieldNum, int subIndex, int sectionIndex, unsigned char *&data, size_t length) {
            auto *fieldDef = &buildConfig.columns[0];
            fieldDef = nullptr;
            for (auto &columnDef : buildConfig.columns) {
                if (columnDef.columnIndex == fieldNum) {
                    fieldDef = &columnDef;
                    break;
                }
            }

            if (fieldDef->isId) {
                id = *(uint32_t *) data;
                recordId = id;
            }

            if (fieldNum == 0 && InlineIdIndex > -1) {
                //Id is pushed from internals of DB2Base
                fieldValues[InlineIdIndex] = std::to_string(id);
                recordId = id;
            }

            int fieldValuesIndex = columnDefFieldIndexToFieldIndex[fieldNum];
            if (subIndex > 0) {
                fieldValuesIndex += subIndex;
            }

            int arrSize = (fieldDef->arraySize > 0 && (subIndex == -1))? fieldDef->arraySize : 1;
            //                bool int16Detected = (length / arrSize) == 2;
            for (int j = 0; j < arrSize; j++) {
                auto &columnDef = m_dbdFile->getColumnDef(fieldDef->fieldName);
                std::string fieldName = columnDef.fieldName;

                switch (columnDef.type) {
                    case FieldType::INT:
                        if (fieldDef->bitSize == 16) {
                            if (fieldDef->isSigned) {
                                fieldValues[fieldValuesIndex++] = std::to_string(*(int16_t *) data);
                            } else {
                                fieldValues[fieldValuesIndex++] = std::to_string(*(uint16_t *) data);
                            }
                        } else if (fieldDef->bitSize == 32 || fieldDef->bitSize == 0) {
                            if (fieldDef->isSigned) {
                                fieldValues[fieldValuesIndex++] = std::to_string(*(int32_t *) data);
                            } else {
                                fieldValues[fieldValuesIndex++] = std::to_string(*(uint32_t *) data);
                            }
                        } else if (fieldDef->bitSize == 8 ) {
                            if (fieldDef->isSigned) {
                                fieldValues[fieldValuesIndex++] = std::to_string(*(int8_t *) data);
                            } else {
                                fieldValues[fieldValuesIndex++] = std::to_string(*(uint8_t *) data);
                            }
                        }
//                                    if (db2Base.isEmbeddedType()) {
                        data += (fieldDef->bitSize / 8);
//                                    }
                        break;
                    case FieldType::FLOAT:
                        fieldValues[fieldValuesIndex++] = std::to_string(*(float *) data);
                        data += 4;
                        break;
                    case FieldType::STRING:
                        //                            int offset = *(uint32_t *) data;
                        //                            offset += stringOffset;
                        fieldValues[fieldValuesIndex++] = (db2Base.readString(data, sectionIndex));
                        break;
                }

            }
        });

    for (int j = 0; j < buildConfig.columns.size(); j++) {
        auto &columnDef = buildConfig.columns[j];
        if (columnDef.isRelation && columnDef.isNonInline) {
            fieldValues[j] = std::to_string(db2Base.getRelationRecord(recordIndex));
        }
    }

    return recordRead;
}

std::string CSQLLiteImporter::generateTableCreateSQL(std::string tableName,
    std::shared_ptr<DBDFile> m_dbdFile,
    DBDFile::BuildConfig &buildConfig,
    std::vector<std::string> &fieldNames,
    std::vector<std::string> &fieldDefaultValues) {

    int sqlIndex = 0;

    std::string tableCreateQuery = "CREATE TABLE IF NOT EXISTS "+tableName+" (";
    //Per columndef in DBD
    for (int i = 0; i < buildConfig.columns.size(); i++) {
        auto &columnDef = buildConfig.columns[i];

        if (columnDef.isNonInline) {
            tableCreateQuery += columnDef.fieldName + " INTEGER ";
            if (columnDef.isId) {
                tableCreateQuery +=" PRIMARY KEY";
            }
            tableCreateQuery +=", ";
            fieldNames.push_back(columnDef.fieldName);
            fieldDefaultValues.push_back("0");
            sqlIndex++;
            continue;
        }

        if (columnDef.arraySize > 1) {
            for (int j = 0; j < columnDef.arraySize; j++) {
                std::string columnName = columnDef.fieldName+"_"+std::to_string(j);
                tableCreateQuery += columnName + " ";
                auto &columnTypeDef = m_dbdFile->getColumnDef(columnDef.fieldName);
                switch (columnTypeDef.type) {
                    case FieldType::INT:
                        fieldDefaultValues.push_back("0");
                        tableCreateQuery +="INTEGER ";
                        break;
                    case FieldType::FLOAT:
                        fieldDefaultValues.push_back("0");
                        tableCreateQuery +="REAL ";
                        break;
                    case FieldType::STRING:
                        fieldDefaultValues.push_back("");
                        tableCreateQuery +="TEXT ";
                        break;
                }
                tableCreateQuery +=", ";


                fieldNames.push_back(columnName);
                sqlIndex++;
            }
        } else {
            tableCreateQuery += columnDef.fieldName + " ";
            auto &columnTypeDef = m_dbdFile->getColumnDef(columnDef.fieldName);
            switch (columnTypeDef.type) {
                case FieldType::INT:
                    fieldDefaultValues.push_back("0");
                    tableCreateQuery +="INTEGER ";
                    break;
                case FieldType::FLOAT:
                    fieldDefaultValues.push_back("0");
                    tableCreateQuery +="REAL ";
                    break;
                case FieldType::STRING:
                    fieldDefaultValues.push_back("");
                    tableCreateQuery +="TEXT ";
                    break;
            }

            if (columnDef.isId) {
                tableCreateQuery += " PRIMARY KEY";
            }
            tableCreateQuery +=", ";

            fieldNames.push_back(columnDef.fieldName);
            sqlIndex++;
        }
    }

    tableCreateQuery.resize(tableCreateQuery.size()-2);
    tableCreateQuery +="); ";

    return tableCreateQuery;
}
