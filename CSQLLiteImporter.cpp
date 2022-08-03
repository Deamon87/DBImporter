//
// Created by deamon on 16.12.19.
//

//#include <sqlite3.h>
#include <iostream>
#include <fstream>
#include <SQLiteCpp/Transaction.h>
#include <SQLiteCpp/Database.h>
#include "CSQLLiteImporter.h"
#include "WDC3/DB2Base.h"
#include "WDC2/DB2Base.h"
#include "3rdparty/SQLiteCpp/sqlite3/sqlite3.h"
#include "DBDFileStorage.h"
#include <algorithm>
#include <type_traits>


CSQLLiteImporter::CSQLLiteImporter(const std::string &databaseFile) : m_databaseFile(databaseFile), m_sqliteDatabase(":memory:", SQLite::OPEN_READWRITE|SQLite::OPEN_CREATE) {
    //Tell database that often writes to disk is not necessary
    char *sErrMsg = "";
    sqlite3_exec(m_sqliteDatabase.getHandle(), "PRAGMA synchronous = OFF", NULL, NULL, &sErrMsg);
    sqlite3_exec(m_sqliteDatabase.getHandle(), "PRAGMA schema.journal_mode = MEMORY", NULL, NULL, &sErrMsg);
}



void CSQLLiteImporter::addTable(std::string &tableName, std::string db2File, std::shared_ptr<DBDFileStorage> fileDBDStorage) {
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
        std::shared_ptr<WDC3::DB2Base> db2Base = std::make_shared<WDC3::DB2Base>();
        db2Base->process(vec, "");
        DBDFile::BuildConfig *buildConfig = nullptr;

        auto dbdFile = fileDBDStorage->getDBDFile(tableName);
        if ( dbdFile!= nullptr ) {
            std::string tableNameFromDBD = fileDBDStorage->getTableName(db2Base->getWDCHeader()->table_hash);
            if (tableNameFromDBD != "") {
                tableName = tableNameFromDBD;
            }

            bool configFound = dbdFile->findBuildConfigByLayout(db2Base->getLayoutHash(), buildConfig);
            if (!configFound) {
                std::cout << "Could not proper build find config for table " << tableName <<
                    "for layout hash " << db2Base->getLayoutHash() << std::endl;

                buildConfig = nullptr;
            }
        }


        if (db2Base->getWDCHeader()->field_storage_info_size == 0) {
            if (buildConfig == nullptr) {
                std::cout << "DB2 " << tableName
                    << " do not have field info. Unable to parse without build config in DBD file"
                    << std::endl;
                return;
            }
        }

        //TODO: HACK
        /*
        if (db2Base->getWDCHeader()->flags.isSparse) return;

        //Debug info dump
        if (db2Base->getWDCHeader()->field_storage_info_size != 0) {
            for (int i = 0; i < db2Base->getWDCHeader()->field_count; i++) {
                decltype(buildConfig->columns)::value_type *dbdBuildColumnDef = nullptr;

                if (buildConfig != nullptr) {
                    for (auto &columnDef: buildConfig->columns) {
                        if (columnDef.columnIndex == i) {
                            dbdBuildColumnDef = &columnDef;
                            break;
                        }
                    }
                }

                auto fieldInfo = db2Base->getFieldInfo(i);
                if (fieldInfo->storage_type == WDC3::field_compression_bitpacked_indexed ||
                    fieldInfo->storage_type == WDC3::field_compression_bitpacked_indexed_array) {

                    std::cout << "pallete field "
                              << (dbdBuildColumnDef != nullptr ? dbdBuildColumnDef->fieldName : "field_" +
                                                                                                std::to_string(i))
                              << " size bits = "
                              << fieldInfo->field_size_bits
                              << " dbd size bits = "
                              << (dbdBuildColumnDef != nullptr ? dbdBuildColumnDef->bitSize : -1)
                              << " additional_data_size bits = "
                              << fieldInfo->additional_data_size
                              << " bitpacked offset bits = "
                              << fieldInfo->field_compression_bitpacked_indexed.bitpacking_offset_bits
                              << " offset bits = "
                              << fieldInfo->field_offset_bits
                              << " array_count = "
                              << fieldInfo->field_compression_bitpacked_indexed_array.array_count
                              << std::endl;
                }
            }
        }
        */

        processWDC3(tableName, db2Base, dbdFile, buildConfig);
    }
}

void CSQLLiteImporter::processWDC3(std::string tableName,
                                   std::shared_ptr<WDC3::DB2Base> db2Base,
                                   std::shared_ptr<DBDFile> dbdFile,
                                   DBDFile::BuildConfig *buildConfig) {


    bool isIdNonInline = false;
    int idSqlIndex = -1;

    int columnSize ;


    if (buildConfig != nullptr) {
        columnSize = buildConfig->columns.size();
    } else {
        columnSize = db2Base->getWDCHeader()->field_count;
    }

    std::vector<int> db2FieldIndexToSQLIndex = std::vector<int>(columnSize, -1);
    std::vector<int> dbdFieldIndexToSQLIndex = std::vector<int>(columnSize, -1);

    int sqlFieldsCount;
    {
        int sqlIndex = 0;
        int columnDB2Index = 0;

        if (buildConfig != nullptr) {
            for (size_t i = 0; i < columnSize; i++) {
                auto &columnDef = buildConfig->columns[i];
                dbdFieldIndexToSQLIndex[i] = sqlIndex;

                if (columnDef.isId) {
                    idSqlIndex = sqlIndex;
                    if (columnDef.isNonInline) {
                        isIdNonInline = true;
                        idSqlIndex = sqlIndex++;
                        continue;
                    }
                }
                db2FieldIndexToSQLIndex[columnDB2Index++] = sqlIndex;

                if (columnDef.arraySize > 1) {
                    sqlIndex += columnDef.arraySize;
                } else {
                    sqlIndex++;
                }
            }
        } else {
            if (db2Base->getWDCHeader()->flags.hasNonInlineId) {
                isIdNonInline = true;
                idSqlIndex = sqlIndex++;
            } else {
                idSqlIndex = db2Base->getWDCHeader()->id_index;
            }

            for (size_t i = 0; i < columnSize; i++) {
                dbdFieldIndexToSQLIndex[i] = sqlIndex;

                db2FieldIndexToSQLIndex[columnDB2Index++] = sqlIndex;

                auto columnDef = db2Base->getFieldInfo(i);
                if (columnDef->storage_type == WDC3::field_compression_bitpacked_indexed_array &&
                    columnDef->field_compression_bitpacked_indexed_array.array_count > 1) {
                    sqlIndex += columnDef->field_compression_bitpacked_indexed_array.array_count;
                } else {
                    sqlIndex++;
                }
            }

            //Last field if Relation if it exists;
            if (db2Base->hasRelationshipField()) {
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
        std::string createTable = this->generateTableCreateSQL(tableName,
            dbdFile, db2Base,

            buildConfig, fieldNames, fieldDefaultValues);

        SQLite::Transaction transaction(m_sqliteDatabase);

        m_sqliteDatabase.exec("DROP TABLE IF EXISTS " + tableName + ";");
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
        if (sqlFieldInd == idSqlIndex) {
            copyStatement += + "?, ";
        } else {
            copyStatement += fieldNames[sqlFieldInd]+", ";
        }
    }
    copyStatement = copyStatement.substr(0, copyStatement.size()-2);
    copyStatement += " FROM "+tableName;
    copyStatement += " where " + fieldNames[idSqlIndex] +" = ?";

    SQLite::Statement   copyQuery(m_sqliteDatabase, copyStatement);

    for (int i = 0; i < db2Base->getRecordCount(); i++) {
        std::string newRec = "";


        fieldValues = fieldDefaultValues;
        bool recordRead = readWDC3Record(i, idSqlIndex, fieldValues, db2Base, dbdFile,
                                         buildConfig, db2FieldIndexToSQLIndex, dbdFieldIndexToSQLIndex);

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
        db2Base->iterateOverCopyRecords([&copyQuery](int oldId, int newId) -> void {
            copyQuery.reset();

            copyQuery.bind(1, std::to_string(newId));
            copyQuery.bind(2, std::to_string(oldId));

            copyQuery.exec();
        });
        transaction.commit();
    }
}

bool CSQLLiteImporter::readWDC3Record(int recordIndex,
                                      int recordIdSqlIndex,
                                      std::vector<std::string> &fieldValues,
                                      std::shared_ptr<WDC3::DB2Base> db2Base,
                                      std::shared_ptr<DBDFile> &m_dbdFile,
                                      DBDFile::BuildConfig *buildConfig,
                                      const std::vector<int> &db2FieldIndexToSQLIndex,
                                      const std::vector<int> &dbdFieldIndexToSQLIndex) {
    if (!db2Base->isSparse()) {
        auto normalRecord = db2Base->getRecord(recordIndex);
        if (normalRecord == nullptr)
            return false;


        if (db2Base->getWDCHeader()->flags.hasNonInlineId) {
            fieldValues[recordIdSqlIndex] = std::to_string(normalRecord->getRecordId());
        }

        for (int i = 0; i < db2Base->getWDCHeader()->field_count; i++) {
            decltype(buildConfig->columns)::value_type *dbdBuildColumnDef = nullptr;

            decltype(std::declval<DBDFile>().getColumnDef(std::declval<std::string &>()))
                    dbdGlobalColumnDef = nullptr;

            if (buildConfig != nullptr) {
                for (auto &columnDef : buildConfig->columns) {
                    if (columnDef.columnIndex == i) {
                        dbdBuildColumnDef = &columnDef;
                        break;
                    }
                }
                dbdGlobalColumnDef = m_dbdFile->getColumnDef(dbdBuildColumnDef->fieldName);
            }

            int arraySize = -1;
            int elementSize = -1;
            if (dbdBuildColumnDef != nullptr && dbdGlobalColumnDef != nullptr) {
                if (dbdGlobalColumnDef->type == FieldType::STRING) {
                    arraySize = 1;
                    elementSize = 1;
                } else {
                    arraySize = dbdBuildColumnDef->arraySize > 0 ? dbdBuildColumnDef->arraySize : 1;
                    elementSize = dbdBuildColumnDef->bitSize >> 3;
                }

                if (dbdGlobalColumnDef->type == FieldType::FLOAT)
                    elementSize = 4;
            }

            auto fieldStruct = db2Base->getFieldInfo(i);
            auto valueVector = normalRecord->getField(i, arraySize, elementSize);



            for (int j = 0; j < valueVector.size(); j++) {
                if (dbdGlobalColumnDef == nullptr) {
                    if (fieldStruct->field_size_bits == 64) {
                        fieldValues[db2FieldIndexToSQLIndex[i] + j] = std::to_string(valueVector[j].v64);
                    } else {
                        if (fieldStruct->storage_type== WDC3::field_compression_bitpacked_signed) {
                            fieldValues[db2FieldIndexToSQLIndex[i] + j] = std::to_string(valueVector[j].v32s);
                        } else {
                            fieldValues[db2FieldIndexToSQLIndex[i] + j] = std::to_string(valueVector[j].v32);
                        }
                    }
                } else {
                    switch (dbdGlobalColumnDef->type) {
                        case FieldType::FLOAT:
                            fieldValues[db2FieldIndexToSQLIndex[i] + j] = std::to_string(valueVector[j].v_f);
                            break;
                        case FieldType::STRING:
                            fieldValues[db2FieldIndexToSQLIndex[i] + j] = normalRecord->readString(i);
                            break;
                        case FieldType::INT:
                            if (dbdBuildColumnDef->bitSize == 64) {
                                if (dbdBuildColumnDef->isSigned) {
                                    fieldValues[db2FieldIndexToSQLIndex[i] + j] = std::to_string(valueVector[j].v64);
                                } else {
                                    fieldValues[db2FieldIndexToSQLIndex[i] + j] = std::to_string(valueVector[j].v64s);
                                }
                            } else {
                                if (dbdBuildColumnDef->isSigned) {
                                    fieldValues[db2FieldIndexToSQLIndex[i] + j] = std::to_string(valueVector[j].v32);
                                } else {
                                    fieldValues[db2FieldIndexToSQLIndex[i] + j] = std::to_string(valueVector[j].v32s);
                                }
                            }
                            break;
                    }
                }
            }
        }
    } else {
        auto sparseRecord = db2Base->getSparseRecord(recordIndex);
        if (sparseRecord == nullptr)
            return false;

        if (db2Base->getWDCHeader()->flags.hasNonInlineId) {
            fieldValues[recordIdSqlIndex] = std::to_string(sparseRecord->getRecordId());
        }

        for (int i = 0; i < db2Base->getWDCHeader()->field_count; i++) {
            decltype(buildConfig->columns)::value_type *dbdBuildColumnDef = nullptr;

            decltype(std::declval<DBDFile>().getColumnDef(std::declval<std::string &>()))
                    dbdGlobalColumnDef = nullptr;

            if (buildConfig != nullptr) {
                for (auto &columnDef : buildConfig->columns) {
                    if (columnDef.columnIndex == i) {
                        dbdBuildColumnDef = &columnDef;
                        break;
                    }
                }
                dbdGlobalColumnDef = m_dbdFile->getColumnDef(dbdBuildColumnDef->fieldName);
            }

            if (dbdGlobalColumnDef->type == FieldType::STRING) {
                auto stringVal = sparseRecord->readNextAsString();
                fieldValues[db2FieldIndexToSQLIndex[i]] = stringVal;
            } else {
                int fieldSizeInBytes = dbdBuildColumnDef->bitSize >> 3;
                if (dbdGlobalColumnDef->type == FieldType::FLOAT) {
                    fieldSizeInBytes = 4;
                }

                int arraySize = 1;
                if (dbdBuildColumnDef->arraySize > 1) {
                    arraySize = dbdBuildColumnDef->arraySize;
                }

                for (int j = 0; j < arraySize; j++) {
                    auto valueVector = sparseRecord->readNextField(fieldSizeInBytes);

                    if (dbdGlobalColumnDef->type != FieldType::FLOAT) {
                        fieldValues[db2FieldIndexToSQLIndex[i] + j] = std::to_string(valueVector[0].v32);
                    } else {
                        fieldValues[db2FieldIndexToSQLIndex[i] + j] = std::to_string(valueVector[0].v_f);
                    }
                }
            }

        }
    }

    if (buildConfig != nullptr) {
        for (int j = 0; j < buildConfig->columns.size(); j++) {
            auto &columnDef = buildConfig->columns[j];
            if (columnDef.isRelation && columnDef.isNonInline) {
                fieldValues[dbdFieldIndexToSQLIndex[j]] = std::to_string(db2Base->getRelationRecord(recordIndex));
            }
        }
    } else {
        int j = fieldValues.size() - 1; //The relation field is the last field
        fieldValues[j] = std::to_string(db2Base->getRelationRecord(recordIndex));
    }

    return true;
}

std::string toLowerCase(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(),
                   [](unsigned char c){ return std::tolower(c); });
    return s;
}

std::string CSQLLiteImporter::generateTableCreateSQL(std::string tableName,
    std::shared_ptr<DBDFile> m_dbdFile,
    std::shared_ptr<WDC3::DB2Base> db2Base,
    DBDFile::BuildConfig *buildConfig,
    std::vector<std::string> &fieldNames,
    std::vector<std::string> &sqlFieldDefaultValues) {

    std::string tableCreateQuery = "CREATE TABLE IF NOT EXISTS "+tableName+" (";

    if (buildConfig != nullptr) {
        //Per columndef in DBD
        generateFieldsFromDBDColumns(m_dbdFile, buildConfig, fieldNames, sqlFieldDefaultValues,
                                                        tableCreateQuery);
    } else {
        generateFieldsFromDB2Columns(db2Base, fieldNames, sqlFieldDefaultValues, tableCreateQuery);
    }

    tableCreateQuery.resize(tableCreateQuery.size()-2);
    tableCreateQuery +="); ";

    return tableCreateQuery;
}

void CSQLLiteImporter::generateFieldsFromDBDColumns(std::shared_ptr<DBDFile> &m_dbdFile,
                                                            const DBDFile::BuildConfig *buildConfig,
                                                            std::vector<std::string> &fieldNames,
                                                            std::vector<std::string> &sqlFieldDefaultValues,
                                                            std::string &tableCreateQuery) {
    int sqlIndex = 0;
    for (int i = 0; i < buildConfig->columns.size(); i++) {
        auto &columnDef = buildConfig->columns[i];
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

        if (columnDef.isNonInline) {
            tableCreateQuery += colFieldName + " INTEGER ";
            if (columnDef.isId) {
                tableCreateQuery += " PRIMARY KEY";
            }
            tableCreateQuery += ", ";
            fieldNames.push_back(colFieldName);
            sqlFieldDefaultValues.emplace_back("0");
            sqlIndex++;
            continue;
        }
        auto columnDefToType = [&m_dbdFile, &sqlFieldDefaultValues, &tableCreateQuery](std::string colFieldName ) -> void {
            auto *columnTypeDef = m_dbdFile->getColumnDef(colFieldName);
            switch (columnTypeDef->type) {
                case FieldType::INT:
                    sqlFieldDefaultValues.emplace_back("0");
                    tableCreateQuery += "INTEGER ";
                    break;
                case FieldType::FLOAT:
                    sqlFieldDefaultValues.emplace_back("0");
                    tableCreateQuery += "REAL ";
                    break;
                case FieldType::STRING:
                    sqlFieldDefaultValues.emplace_back("");
                    tableCreateQuery += "TEXT ";
                    break;
            }
        };

        if (columnDef.arraySize > 1) {
            for (int j = 0; j < columnDef.arraySize; j++) {
                std::string columnName = colFieldName + "_" + std::to_string(j);
                tableCreateQuery += columnName + " ";

                columnDefToType(columnDef.fieldName);

                tableCreateQuery += ", ";

                fieldNames.push_back(columnName);
                sqlIndex++;
            }
        } else {
            tableCreateQuery += colFieldName + " ";

            columnDefToType(columnDef.fieldName);

            if (columnDef.isId) {
                tableCreateQuery += " PRIMARY KEY";
            }
            tableCreateQuery += ", ";

            fieldNames.push_back(colFieldName);
            sqlIndex++;
        }
    }
}

void CSQLLiteImporter::generateFieldsFromDB2Columns(
                std::shared_ptr<WDC3::DB2Base>db2Base,
                std::vector<std::string> &fieldNames,
                std::vector<std::string> &sqlFieldDefaultValues,
                std::string &tableCreateQuery) {
    int sqlIndex = 0;

    if (db2Base->getWDCHeader()->flags.hasNonInlineId) {
        std::string colFieldName = "inlineId";

        tableCreateQuery += colFieldName + " INTEGER ";

        tableCreateQuery += " PRIMARY KEY";

        tableCreateQuery += ", ";
        fieldNames.push_back(colFieldName);
        sqlFieldDefaultValues.emplace_back("0");
        sqlIndex++;
    }
    for (int i = 0; i < db2Base->getWDCHeader()->field_count; i++) {
        auto db2Field = db2Base->getFieldInfo(i);

        if (db2Field->storage_type == WDC3::field_compression_bitpacked_indexed_array &&
            db2Field->field_compression_bitpacked_indexed_array.array_count > 1) {
            for (int j = 0; j < db2Field->field_compression_bitpacked_indexed_array.array_count; j++) {
                std::string columnName = "field_" + std::to_string(i) + "_" + std::to_string(j);
                tableCreateQuery += columnName + " ";
                tableCreateQuery += "INTEGER ";
                tableCreateQuery += ", ";

                fieldNames.push_back(columnName);
                sqlFieldDefaultValues.emplace_back("0");
            }
        } else {
            std::string columnName = "field_" + std::to_string(i);
            if (i == db2Base->getWDCHeader()->id_index && !db2Base->getWDCHeader()->flags.hasNonInlineId) {
                columnName = "id";
            }
            tableCreateQuery += columnName + " ";
            tableCreateQuery += "INTEGER ";
            tableCreateQuery += ", ";

            fieldNames.push_back(columnName);
            sqlFieldDefaultValues.emplace_back("0");
        }
    }

    if (db2Base->hasRelationshipField()) {
        std::string columnName = "foreignId" ;
        tableCreateQuery += columnName + " ";
        tableCreateQuery += "INTEGER";
        tableCreateQuery += ", ";

        fieldNames.push_back(columnName);
        sqlFieldDefaultValues.emplace_back("0");
    }
}

CSQLLiteImporter::~CSQLLiteImporter() {
    m_sqliteDatabase.backup(m_databaseFile.c_str(), SQLite::Database::BackupType::Save);
}
