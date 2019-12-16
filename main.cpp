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

namespace fs = std::experimental::filesystem;


int main() {

    SQLite::Database    db("export.db3", SQLite::OPEN_READWRITE|SQLite::OPEN_CREATE);
    char *sErrMsg = "";
    sqlite3_exec(db.getHandle(), "PRAGMA synchronous = OFF", NULL, NULL, &sErrMsg);

    std::string definitionsPath = "../3rdparty/WoWDBDefs/definitions/";

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

//            dbdFileName = "Wmoareatable";

            std::string tableName = dbdFileName;



            DBDFile dbdFile(definitionsPath+dbdFileName+".dbd");

            DBDFile::BuildConfig *buildConfig_;

            bool configFound = dbdFile.findBuildConfig("8.3.0.32414", "", buildConfig_);
            if (!configFound) {
                std::cout << "Could not find config for file " + dbdFileName << std::endl;
                continue;
            }
            DBDFile::BuildConfig &buildConfig = *buildConfig_;

            std::string db2Name = dbdFileName;
            std::transform(db2Name.begin(), db2Name.end(), db2Name.begin(),
                           [](unsigned char c){ return std::tolower(c); });


            std::string pathToDB2 = "../"+ db2Name+".db2";

            std::ifstream cache_file(pathToDB2, std::ios::in |std::ios::binary);
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
                std::cout << "DB2 File opened " << db2Name << std::endl;
            } else {
                std::cout << "DB2 File was not found " << db2Name << std::endl;
                continue;
            }

            if (*(uint32_t *)vec->data() == 'WDC2') {
                WDC2::DB2Base db2Base;
                db2Base.process(vec, pathToDB2);


            } else if (*(uint32_t *)vec->data() == '3CDW') {
                WDC3::DB2Base db2Base;
                db2Base.process(vec, pathToDB2);

                bool isIdNonInline = false;
                for (auto &columnDef : buildConfig.columns) {
                    if (columnDef.isId && columnDef.isNonInline) {
                        isIdNonInline = true;
                        break;
                    }
                }


                //Indexed by sqlIndex
                std::vector<std::string> fieldNames = {};
                std::vector<std::string> fieldValues = {};
                std::vector<std::string> fieldDefaultValues = {};

                //To IndexInto fieldNames
                std::vector<int> columnDefFieldIndexToFieldIndex = std::vector<int>(buildConfig.columns.size(), -1);


                int totalColumnNum = 0;

                //columnIndex as in db2 file
                int columnDBDIndex = 0;
                int sqlIndex = 0;
                int InlineIdIndex = -1;
                //Per columndef in DBD

                std::string tableCreateQuery = "CREATE TABLE IF NOT EXISTS "+tableName+" (";
                for (int i = 0; i < buildConfig.columns.size(); i++) {
                    auto &columnDef = buildConfig.columns[i];

                    if (columnDef.isNonInline) {
                        tableCreateQuery += columnDef.fieldName + " INTEGER ";
                        if (columnDef.isId) {
                            tableCreateQuery +=" PRIMARY KEY";
                            InlineIdIndex = sqlIndex;
                        }
                        tableCreateQuery +=", ";
                        fieldNames.push_back(columnDef.fieldName);
                        fieldDefaultValues.push_back("0");
                        sqlIndex++;
                        continue;
                    }

                    columnDefFieldIndexToFieldIndex[columnDBDIndex++] = sqlIndex;

                    if (columnDef.arraySize > 1) {
                        for (int j = 0; j < columnDef.arraySize; j++) {
                            std::string columnName = columnDef.fieldName+"_"+std::to_string(j);
                            tableCreateQuery += columnName + " ";
                            auto &columnTypeDef = dbdFile.getColumnDef(columnDef.fieldName);
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
                        auto &columnTypeDef = dbdFile.getColumnDef(columnDef.fieldName);
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
                //Create table if not exists
                {
                    SQLite::Transaction transaction(db);

                    db.exec(tableCreateQuery);

                    transaction.commit();
                }


                //Prepear statement
                std::string statement = "INSERT INTO "+tableName+" (  ";
                for (int sqlFieldInd = 0; sqlFieldInd < sqlIndex-1; sqlFieldInd++)  {
                    statement += fieldNames[sqlFieldInd]+", ";
                }
                statement += fieldNames[sqlIndex-1]+") VALUES ( ";
                for (int sqlFieldInd = 0; sqlFieldInd < sqlIndex-1; sqlFieldInd++)  {
                    statement += "?, ";
                }
                statement += "? );";
                SQLite::Statement   query(db,statement);

                for (int i = 0; i < db2Base.getRecordCount(); i++) {
                    std::string newRec = "";
                    //TODO: HackFix bc some fields are still not read properly
//                    fieldValues = std::vector<std::string>(sqlIndex, "");
                    fieldValues = fieldDefaultValues;

                    bool recordRead = db2Base.readRecordByIndex(i, 0, -1, [&buildConfig, &dbdFile, &newRec, &db2Base, InlineIdIndex, &fieldValues, columnDefFieldIndexToFieldIndex] (uint32_t &id, int fieldNum, int subIndex, int sectionIndex, unsigned char *&data, size_t length) {
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
                        }
                        if (fieldNum == 0 && InlineIdIndex > -1) {
                            //Id is pushed from internals of DB2Base
                            fieldValues[InlineIdIndex] = std::to_string(id);
                        }

                        int fieldValuesIndex = columnDefFieldIndexToFieldIndex[fieldNum];
                        if (subIndex > 0) {
                            fieldValuesIndex += subIndex;
                        }

                        int arrSize = (fieldDef->arraySize > 0 && (subIndex == -1))? fieldDef->arraySize : 1;
        //                bool int16Detected = (length / arrSize) == 2;
                        for (int j = 0; j < arrSize; j++) {
                            auto &columnDef = dbdFile.getColumnDef(fieldDef->fieldName);
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

                    if (recordRead) {
                        SQLite::Transaction transaction(db);

                        query.reset();
                        for (int sqlFieldInd = 0; sqlFieldInd < sqlIndex; sqlFieldInd++)  {
                            query.bind(sqlFieldInd+1, fieldValues[sqlFieldInd]);
                        }

                        query.exec();

                        // Commit transaction
                        transaction.commit();

//                        std::cout << newRec << std::endl;
                    }
                }
            }
        }
    }

    return 0;
}