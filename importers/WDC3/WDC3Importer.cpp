//
// Created by Deamon on 8/24/2022.
//

#include "WDC3Importer.h"
#include "../../utils/string_utils.h"

/*
void dumpDebugInfo(std::shared_ptr<WDC3::DB2Ver3> &db2Base, DBDFile::BuildConfig *buildConfig) const {
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
}
 */

void WDC3Importer::processWDC3(const std::string &tableName,
                               const std::shared_ptr<WDC3::DB2Ver3> &db2Base,
                               const std::shared_ptr<DBDFile> &dbdFile,
                               IExporter * exporter,
                               const DBDFile::BuildConfig *buildConfig) {

    int columnSize ;
    if (buildConfig != nullptr) {
        columnSize = buildConfig->columns.size();
    } else {
        columnSize = db2Base->getWDCHeader()->field_count;
    }

    std::vector<int> db2FieldIndexToOutputFieldIndex = std::vector<int>(columnSize, -1);
    std::vector<int> dbdFieldIndexToOutputFieldIndex = std::vector<int>(columnSize, -1);

    std::vector<fieldInterchangeData> fieldDefs;

    //Generate interchangable data
    if (buildConfig != nullptr) {
        fieldDefs = generateFieldsFromDBDColumns(dbdFile, buildConfig,
                                     db2FieldIndexToOutputFieldIndex,
                                     dbdFieldIndexToOutputFieldIndex);
    } else {
        fieldDefs = generateFieldsFromDB2Columns(db2Base, db2FieldIndexToOutputFieldIndex);
    }

    //
    int exportIdIndex = -1;
    for (int i = 0; i < fieldDefs.size(); i++) {
        if (fieldDefs[i].isId) {
            exportIdIndex = i;
            break;
        }
    }

    std::vector<std::string> fieldValues = {};
    std::vector<std::string> fieldDefaultValues = std::vector<std::string>(fieldDefs.size(), "");
    for (int i = 0; i < fieldDefs.size(); i++) {
        if (fieldDefs[i].fieldType != FieldType::STRING) {
            fieldDefaultValues[i] = "0";
        }
    }

    auto readRecordsLambda = [&](const std::function<void(std::vector<std::string> &fieldValues)> &consumer) -> void {
        fieldValues = fieldDefaultValues;
        for (int i = 0; i < db2Base->getRecordCount(); i++) {
            bool recordRead = readWDC3Record(i, exportIdIndex,
                                             fieldValues,
                                             db2Base, dbdFile,
                                             buildConfig,
                                             db2FieldIndexToOutputFieldIndex,
                                             dbdFieldIndexToOutputFieldIndex);

            if (recordRead) {
                consumer(fieldValues);
                fieldValues = fieldDefaultValues;
            }
        }
    };

    auto copyLambda = [&](const std::function<void (int fromId, int toId)> &consumer) -> void {
        db2Base->iterateOverCopyRecords([&consumer](int fromId, int toId) -> void {
            consumer(fromId, toId);
        });
    };
    exporter->addTableData(tableName, fieldDefs, readRecordsLambda, copyLambda);
}

std::vector<fieldInterchangeData>
WDC3Importer::generateFieldsFromDB2Columns(std::shared_ptr<WDC3::DB2Ver3> db2Base,
                                           std::vector<int> &db2FieldIndexToOutputFieldIndex) {
    std::vector<fieldInterchangeData> result;

    int exportFieldIndex = 0;
    if (db2Base->getWDCHeader()->flags.hasNonInlineId) {
        result.push_back({
            "inlineId",
            true,
            FieldType::INT,
        });

        exportFieldIndex++;
    }
    for (int i = 0; i < db2Base->getWDCHeader()->field_count; i++) {
        auto db2Field = db2Base->getFieldInfo(i);

        db2FieldIndexToOutputFieldIndex[i] = exportFieldIndex;

        int arrayCount = 1;
        if (db2Field->storage_type == WDC3::field_compression::field_compression_bitpacked_indexed_array) {
            arrayCount = db2Field->field_compression_bitpacked_indexed_array.array_count;
        }

        if (db2Field->storage_type == WDC3::field_compression::field_compression_none) {
            int fieldSizeInByte = 0;
            WDC3::DB2Ver3::guessFieldSizeForCommon(db2Field->field_size_bits, fieldSizeInByte, arrayCount);
        }

        if (arrayCount > 1) {
            for (int j = 0; j < arrayCount; j++) {
                std::string columnName = "field_" + std::to_string(i) + "_" + std::to_string(j);

                result.push_back({
                    columnName,
                    false,
                    FieldType::INT,
                 });
                exportFieldIndex++;
            }
        } else {
            std::string columnName = "field_" + std::to_string(i);
            bool isId = false;
            if (i == db2Base->getWDCHeader()->id_index && !db2Base->getWDCHeader()->flags.hasNonInlineId) {
                columnName = "id";
                isId = true;
            }
            result.push_back({
               columnName,
               isId,
               FieldType::INT,
            });
            db2FieldIndexToOutputFieldIndex[i] = exportFieldIndex++;
        }
    }

    if (db2Base->hasRelationshipField()) {
        result.push_back({
             "foreignId",
             false,
             FieldType::INT,
        });
    }

    return result;
}

std::vector<fieldInterchangeData>
WDC3Importer::generateFieldsFromDBDColumns(const std::shared_ptr<DBDFile> &m_dbdFile,
                                           const DBDFile::BuildConfig *buildConfig,
                                           std::vector<int> &db2FieldIndexToOutputFieldIndex,
                                           std::vector<int> &dbdFieldIndexToOutputFieldIndex) {
    std::vector<fieldInterchangeData> result;

    int exportFieldIndex = 0;
    int db2FieldIndex = 0;

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
            result.push_back({
                colFieldName,
                columnDef.isId,
                FieldType::INT,
                columnDef.isRelation
            });

            dbdFieldIndexToOutputFieldIndex[i] = exportFieldIndex++;
            continue;
        }

        if (columnDef.arraySize > 1) {
            db2FieldIndexToOutputFieldIndex[db2FieldIndex++] = exportFieldIndex;
            for (int j = 0; j < columnDef.arraySize; j++) {
                auto *columnTypeDef = m_dbdFile->getColumnDef(columnDef.fieldName);

                std::string columnName = colFieldName + "_" + std::to_string(j);

                result.push_back({
                  columnName,
                    false,
                    columnTypeDef->type,
                    false,
                });

                dbdFieldIndexToOutputFieldIndex[i] = exportFieldIndex++;
            }
        } else {
            auto *columnTypeDef = m_dbdFile->getColumnDef(columnDef.fieldName);

            result.push_back({
                   colFieldName,
                     columnDef.isId,
                     columnTypeDef->type,
                     columnDef.isRelation,
            });
            db2FieldIndexToOutputFieldIndex[db2FieldIndex++] = exportFieldIndex;
            dbdFieldIndexToOutputFieldIndex[i] = exportFieldIndex++;
        }
    }

    return result;
}

bool WDC3Importer::readWDC3Record(const int recordIndex,
                                  const int recordIdExportIndex,
                                  std::vector<std::string> &fieldValues,
                                  const std::shared_ptr<WDC3::DB2Ver3> db2Base,
                                  const std::shared_ptr<DBDFile> &m_dbdFile,
                                  const DBDFile::BuildConfig *buildConfig,
                                  const std::vector<int> &db2FieldIndexToSQLIndex,
                                  const std::vector<int> &dbdFieldIndexToSQLIndex) {
    if (!db2Base->isSparse()) {
        auto normalRecord = db2Base->getRecord(recordIndex);
        if (normalRecord == nullptr)
            return false;


        if (db2Base->getWDCHeader()->flags.hasNonInlineId) {
            fieldValues[recordIdExportIndex] = std::to_string(normalRecord->getRecordId());
        }

        for (int i = 0; i < db2Base->getWDCHeader()->field_count; i++) {
            const decltype(buildConfig->columns)::value_type * dbdBuildColumnDef = nullptr;

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
                arraySize = dbdBuildColumnDef->arraySize > 0 ? dbdBuildColumnDef->arraySize : 1;

                if (dbdGlobalColumnDef->type == FieldType::STRING || dbdGlobalColumnDef->type == FieldType::FLOAT) {
                    elementSize = 4;
                } else {
                    elementSize = dbdBuildColumnDef->bitSize >> 3;
                }
            }

            auto fieldStruct = db2Base->getFieldInfo(i);
            auto valueVector = normalRecord->getField(i, arraySize, elementSize);

            for (int j = 0; j < valueVector.size(); j++) {
                if (dbdGlobalColumnDef == nullptr) {
                    if (fieldStruct->field_size_bits == 64) {
                        fieldValues[db2FieldIndexToSQLIndex[i] + j] = std::to_string(valueVector[j].v64);
                    } else {
                        if (fieldStruct->storage_type== WDC3::field_compression::field_compression_bitpacked_signed) {
                            fieldValues[db2FieldIndexToSQLIndex[i] + j] = std::to_string(valueVector[j].v32s);
                        } else {
                            fieldValues[db2FieldIndexToSQLIndex[i] + j] = std::to_string(valueVector[j].v32);
                        }
                    }
                } else {
                    switch (dbdGlobalColumnDef->type) {
                        case FieldType::FLOAT: {
                            fieldValues[db2FieldIndexToSQLIndex[i] + j] = std::to_string(valueVector[j].v_f);
                            break;
                        };
                        case FieldType::STRING: {
                            fieldValues[db2FieldIndexToSQLIndex[i] + j] = normalRecord->readString(i, j * 4,
                                                                                                   valueVector[j].v32);
                            break;
                        }
                        case FieldType::INT: {
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
        }
    } else {
        auto sparseRecord = db2Base->getSparseRecord(recordIndex);
        if (sparseRecord == nullptr)
            return false;

        if (db2Base->getWDCHeader()->flags.hasNonInlineId) {
            fieldValues[recordIdExportIndex] = std::to_string(sparseRecord->getRecordId());
        }

        for (int i = 0; i < db2Base->getWDCHeader()->field_count; i++) {
            const decltype(buildConfig->columns)::value_type *dbdBuildColumnDef = nullptr;

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

            int arraySize = 1;
            int fieldSizeInBytes = 0;
            if (dbdGlobalColumnDef != nullptr) {
                if (dbdGlobalColumnDef->type == FieldType::STRING) {
                    auto stringVal = sparseRecord->readNextAsString();
                    fieldValues[db2FieldIndexToSQLIndex[i]] = stringVal;
                    continue;
                } else {
                    fieldSizeInBytes = dbdBuildColumnDef->bitSize >> 3;
                    if (dbdGlobalColumnDef->type == FieldType::FLOAT) {
                        fieldSizeInBytes = 4;
                    }
                    if (dbdBuildColumnDef->arraySize > 1) {
                        arraySize = dbdBuildColumnDef->arraySize;
                    }
                }
            }

            auto valueVector = sparseRecord->readNextField(fieldSizeInBytes, arraySize);
            for (int j = 0; j < valueVector.size(); j++) {
                if (dbdGlobalColumnDef == nullptr || dbdGlobalColumnDef->type != FieldType::FLOAT) {
                    fieldValues[db2FieldIndexToSQLIndex[i] + j] = std::to_string(valueVector[0].v32);
                } else {
                    fieldValues[db2FieldIndexToSQLIndex[i] + j] = std::to_string(valueVector[0].v_f);
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
    } else if (db2Base->hasRelationshipField()) {
        int j = fieldValues.size() - 1; //The relation field is the last field
        fieldValues[j] = std::to_string(db2Base->getRelationRecord(recordIndex));
    }

    return true;
}
