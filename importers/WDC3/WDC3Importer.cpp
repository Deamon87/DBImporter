//
// Created by Deamon on 8/24/2022.
//

#include "WDC3Importer.h"
#include "../../utils/string_utils.h"

/*
void dumpDebugInfo(std::shared_ptr<WDC3::DB2Base> &db2Base, DBDFile::BuildConfig *buildConfig) const {
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
                               const std::shared_ptr<WDC3::DB2Base> &db2Base,
                               const std::shared_ptr<DBDFile> &dbdFile,
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

    int recordIndex = 0;
    auto readRecordsLambda = [&](std::vector<std::string> &fieldValues) mutable -> bool {
        for (int i = recordIndex; i < db2Base->getRecordCount(); i++) {
            bool recordRead = readWDC3Record(i, exportIdIndex,
                                             fieldValues,
                                             db2Base, dbdFile,
                                             buildConfig,
                                             db2FieldIndexToOutputFieldIndex,
                                             dbdFieldIndexToOutputFieldIndex);

            if (recordRead) {
                recordIndex = i;
                return true;
            }
        }

        return false;
    };

}

std::vector<fieldInterchangeData>
WDC3Importer::generateFieldsFromDB2Columns(std::shared_ptr<WDC3::DB2Base> db2Base,
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

        if (db2Field->storage_type == WDC3::field_compression_bitpacked_indexed_array &&
            db2Field->field_compression_bitpacked_indexed_array.array_count > 1) {
            for (int j = 0; j < db2Field->field_compression_bitpacked_indexed_array.array_count; j++) {
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
            });

            dbdFieldIndexToOutputFieldIndex[i] = exportFieldIndex++;
            continue;
        }

        if (columnDef.arraySize > 1) {
            for (int j = 0; j < columnDef.arraySize; j++) {
                auto *columnTypeDef = m_dbdFile->getColumnDef(columnDef.fieldName);

                std::string columnName = colFieldName + "_" + std::to_string(j);

                result.push_back({
                  columnName,
                    false,
                    columnTypeDef->type,
                });

                dbdFieldIndexToOutputFieldIndex[i] = exportFieldIndex++;
            }
            db2FieldIndexToOutputFieldIndex[i] = db2FieldIndex++;
        } else {
            auto *columnTypeDef = m_dbdFile->getColumnDef(columnDef.fieldName);

            result.push_back({
                   colFieldName,
                     columnDef.isId,
                     columnTypeDef->type,
            });
            db2FieldIndexToOutputFieldIndex[i] = db2FieldIndex++;
        }
    }

    return result;
}

bool WDC3Importer::readWDC3Record(const int recordIndex,
                                  const int recordIdExportIndex,
                                  std::vector<std::string> &fieldValues,
                                  const std::shared_ptr<WDC3::DB2Base> db2Base,
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
            fieldValues[recordIdExportIndex] = std::to_string(sparseRecord->getRecordId());
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
