//
// Created by Deamon on 8/24/2022.
//

#ifndef DBIMPORTER_WDC3IMPORTER_H
#define DBIMPORTER_WDC3IMPORTER_H

#include <string>
#include <vector>
#include "../../fileReaders/DBD/DBDFile.h"
#include "../../fileReaders/WDC3/DB2Ver3.h"
#include "../../exporters/IExporter.h"

class WDC3Importer {
public:
    static void processWDC3(const std::string &tableName,
                     const std::shared_ptr<WDC3::DB2Ver3> &db2Base,
                     const std::shared_ptr<DBDFile> &dbdFile,
                     IExporter * exporter,
                     const DBDFile::BuildConfig *buildConfig);

private:
    static std::vector<fieldInterchangeData> generateFieldsFromDBDColumns(
                                        const std::shared_ptr<DBDFile> &dbdFile,
                                        const DBDFile::BuildConfig *buildConfig,
                                        std::vector<int> &db2FieldIndexToOutputFieldIndex,
                                        std::vector<int> &dbdFieldIndexToOutputFieldIndex);

    static std::vector<fieldInterchangeData> generateFieldsFromDB2Columns(
            std::shared_ptr<WDC3::DB2Ver3> db2Base,
            std::vector<int> &db2FieldIndexToOutputFieldIndex);

    static bool readWDC3Record(const int recordIndex,
                        const int recordIdSqlIndex,
                        std::vector<std::string> &fieldValues,
                        const std::shared_ptr<WDC3::DB2Ver3> db2Base,
                        const std::shared_ptr<DBDFile> &dbdFile,
                        const DBDFile::BuildConfig *buildConfig,
                        const std::vector<int> &db2FieldIndexToSQLIndex,
                        const std::vector<int> &dbdFieldIndexToSQLIndex);
};


#endif //DBIMPORTER_WDC3IMPORTER_H
