//
// Created by Deamon on 8/25/2022.
//

#ifndef DBIMPORTER_CIMPORTERCLASS_H
#define DBIMPORTER_CIMPORTERCLASS_H

#include "../fileReaders/DBD/DBDFileStorage.h"
#include "../exporters/IExporter.h"

class CImporterClass {
public:
    static void addTable(std::string &tableName,
                  std::string db2File,
                  IExporter * exporter,
                  std::shared_ptr<DBDFileStorage> fileDBDStorage);
};


#endif //DBIMPORTER_CIMPORTERCLASS_H
