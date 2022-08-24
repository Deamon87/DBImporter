//
// Created by Deamon on 8/25/2022.
//

#ifndef DBIMPORTER_CIMPORTERCLASS_H
#define DBIMPORTER_CIMPORTERCLASS_H

#include "../fileReaders/DBD/DBDFileStorage.h"

class CImporterClass {
    void addTable(std::string &tableName,
                  std::string db2File,
                  std::shared_ptr<DBDFileStorage> fileDBDStorage);
};


#endif //DBIMPORTER_CIMPORTERCLASS_H
