//
// Created by Deamon on 6/7/2022.
//

#ifndef DBIMPORTER_DBDFILESTORAGE_H
#define DBIMPORTER_DBDFILESTORAGE_H

#include <unordered_map>
#include <string>
#include <memory>
#include "DBDFile.h"

class DBDFileStorage {
public:
    DBDFileStorage(const std::string &directoryPath);

    void addOrReplaceDBDFile(std::string tableName, std::string dbdFilePath);

    std::shared_ptr<DBDFile> getDBDFile(std::string tableName);
    std::shared_ptr<DBDFile> getDBDFile(uint32_t tableHash);
    std::string getTableName(std::string db2FileName);
    std::string getTableName(uint32_t tableHash);
private:
    std::string m_directoryPath;

    //TableHash to DBDFile
    std::unordered_map<uint32_t, std::shared_ptr<DBDFile>> m_storage;
    std::unordered_map<uint32_t, std::string> m_tableHashToTableName;

    void loadDBDFiles();
};


#endif //DBIMPORTER_DBDFILESTORAGE_H
