//
// Created by Deamon on 6/7/2022.
//

#include <cassert>
#include <filesystem>
#include <iostream>
#include <sstream>
#include "DBDFileStorage.h"

namespace fs = std::filesystem;

static char upper_backslash (char c) {
    return c == '/' ? '\\' : toupper (c);
}
static uint32_t const s_hashtable[16] = {
        0x486E26EE, 0xDCAA16B3, 0xE1918EEF, 0x202DAFDB,
        0x341C7DC7, 0x1C365303, 0x40EF2D37, 0x65FD5E49,
        0xD6057177, 0x904ECE93, 0x1C38024F, 0x98FD323B,
        0xE3061AE7, 0xA39B0FA1, 0x9797F25F, 0xE4444563,
};
uint32_t SStrHash (char const* string, bool no_caseconv, uint32_t seed)
{
    assert (string);
    if (!seed) {
        seed = 0x7FED7FED;
    }

    uint32_t shift = 0xEEEEEEEE;
    while (*string) {
        char c = *string++;
        if (!no_caseconv) {
            c = upper_backslash (c);
        }

        seed = (s_hashtable[c >> 4] - s_hashtable[c & 0xF]) ^ (shift + seed);
        shift = c + seed + 33 * shift + 3;
    }

    return seed ? seed : 1;
}

unsigned int hexToInt(const std::string &value) {
    std::stringstream ss;

    uint32_t returnValue;
    ss << std::hex << value;
    ss >> returnValue;

    return returnValue;
}

DBDFileStorage::DBDFileStorage(const std::string &directoryPath) : m_directoryPath(directoryPath) {
    loadDBDFiles();
}

void DBDFileStorage::loadDBDFiles() {
    fs::directory_iterator dirIterator;
    try {
        dirIterator = fs::directory_iterator(m_directoryPath);
    } catch (...) {
        return;
    }

    for (const auto& entry : dirIterator) {
        const auto filenameStr = entry.path().filename().string();
        if (entry.status().type() == fs::file_type::regular) {

            std::string dbdFileName = "";
            std::string fileExtension = "";
            auto pointPos = filenameStr.find(".");
            if (pointPos != std::string::npos) {
                dbdFileName = filenameStr.substr(0, pointPos);
                fileExtension = filenameStr.substr(pointPos + 1, filenameStr.size() - pointPos);
                if (fileExtension != "dbd") continue;
            } else {
                continue;
            }

            std::string tableName = dbdFileName;
            addOrReplaceDBDFile(tableName, entry.path().string());
        }
    }
}

void DBDFileStorage::addOrReplaceDBDFile(std::string tableName, std::string dbdFilePath) {

    uint32_t tableHash = SStrHash(tableName.c_str(), false, 0);

    //Specially named file
    const std::string tablehash_str = "tablehash_";
    if (tableName.substr(0, tablehash_str.size()) == tablehash_str) {
        std::string hex = tableName.substr(tablehash_str.size(), tableName.size());

        uint32_t tableHash = 0;
        try {
            tableHash = hexToInt(hex);
        } catch (...) {
            std::cout << "incorrectly named tablehash_*.dbd file found = " << tableName
                << ". Expected hex value representing tablehash after 'tablehash_'" << std::endl;
            return;
        }
    }

    std::shared_ptr<DBDFile> m_dbdFile = std::make_shared<DBDFile>(dbdFilePath);

    m_storage[tableHash] = m_dbdFile;
    m_tableHashToTableName[tableHash] = tableName;
}

std::shared_ptr<DBDFile> DBDFileStorage::getDBDFile(uint32_t tableHash) {
    if (auto it = m_storage.find(tableHash); it != m_storage.end()) {
        return it->second;
    }

    return nullptr;
}

std::shared_ptr<DBDFile> DBDFileStorage::getDBDFile(std::string tableName) {
    uint32_t tableHash = SStrHash(tableName.c_str(), false, 0);

    return getDBDFile(tableHash);
}

std::string DBDFileStorage::getTableName(std::string db2FileName) {
    auto pointPos = db2FileName.find(".");
    if (pointPos != std::string::npos) {
        db2FileName = db2FileName.substr(0, pointPos);
    }
    uint32_t tableHash = SStrHash(db2FileName.c_str(), false, 0);

    return getTableName(tableHash);
}

std::string DBDFileStorage::getTableName(uint32_t tableHash) {
    if (auto it = m_tableHashToTableName.find(tableHash); it != m_tableHashToTableName.end()) {
        return it->second;
    }

    return "";
}

