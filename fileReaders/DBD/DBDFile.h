//
// Created by deamon on 08.11.19.
//

#ifndef DBIMPORTER_DBDFILE_H
#define DBIMPORTER_DBDFILE_H


#include <vector>
#include <string>
#include <unordered_map>
#include "../../importers/FieldInterchangeData.h"


class DBDFile {
public:
    DBDFile(std::string fileName);

private:


    struct ColumnDef {
        FieldType type;
        std::string fieldName;

        struct {
            std::string fieldName = "";
            std::string tableName = "";
        } foreingKey;
    };

    struct ColumnBuildDef {
        bool isId = false;
        bool isNonInline = false;
        bool isRelation = false;
        bool isSigned = false;
        int bitSize = 0;
        int arraySize = 0;
        std::string fieldName;
        int columnIndex = -1;
    };
public:
    struct BuildConfig {
        std::vector<std::string> layoutHashes = {};
        std::vector<std::string> builds = {};

        std::vector<ColumnBuildDef> columns = {};
    };
private:

    std::unordered_map<std::string, ColumnDef> columnDefs;
    std::vector<BuildConfig> buildConfigs;
public:
    bool findBuildConfig(std::string buildVersionString, std::string layout, DBDFile::BuildConfig *&buildConfig);
    bool findBuildConfigByLayout(std::string layout, DBDFile::BuildConfig *&buildConfig_);
    const ColumnDef *getColumnDef(const std::string &columnName) const {
        return &columnDefs.at(columnName);
    }
private:
    enum class SectionMode { NONE, COLUMNS, BUILD};

    void parseColumnDefLine(std::string &line);
    void parseColumnBuildDefLine(std::string &line, BuildConfig &buildConfig);
    void commitBuildConfig(SectionMode currentMode, BuildConfig &buildConfig);
};


#endif //DBIMPORTER_DBDFILE_H
