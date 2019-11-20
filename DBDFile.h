//
// Created by deamon on 08.11.19.
//

#ifndef DBIMPORTER_DBDFILE_H
#define DBIMPORTER_DBDFILE_H


#include <string>
#include <unordered_map>
enum class FieldType { INT, FLOAT, STRING};

class DBDFile {
public:
    DBDFile(std::string fileName);

private:


    struct ColumnDef {
        FieldType type;
        std::string fieldName;
//        size_t fieldSize;

        struct {
            std::string fieldName = "";
            std::string tableName = "";
        } foreingKey;
    };

    struct ColumnBuildDef {
        bool isId = false;
        bool isNonInline = false;
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
    ColumnDef &getColumnDef(std::string &columnName) {
        return columnDefs[columnName];
    }
private:
    enum class SectionMode { NONE, COLUMNS, BUILD};

    void parseColumnDefLine(std::string &line);
    void parseColumnBuildDefLine(std::string &line, BuildConfig &buildConfig);
    void CommitBuildConfig(SectionMode currentMode, BuildConfig &buildConfig);
};


#endif //DBIMPORTER_DBDFILE_H
