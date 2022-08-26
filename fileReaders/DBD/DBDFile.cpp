//
// Created by deamon on 08.11.19.
//

#include <vector>
#include <fstream>
#include <algorithm>
#include "DBDFile.h"
#include "../../utils/string_utils.h"

void DBDFile::commitBuildConfig(SectionMode currentMode, BuildConfig &buildConfig) {
    if (currentMode == SectionMode::BUILD) {

        int columnIndex = 0;
        for (int k = 0; k < buildConfig.columns.size(); k++) {
            auto &columnDef = buildConfig.columns[k];

            if (columnDef.isNonInline) continue;
            columnDef.columnIndex = columnIndex++;
        }
        buildConfigs.push_back(buildConfig);
    }
}

DBDFile::DBDFile(std::string fileName) {
    std::ifstream infile(fileName);

    std::string line;

    SectionMode currentMode = SectionMode::NONE;

    BuildConfig buildConfig;
    while (std::getline(infile, line)) {
        if (line == "") {
            commitBuildConfig(currentMode, buildConfig);

            currentMode = SectionMode ::NONE;
            continue;
        }
        if (line == "COLUMNS") {
            currentMode = SectionMode::COLUMNS;
            continue;
        }
        if (line.find("COMMENT") != std::string::npos) {
//            currentMode = SectionMode::COLUMNS;
            continue;
        }
        if (line.find("BUILD") != std::string::npos) {
            if (currentMode == SectionMode::NONE) {
                buildConfig = BuildConfig();
            }

            std::vector<std::string> out;

            tokenize(line, " ", out);
            for (int i = 1; i < out.size(); i++) {
                std::vector<std::string> build;
                tokenize(out[i], ",", build);
                buildConfig.builds.push_back(build[0]);
            }

            currentMode = SectionMode::BUILD;
            continue;
        }
        if (line.find("LAYOUT") != std::string::npos) {
            if (currentMode == SectionMode::NONE) {
                buildConfig = BuildConfig();
            }

            std::vector<std::string> out;
            std::vector<std::string> layoutHashes;

            tokenize(line, " ", out);
            for (int i = 1; i < out.size(); i++) {
                std::vector<std::string> layoutHash;
                tokenize(out[i], ",", layoutHash);
                buildConfig.layoutHashes.push_back(layoutHash[0]);
            }

            currentMode = SectionMode::BUILD;
            continue;
        }

        switch (currentMode) {
            case SectionMode::COLUMNS:
                parseColumnDefLine(line);
                break;
            case SectionMode::BUILD:
                parseColumnBuildDefLine(line, buildConfig);
                break;
        }
    }
    commitBuildConfig(currentMode, buildConfig);
}

void DBDFile::parseColumnDefLine(std::string &line) {
    std::vector<std::string> out;

    tokenize(line, " ", out);

    ColumnDef newDef;
    std::string fieldType = out[0];

    newDef.fieldName = out[1];
    if (newDef.fieldName[newDef.fieldName.size() -1] == '?') {
        newDef.fieldName = newDef.fieldName.substr(0, newDef.fieldName.size() -1);
    }

    //If type has "<", extract the type and foreign key name
    if (fieldType.find("<") != std::string::npos) {

        int braceStart = fieldType.find('<', 0);
        int braceEnd = fieldType.find('>', braceStart);

        auto foreignKeyToken = fieldType.substr(braceStart+1, braceEnd - braceStart-1);
        fieldType = fieldType.substr(0, braceStart);

        std::vector<std::string> foreignOut;
        tokenize(foreignKeyToken, "::", foreignOut);

        newDef.foreingKey.tableName = foreignOut[0];
        newDef.foreingKey.fieldName = foreignOut[1];
    }

    if (fieldType.compare("int") == 0) {
        newDef.type = FieldType::INT;
    } else if (fieldType == "float") {
        newDef.type = FieldType::FLOAT;
    } else if (fieldType == "locstring") {
        newDef.type = FieldType::STRING;
    } else if (fieldType == "string") {
        newDef.type = FieldType::STRING;
    } else {
        throw std::runtime_error("oops!");
    }
    columnDefs[newDef.fieldName] = newDef;
//    columnDefs.insert(, newDef);
}

void DBDFile::parseColumnBuildDefLine(std::string &line, BuildConfig &buildConfig) {
    ColumnBuildDef buildDef;

    int prefixStart = line.find('$', 0);
    int prefixEnd = line.find('$', prefixStart+1);

    int braceStart = line.find('<', 0);
    int braceEnd = line.find('>', braceStart+1);

    int squareBraceStart = line.find('[', 0);
    int squareBraceEnd = line.find(']', squareBraceStart+1);

    int nameStart = 0;
    int nameEnd = line.size();

    if (prefixStart != std::string::npos ) {
        std::string prefix = line.substr(prefixStart+1, prefixEnd - prefixStart-1);
        if (prefix.find("id") != std::string::npos) {
            buildDef.isId = true;
        }
        if (prefix.find("noninline") != std::string::npos) {
            buildDef.isNonInline = true;
        }
        if (prefix.find("relation") != std::string::npos) {
            buildDef.isRelation = true;
        }

        nameStart = prefixEnd+1;
    }
    if (squareBraceStart != std::string::npos ) {
        nameEnd = squareBraceStart;
        auto arrayStr = line.substr(squareBraceStart+1, squareBraceEnd - squareBraceStart-1);
        buildDef.arraySize= std::atoi(arrayStr.c_str());
    }

    if (braceStart != std::string::npos ) {
        nameEnd = braceStart;
        auto bitStr = line.substr(braceStart+1, braceEnd - braceStart-1);
        if (bitStr == "64") {
            buildDef.isSigned = true;
            buildDef.bitSize = 64;
        } else if (bitStr == "32") {
            buildDef.isSigned = true;
            buildDef.bitSize = 32;
        } else if (bitStr == "u32") {
            buildDef.bitSize = 32;
        } else if (bitStr == "16") {
            buildDef.isSigned = true;
            buildDef.bitSize = 16;
        } else if (bitStr == "u16") {
            buildDef.bitSize = 16;
        } else if (bitStr == "u8") {
            buildDef.bitSize = 8;
        } else if (bitStr == "8") {
            buildDef.isSigned = true;
            buildDef.bitSize = 8;
        }
    }

    buildDef.fieldName = line.substr(nameStart, nameEnd - nameStart);

    buildConfig.columns.push_back(buildDef);
}

bool DBDFile::findBuildConfig(std::string buildVersionString, std::string layout, DBDFile::BuildConfig *&buildConfig_) {
    for (auto &buildConfig :buildConfigs) {
        if (std::find(buildConfig.builds.begin(), buildConfig.builds.end(), buildVersionString) != buildConfig.builds.end()) {
            buildConfig_ = &buildConfig;
            return true;
        }
    }
    return false;
}
bool DBDFile::findBuildConfigByLayout(std::string layout, DBDFile::BuildConfig *&buildConfig_) {
    for (auto &buildConfig :buildConfigs) {
        if (std::find(buildConfig.layoutHashes.begin(), buildConfig.layoutHashes.end(), layout) != buildConfig.layoutHashes.end()) {
            buildConfig_ = &buildConfig;
            return true;
        }
    }
    return false;
}
