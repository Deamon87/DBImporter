//
// Created by Deamon on 8/24/2022.
//

#ifndef DBIMPORTER_FIELDINTERCHANGEDATA_H
#define DBIMPORTER_FIELDINTERCHANGEDATA_H

#include <string>

enum class FieldType { INT, FLOAT, STRING};

struct fieldInterchangeData {
    std::string fieldName;
    bool isId;
    FieldType fieldType;
    bool isForeignKey;
};

#endif //DBIMPORTER_FIELDINTERCHANGEDATA_H
