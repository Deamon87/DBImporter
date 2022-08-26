//
// Created by Deamon on 8/25/2022.
//

#ifndef DBIMPORTER_IEXPORTER_H
#define DBIMPORTER_IEXPORTER_H

#include <string>
#include <vector>
#include <functional>
#include "../importers/FieldInterchangeData.h"


class IExporter {
public:
    virtual ~IExporter() = default;
    virtual void addTableData(
            std::string tableName,
            std::vector <fieldInterchangeData> &fieldDefs,
            const std::function<void(const std::function <void(std::vector<std::string>&)>& )> &fieldValueIterator,
            const std::function<void(const std::function <void(int, int)>& )> &copyIterator
    ) = 0;
};

#endif //DBIMPORTER_IEXPORTER_H
