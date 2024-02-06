//
// Created by deamon on 02.04.18.
//

#ifndef WEBWOWVIEWERCPP_DB2BASE_WDC5_H
#define WEBWOWVIEWERCPP_DB2BASE_WDC5_H

#include <vector>
#include <functional>
#include <memory>
#include <unordered_map>
#include "../../persistance/persistanceFile.h"
#include "../WDC3/DB2Ver3.h"

#ifndef _MSC_VER
#define PACK( __Declaration__ ) __Declaration__ __attribute__((__packed__))
#else
#define PACK( __Declaration__ ) __pragma( pack(push, 1) ) __Declaration__ __pragma( pack(pop) )
#endif

namespace WDC5 {
    PACK(
        struct db2_header {
            uint32_t magic; // 'WDC5'
            uint32_t versionNum;             // 5, probably numeric version?
            char schemaString[128];
        }
    );

    class DB2Ver5 : public WDC3::DB2Ver3 {
    public:
        void process(HFileContent db2File, const std::string &fileName) override;
    };
}

#endif //WEBWOWVIEWERCPP_DB2BASE_WDC5_H
