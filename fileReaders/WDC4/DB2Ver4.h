//
// Created by deamon on 02.04.18.
//

#ifndef WEBWOWVIEWERCPP_DB2BASE_WDC4_H
#define WEBWOWVIEWERCPP_DB2BASE_WDC4_H

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

namespace WDC4 {
    class DB2Ver4 : public WDC3::DB2Ver3 {
    public:
        void process(HFileContent db2File, const std::string &fileName) override;
    };
}

#endif //WEBWOWVIEWERCPP_DB2BASE_WDC4_H
