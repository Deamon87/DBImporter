//
// Created by Deamon on 8/25/2022.
//

#include "CImporterClass.h"
#include "../persistance/persistanceFile.h"


#include <fstream>

void CImporterClass::addTable(std::string &tableName,
                              std::string db2File,
                              std::shared_ptr<DBDFileStorage> fileDBDStorage) {
    //Read DB2 into memory
    std::ifstream cache_file(db2File, std::ios::in |std::ios::binary);
    HFileContent vec;
    if (cache_file.good()) {
        cache_file.unsetf(std::ios::skipws);

        // get its size:
        std::streampos fileSize;

        cache_file.seekg(0, std::ios::end);
        fileSize = cache_file.tellg();
        cache_file.seekg(0, std::ios::beg);


        vec = std::make_shared<FileContent>(fileSize);
        cache_file.read((char *) &(*vec.get())[0], fileSize);
        std::cout << "DB2 File opened " << db2File << std::endl;
    } else {
        std::cout << "DB2 File was not found " << db2File << std::endl;
        return;
    }

    if (*(uint32_t *)vec->data() == 'WDC2') {
        WDC2::DB2Base db2Base;
        db2Base.process(vec, db2File);


    } else if (*(uint32_t *)vec->data() == '3CDW') {
        std::shared_ptr<WDC3::DB2Base> db2Base = std::make_shared<WDC3::DB2Base>();
        db2Base->process(vec, "");
        DBDFile::BuildConfig *buildConfig = nullptr;

        auto dbdFile = fileDBDStorage->getDBDFile(tableName);
        if ( dbdFile!= nullptr ) {
            std::string tableNameFromDBD = fileDBDStorage->getTableName(db2Base->getWDCHeader()->table_hash);
            if (tableNameFromDBD != "") {
                tableName = tableNameFromDBD;
            }

            bool configFound = dbdFile->findBuildConfigByLayout(db2Base->getLayoutHash(), buildConfig);
            if (!configFound) {
                std::cout << "Could not proper build find config for table " << tableName <<
                          "for layout hash " << db2Base->getLayoutHash() << std::endl;

                buildConfig = nullptr;
            }
        }


        if (db2Base->getWDCHeader()->field_storage_info_size == 0) {
            if (buildConfig == nullptr) {
                std::cout << "DB2 " << tableName
                          << " do not have field info. Unable to parse without build config in DBD file"
                          << std::endl;
                return;
            }
        }

        //TODO: HACK

        //Debug info dump
        //dumpDebugInfo(db2Base, buildConfig);

        processWDC3(tableName, db2Base, dbdFile, buildConfig);
    }
}
