//
// Created by Deamon on 8/25/2022.
//

#include "CImporterClass.h"
#include "../persistance/persistanceFile.h"
#include "../fileReaders/WDC2/DB2Base.h"
#include "../fileReaders/WDC3/DB2Ver3.h"
#include "WDC3/WDC3Importer.h"
#include "../fileReaders/WDC4/DB2Ver4.h"

#include <fstream>
#include <iostream>

void CImporterClass::addTable(std::string &tableName,
                              std::string db2File,
                              IExporter * exporter,
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


    } else if (*(uint32_t *)vec->data() == '3CDW' || *(uint32_t *)vec->data() == '4CDW') {
        std::shared_ptr<WDC3::DB2Ver3> db2Base = nullptr;

        if (*(uint32_t *)vec->data() == '4CDW') {
            db2Base = std::make_shared<WDC4::DB2Ver4>();
        } else {
            db2Base = std::make_shared<WDC3::DB2Ver3>();
        }
        db2Base->process( vec, "");
        DBDFile::BuildConfig *buildConfig = nullptr;

        auto dbdFile = fileDBDStorage->getDBDFile(tableName);
        if ( dbdFile!= nullptr ) {
            std::string tableNameFromDBD = fileDBDStorage->getTableName(db2Base->getWDCHeader()->table_hash);
            if (tableNameFromDBD != "") {
                tableName = tableNameFromDBD;
            }

            bool configFound = dbdFile->findBuildConfigByLayout(db2Base->getLayoutHash(), buildConfig);
            if (!configFound) {
                std::cout << "Could not find proper build config for table " << tableName <<
                          " for layout hash " << db2Base->getLayoutHash() << std::endl;

                buildConfig = nullptr;
            }
        } else {
            std::cout << "Could not find DBD file for table " << tableName << std::endl;

        }

        if (db2Base->getWDCHeader()->field_storage_info_size == 0) {
            if (db2Base->getWDCHeader()->record_count > 0) {
                std::cout << "DB2 " << tableName
                          << " do not have field info and have records. Unable to parse without build config in DBD file"
                          << std::endl;
            }

            if (buildConfig == nullptr) {
                std::cout << "DB2 " << tableName
                          << " do not have field info. Unable to parse without build config in DBD file"
                          << std::endl;
                return;
            }
        }

        WDC3Importer::processWDC3(tableName, db2Base, dbdFile, exporter, buildConfig);
    }
}
