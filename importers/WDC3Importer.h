//
// Created by Deamon on 8/24/2022.
//

#ifndef DBIMPORTER_WDC3IMPORTER_H
#define DBIMPORTER_WDC3IMPORTER_H


class WDC3Importer {
    void processWDC3(std::string tableName, std::shared_ptr<WDC3::DB2Base> db2Base,
                     std::shared_ptr<DBDFile> m_dbdFile, DBDFile::BuildConfig *buildConfig);
};


#endif //DBIMPORTER_WDC3IMPORTER_H
