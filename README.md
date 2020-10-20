 # DBImporter
 
 Small utility that imports WoW's WDC3 version of db2 files into SQLite database
 
```
 Usage: DBImporter -x <pathToDBDFiles> <pathToDBFiles> <sqliteFileName> 
 
 Options: 
   -x                   Mandatory flag for future uses
   <pathToDBDFiles>     Path to folder with database definition files *.dbd
   <pathToDBFiles>      Path to folder with db2 files. Right now only files with WDC3 header are supported
   <sqliteFileName>     File name for sqlite database. File will be created if it doesnt exist

 Example of usage: 
 DBImporter -x ../3rdparty/WoWDBDefs/definitions/ ../db2/ export.db3
```
You can find database definitions in https://github.com/wowdev/WoWDBDefs repository
