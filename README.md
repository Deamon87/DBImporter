 # DBImporter
 
 Small utility that imports WoW's db2 files into SQLite database
 
 ```
 Usage: DBImporter -x <pathToDBDFiles> <pathToDBFiles> <version> <sqliteFileName> 
 
 Options: 
   -x                   Mandatory flag for future uses
   <pathToDBDFiles>     Path to folder with database definition files *.dbd
   <pathToDBFiles>      Path to folder with db2 files. Right now only files with WDC3 header are supported
   <version>            Build and version of db2 files. For example: 8.3.0.32414
   <sqliteFileName>     File name for sqlite database. File will be created if it doesnt exist
   ```
You can find database definitions in https://github.com/wowdev/WoWDBDefs repository
