# item-remap
### Requirements
* Python 3.8 + 
* cx_Oracle 
* 5C Aleph oracle read only credentials stored as passwords.json  
* 5C location mapping TSV
* Loan type mapping tsv
* Item process status mapping tsv
* Material type mapping csv
### Purpose
Peform pre-item ingest cleanup activities  

Call numbers in item records are not always stored in order, so the process of removing the subfields will not result in a properly parsed call number.  
This script pulls item records using the original Steve Item query, and views created by Steve and applies a call number parsing function that will detect call number fields  
prefix and suffix fields  

System generated barcodes can be non-unique.  Remove any trailing barcode whitespace and for barcodes less than 15 characters, append institution code  

Map Aleph Sub Library/Collection to FOLIO locationcode.

Map Aleph Sub Library/Item process to FOLIO loan type

Map Aleph sublibrary/material types to FOLIO materials types.

### Setup

You must have tsvs for materials, loan types, item process status, 
loan types requre:
folio_name	Z30_SUB_LIBRARY	Z30_ITEM_STATUS
locations require:
Z30_SUB_LIBRARY	Z30_COLLECTION	legacy_code	folio_code
Item statuses require:
legacy_code	folio_name
Material types require: 
folio_name	Z30_MATERIAL

If you are doing a select  * , you may wish to delete fields. You can list these fields in data.py Same with the final headers for your folio ready item file.  Merger fields are currently hardcoded and stored in the parse function.  THey are set to merge all chron and enum fields.  


You will also need cx_Oracle.  I suggest using Anaconda, since compiling the C binaries from the Pypi version is a bear.  You will need an oracle instant client, and for reasons I don't understand, you will sometimes need to specify the file path.  

This query makes use of something I know as a "Steve table", populated by a query to filter the items we need to export.  Using the Z30 with your own export critera should be good enough.

### Process
1) Obtain credentials for oracle and store in passwords.json file using format  
    `{"user" : --userName,  
    "password" --password,  
    "dsn": -- serverAddress}`
2) Pull an up to date locations tsv file, and update script with correct directory
3) Pull an up to date loan type tsv file and update script with correct directory
4) comment out rownum in WHERE clause of SQL query. ROWNUM is included to avoid processing the entire database, which will be a very large file
5) Set chucks for generator function.  The generator function will prevent a memory error and apply the remap function.  It can go much higher than the default 100
6) Run program and specify Aleph school prefix code
7) Run.  This will produce a very large file and take around 11 seconds per 100k records.  

### Known Issues
* Rownum and chunk size are hardcoded
* Script will put any $h or $i in call no, $k in prefix, and anything else in suffix.  "Anything else" might not be accurate
* script is depenent on the existance of a Steve view in the Aleph database.  if the view is dropped, the script will fail.  
* git-sh decided the main branch should be called "master", which I don't like, and will fix later
* The output is location code and loan type name.  These are not the UUIDs 

### TODO
* add location.tsv directory as a parameter
* change master to main

