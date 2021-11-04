# item-remap
### Requirements
* Python 3.X
* cx_Oracle 
* 5C Aleph oracle read only credentials stored as passwords.json  
* 5C location mapping TSV
### Purpose
Peform pre-item ingest cleanup activities  

Call numbers in item records are not always stored in order, so the process of removing the subfields will not result in a properly parsed call number.  
This script pulls item records using the original Steve Item query, and views created by Steve and applies a call number parsing function that will detect call number fields  
prefix and suffix fields  

System generated barcodes can be non-unique.  Remove any trailing barcode whitespace and for barcodes less than 15 characters, append institution code  

Map Aleph Sub Library/Collection to FOLIO locationcode.

Map Aleph Sub Library/Item process to FOLIO loan type

### Process
1) Obtain credentials for oracle and store in passwords.json file using format  
    `{"user" : --userName,  
    "password" --password,  
    "server": -- serverAddress}`
2) Pull an up to date locations tsv file, and update script with correct directory
3) Pull an up to date loan type tsv file and update script with correct directory
4) comment out rownum in WHERE clause of SQL query. ROWNUM is included to avoid processing the entire database, which will be a very large file
5) Set chucks for generator function.  The generator function will prevent a memory error and apply the remap function.  It can go much higher than the default 100
6) Run program and specify Aleph school prefix code
7) Run.  This will produce a very large file

### Known Issues
* Rownum and chunk size are hardcoded
* Script will put any $h or $i in call no, $k in prefix, and anything else in suffix.  "Anything else" might not be accurate
* export csv headers are included in the script.  They are very long and probably should live elsewhere.
* script is depenent on the existance of a Steve view in the Aleph database.  if the view is dropped, the script will fail.  
* git-sh decided the main branch should be called "master", which I don't like, and will fix later
* Directory of locations.tsv is hardcoded 
* The output is location code and loan type name.  These are not the UUIDs 

### TODO
* add location.tsv directory as a parameter
* change master to main
* Concat enum and chron into a single field
* Remove unneeded aleph fields

