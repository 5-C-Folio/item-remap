# item-remap
### Requirements
* Python 3.X
* cx_Oracle 
* 5C Aleph oracle read only credentials stored as passwords.json  

### Purpose
Call numbers in item records are not always stored in order, so the process of removing the subfields will not result in  
a properly parsed call number.  
This script pulls item records using the original Steve Item query, and views created by Steve and applies a call number parsing function that will detect call number fields  
prefix and suffix fields  

### Process
1) Obtain credentials for oracle and store in passwords.json file using format  
    `{"user" : --userName,  
    "password" --password,  
    "server": -- serverAddress}`
2)  comment out rownum in WHERE clause of SQL query. ROWNUM is included to avoid processing the entire database, which will be a very large file
3) Set chucks for generator function.  The generator function will prevent a memory error and apply the remap function.  It can go much higher than the default 100
4) Run program and specify Aleph school prefix code
5)  Run.  This will produce a very large file

### Known Issues
* Rownum and chunk size are hardcoded
* Script will put any $h or $i in call no, $k in prefix, and anything else in suffix.  "Anything else" might not be accurate
* export csv headers are included in the script.  They are very long and probably should live elsewhere.
* script is untested with a full extract 
* script is depenent on the existance of a Steve view in the Aleph database.  if the view is dropped, the script will fail.  

