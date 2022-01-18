import cx_Oracle
cx_Oracle.init_oracle_client(lib_dir=r"C:\\oracle\\instantclient_12_1")
from csv import DictWriter, DictReader
import json
from datetime import datetime
from time import perf_counter
from functools import lru_cache
import os


@lru_cache(4)
def callnoType(alephCall):
    try:
        ctypes = {"0": "Library of Congress classification",
               "1": "Dewey Decimal classification",
               "2": "National Library of Medicine classification",
               "3": "Superintendent of Documents classification",
               "4": "Shelving control number",
               "5": "Title",
               "6": "Shelved separately",
               "7": "Source specified in subfield $2",
               "8": "Other scheme",
               "i": "Other scheme",
               "*": "Other scheme"}
        callNumber = ctypes[alephCall]
    except KeyError:
        callNumber = 'None'
    return callNumber


def del_dict(values : list, row: dict):
    for fields in values:
        if fields:
            row.pop(fields)
    return row


def field_merge(fields : list):
    mergeList = []
    for item in fields:
        if item:
            mergeList.append(item)
    if len(mergeList) > 0:
        mergedFields = " ".join(mergeList)
        return mergedFields


class dictMap:
    '''Take the mapping file and parse it into a dict to allow for matching '''
    def __init__(self, file, alephKey, folioValue, extraAlephKey=None):
        self.file = file
        self.alephKey = alephKey
        self.folioValue = folioValue
        self.extraAlephKey = extraAlephKey
        self.lookup_dict={}
        self.dictionify()

    def __str__(self):
        # str method. no use, just good practice
      return json.dumps(self.lookup_dict, indent=4)

    def dictionify(self): 
        try:
            with open(self.file, 'r') as mapfile:
                read_map = DictReader(mapfile, delimiter='\t')
                for row in read_map:
                    if self.extraAlephKey: 
                        self.lookup_dict[row[self.alephKey]+row[self.extraAlephKey]] = row[self.folioValue]
                    else:
                        self.lookup_dict[row[self.alephKey]] = row[self.folioValue]
        except FileNotFoundError:
            print(f"'{self.file}' not found. Check name and path")
            exit()
    
    @lru_cache(8)
    def matchx(self, legCode, fallback):
        try:
            folioMap = self.lookup_dict[legCode]
        except (AttributeError, KeyError):
            folioMap = fallback
        return(folioMap)
  

def lc_parser(callNo):
    # split call numbers.  if it's an H or I, it's the main call number, if k, then prefix. Anything else suffix.  Return
    callnosplit = callNo.split('$$')
    callNodict = {}
    suffix = []
    callnumber = []
    prefix = []
    for subfield in callnosplit:
        if len(subfield) > 0:
            if subfield[0] == 'h':
                callnumber.append(subfield[1:])
            elif subfield[0] == "i":
                callnumber.append(subfield[1:])
            elif subfield[0].strip() == "k":
                prefix.append((subfield[1:]))
            else:
                suffix.append(subfield[1:])

    callNodict["suffix"] = ' '.join(suffix)
    callNodict["call_number"] = ' '.join(callnumber)
    callNodict["prefix"] = ' '.join(prefix)
    return callNodict


def barcode_parse(barcode,schoolCode):
    # remove whitespace in barcodes. If the barcode doesn't match standard barcode length, append school code
    barcode = barcode.replace(" ", "")
    if len(barcode) < 15:
        barcode = f"{barcode}-{schoolCode}"
    # print(barcode)
    return {"Z30_BARCODE":barcode}


def parse(row):
    #call all functions to fix results
   
    barcode = barcode_parse(row["Z30_BARCODE"],inst)
    row.update(barcode)
    materialLookup = singleMatch_materials.matchx(row["Z30_MATERIAL"].rstrip(), "z")
    row.update({"material_type": materialLookup})
    loantype = loantype_map.matchx(row["Z30_SUB_LIBRARY"].rstrip()+row["Z30_ITEM_STATUS"], "oops")
    row.update( {"loanType": loantype})
    
    item_policy = item_policy_map.matchx(row["Z30_ITEM_PROCESS_STATUS"], "Available")
    row.update({"item_status": item_policy})
    
    #include the or row["Z30_TEMP_LOCATION"] == "N" if you want both temp and non temp
    if row["Z30_TEMP_LOCATION"] == "Y" or row["Z30_TEMP_LOCATION"] == "N" :
        callNo = row['Z30_CALL_NO']
        call_number_type = callnoType(row["Z30_CALL_NO_TYPE"])
        row.update({"Z30_CALL_NO_TYPE": call_number_type})
        # hacky change to not include call number if it's not a temp_location
        locationLookup = locations_map.matchx(f"{row['Z30_SUB_LIBRARY'].rstrip()} {row['Z30_COLLECTION'].rstrip()}", row['Z30_COLLECTION'])
        row.update({"folio_location": locationLookup})
        if callNo and "$$" in callNo:
            callNodict = lc_parser(callNo)
            row.update(callNodict)
        elif callNo:
            row.update({"call_number": callNo})
    else:
        row.update({"call_number": None})
    compositeEnum = field_merge([row["Z30_ENUMERATION_A"],
                                row["Z30_ENUMERATION_B"],
                                row["Z30_ENUMERATION_C"],
                                row["Z30_ENUMERATION_D"],
                                row["Z30_ENUMERATION_E"],
                                row["Z30_ENUMERATION_F"],
                                row["Z30_ENUMERATION_G"],
                                row["Z30_ENUMERATION_H"]])
    compositeChron = field_merge([row["Z30_CHRONOLOGICAL_I"],
                                 row["Z30_CHRONOLOGICAL_J"],
                                 row["Z30_CHRONOLOGICAL_K"],
                                 row["Z30_CHRONOLOGICAL_L"],
                                 row["Z30_CHRONOLOGICAL_M"]])

    row.update({"chronology": compositeChron})
    row.update({"enumeration": compositeEnum})
    del_dict(["Z30_ENUMERATION_A",
              "Z30_ENUMERATION_B",
              "Z30_ENUMERATION_C",
              "Z30_ENUMERATION_D",
              "Z30_ENUMERATION_E",
              "Z30_ENUMERATION_F",
              "Z30_ENUMERATION_G",
              "Z30_ENUMERATION_H",
              "Z30_CHRONOLOGICAL_I",
              "Z30_CHRONOLOGICAL_J",
              "Z30_CHRONOLOGICAL_K",
              "Z30_CHRONOLOGICAL_L",
              "Z30_CHRONOLOGICAL_M",
              "Z30_CATALOGER",
              "Z30_IP_LAST_RETURN",
              "Z30_ALPHA",
              "Z30_CALL_NO_KEY",
              "Z30_CALL_NO_2_TYPE",
              "Z30_CALL_NO_2",
              "Z30_CALL_NO_2_KEY",
              "Z30_IP_LAST_RETURN_V6",
              "Z30_REC_KEY_2",
              "Z30_REC_KEY_3",
              "Z30_SHELF_REPORT_NUMBER",
              "Z30_ON_SHELF_DATE",
              "Z30_ON_SHELF_SEQ",
              "Z30_LAST_SHELF_REPORT_DATE",
              ], row)
    try:
        return row
    except AttributeError:
        print("problem", row['Z30_BARCODE'])


class Query:
    '''Query the Aleph database to return all items at a given institution.  Call cleanup as a map function'''
    def __init__(self, connection, inst):
        self.connection = connection
        self.inst = inst

    def make_dict_factory(self, cursor):
        columnNames = [d[0] for d in cursor.description]
        #use zip to create dict from headers x values

        def create_row(*args):
            return dict(zip(columnNames, args))
        return create_row

    def item_query(self):
        # the query.  Use rownum limit for testing.
        cursor = self.connection.cursor()
        cursor.execute(f'''
        select *  from 
        (select KEY from UMA50.SB_KEY 
        where substr(KEY,-5)='{self.inst}50'), {self.inst}50.z30
        where substr(KEY,1,15)=Z30_REC_KEY
        --last line is limit for testing
        and ROWNUM < 100000
        ''')
        numrows = 500000
        while True:
            cursor.rowfactory = self.make_dict_factory(cursor)
            rows = cursor.fetchmany(numrows)
            if not rows:
                break
            else:
                print("retrieving")
                yield map(parse, rows)







if __name__ == "__main__":
    #todo add main class, wrap try except file read in function, add command line arguments for test vs full run
    #todo add file with mapping file locations, command line arguments for 
    # added directory of location mapping- this means changes to locations should happen here
    dir = os.path.dirname(__file__)
    print(dir)
    locations = os.path.join(dir,'mapping_files\\locations.tsv')   
    locations_map = dictMap(locations,'legacy_code', 'folio_code')
    loanTypes =  os.path.join(dir,'mapping_files\\loan_types.tsv')
    loantype_map = dictMap(loanTypes,'Z30_SUB_LIBRARY','folio_name', extraAlephKey= 'Z30_ITEM_STATUS')
    materialsTypes = os.path.join(dir,'mapping_files\\material_types.tsv')
    singleMatch_materials = dictMap(materialsTypes,"Z30_MATERIAL","folio_name")
    item_policies = os.path.join(dir,'mapping_files\\item_statuses.tsv')
    item_policy_map = dictMap(item_policies,"legacy_code", "folio_name")

    # oracle log in file
    with open(os.path.join(dir,"passwords.json"), "r") as pwFile:
        pw = json.load(pwFile)
    # define headers for csv - this should probably be in a separate file
    headers = ["KEY",
               "Z30_REC_KEY",
               "Z30_BARCODE",
               "Z30_SUB_LIBRARY",
               "Z30_MATERIAL",
               "Z30_ITEM_STATUS",
               "Z30_OPEN_DATE",
               "Z30_UPDATE_DATE",
               "Z30_DATE_LAST_RETURN",
               "Z30_HOUR_LAST_RETURN",
               "Z30_NO_LOANS",
               "Z30_COLLECTION",
               "Z30_CALL_NO_TYPE",
               "Z30_CALL_NO",
               "Z30_DESCRIPTION",
               "Z30_NOTE_OPAC",
               "Z30_NOTE_CIRCULATION",
               "Z30_NOTE_INTERNAL",
               "Z30_ORDER_NUMBER",
               "Z30_INVENTORY_NUMBER",
               "Z30_INVENTORY_NUMBER_DATE",
               "Z30_LAST_SHELF_REPORT_DATE",
               "Z30_PRICE",
               "Z30_PAGES",
               "Z30_ISSUE_DATE",
               "Z30_EXPECTED_ARRIVAL_DATE",
               "Z30_ARRIVAL_DATE",
               "Z30_ITEM_STATISTIC",
               "Z30_ITEM_PROCESS_STATUS",
               "Z30_HOL_DOC_NUMBER_X",
               "Z30_TEMP_LOCATION",
               "enumeration",
               "chronology",
               "Z30_SUPP_INDEX_O",
               "Z30_85X_TYPE",
               "Z30_DEPOSITORY_ID",
               "Z30_LINKING_NUMBER",
               "Z30_GAP_INDICATOR",
               "Z30_MAINTENANCE_COUNT",
               "Z30_PROCESS_STATUS_DATE",
               "Z30_UPD_TIME_STAMP",
               "prefix",
               "call_number", 
               "suffix",
               "folio_location",
               "loanType",
               "material_type",
               "item_status",
               "Z30_COPY_ID"]
    # used to get th right database
    global inst
    # define inst as global value to be used in barcode parse as well
    inst = input("enter three character school code> ")
    #try:
    print("connecting to DB")
    query_results = Query(cx_Oracle.connect(pw["user"], pw["password"], pw["server"]), inst)
    #except cx_Oracle.DatabaseError:
        #print("The Oracle Connection is not working. Check your connection, VPN and server address")
        #exit()
    # will yield a constructor that will be called until it returns no results
    query_results = query_results.item_query()
    now = datetime.now()
    now = now.strftime("%m-%d-%H%M")
    # create output file- will append since there will be multiple writes
    f = open(f"items-{inst}-{now}.tsv", 'a', newline='', encoding='utf-8')
    writer = DictWriter(f, fieldnames=headers, delimiter='\t')
    writer.writeheader()
    count = 0
    s = perf_counter()
    for row in query_results:
        for line in row:
            try:
                count +=1
                writer.writerow(line)
                #if count % 1000 == 0:
                    #print(count)
            except AttributeError:
                print("Error- NoneType?")
                continue
            except UnicodeEncodeError:
                print(line["Z30_BARCODE"], "unicode error")
                continue
    f.close()
    st = perf_counter()
    print(f"{count} records processed in {st - s:0.4f} seconds")
    query_results.close()
