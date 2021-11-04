import cx_Oracle
from csv import DictWriter, DictReader
import json
from datetime import datetime
from functools import lru_cache


def del_dict(values : [], row:{}):
    for fields in values:
        if fields:
            row.pop(fields)
    return row


def field_merge(fields : []):
    mergeList = []
    for item in fields:
        if item:
            mergeList.append(item)
    if len(mergeList) > 0:
        x = " ".join(mergeList)
        return x

class dictMap:
    '''Take the mapping file and parse it into a dict to allow for matching '''
    def __init__(self, file):
        self.file = file
        self.locMap = None
        self.read_map()

    def __str__(self):
        # str method. no use, just good practice
      return json.dumps(self.locMap, indent=4)


    def read_map(self):
        # read the dict into a json object.  Use tab as a delimiter.  Check to see if this is reoppening the file everytime?
        readobject = []
        locations = open(self.file, 'r')
        read_map = DictReader(locations, delimiter='\t')
        for row in read_map:
            readobject.append(row)
        self.locMap = readobject


    @lru_cache()
    def get_loc(self, legCode ):
        # match legacy sublibrary+collection to get folio code.  Remove random whitespace. There's not actually a reason for this to be a list- dict would be easier, but peformance is fine as is
        for row in self.locMap:
            if row['legacy_code'] == legCode.rstrip():
                x = row["folio_code"]
                break
            else:
                x = "tech"
        return x


class loc_dictMap(dictMap):

    def read_map(self):
        # read the dict into a json object.  Use tab as a delimiter.  Check to see if this is reoppening the file everytime?
        readobject = []
        locations = open(self.file, 'r')
        read_map = DictReader(locations, delimiter='\t')
        for row in read_map:
            comboRow = f"{row['Z30_SUB_LIBRARY']} {row['Z30_ITEM_STATUS']}"
            row.update({"aleph_loan": comboRow})
            readobject.append(row)
        self.locMap = readobject


    @lru_cache()
    def get_loan(self, legCode):
        # match legacy sublibrary+collection to get folio code.  Remove random whitespace
        for row in self.locMap:
            if row["aleph_loan"] == legCode.rstrip():
                x = row["folio_name"]
                break
            else:
                x = "Non-circulating"
        return x


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
    callNo = row['Z30_CALL_NO']
    barcode = barcode_parse(row["Z30_BARCODE"],inst)
    row.update(barcode)
    try:
        locationLookup = locations_map.get_loc(f"{row['Z30_SUB_LIBRARY']} {row['Z30_COLLECTION'].rstrip()}")
        row.update({"folio_location": locationLookup})
    except AttributeError:
        locationLookup = locations_map.get_loc(f"{row['Z30_SUB_LIBRARY']} {row['Z30_COLLECTION']}")
        row.update({"folio_location": locationLookup})
    loantypeLookup = loantype_map.get_loan(f"{row['Z30_SUB_LIBRARY']} {row['Z30_ITEM_STATUS']}")
    row.update({'loanType': loantypeLookup})
    compositeEnum = field_merge([row["Z30_ENUMERATION_A"],
                                row["Z30_ENUMERATION_B"],
                                row["Z30_ENUMERATION_C"],
                                row["Z30_ENUMERATION_D"],
                                row["Z30_ENUMERATION_E"],
                                row["Z30_ENUMERATION_F"],
                                row["Z30_ENUMERATION_G"],
                                row["Z30_ENUMERATION_H"]])
    if compositeEnum:
        print(row["Z30_BARCODE"], compositeEnum)
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
                                "Z30_CHRONOLOGICAL_M"], row)
    try:
        if callNo and "$$" in callNo:
            callNodict = lc_parser(callNo)
            row.update(callNodict)
            return row
        else:
            return row
    except AttributeError:
        return row


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
        --and ROWNUM < 10
        ''')
        numrows = 100000
        while True:
            cursor.rowfactory = self.make_dict_factory(cursor)
            rows = cursor.fetchmany(numrows)
            if not rows:
                break
            else:
                print("retrieving")
                yield map(parse, rows)


if __name__ == "__main__":
    # added directory of location mapping- this means changes to locations should happen here
    try:
        locations = ('c:\\Users\\aneslin\\Documents\\migration_five_colleges\\mapping_files\\locations.tsv')
    except FileNotFoundError:
        print("no valid location.tsv found.  Check the path")
        exit()
    locations_map = dictMap(locations)
    print(locations_map)
    try:
        loanTypes = ('c:\\Users\\aneslin\\Documents\\migration_five_colleges\\mapping_files\\loan_types.tsv')
    except FileNotFoundError:
        print("no valid loantype.tsv found.  Check the path")
        exit()
    loantype_map = loc_dictMap(loanTypes)
    print(loantype_map)
    # oracle log in file
    with open("passwords.json", "r") as pwFile:
        pw = json.load(pwFile)
    # define headers for csv - this should probably be in a seperate file
    headers = ["KEY",
                "Z30_REC_KEY",
                "Z30_BARCODE",
                "Z30_SUB_LIBRARY",
                "Z30_MATERIAL",
                "Z30_ITEM_STATUS",
                "Z30_OPEN_DATE",
                "Z30_UPDATE_DATE",
                "Z30_CATALOGER",
                "Z30_DATE_LAST_RETURN",
                "Z30_HOUR_LAST_RETURN",
                "Z30_IP_LAST_RETURN",
                "Z30_NO_LOANS",
                "Z30_ALPHA",
                "Z30_COLLECTION",
                "Z30_CALL_NO_TYPE",
                "Z30_CALL_NO",
                "Z30_CALL_NO_KEY",
                "Z30_CALL_NO_2_TYPE",
                "Z30_CALL_NO_2",
                "Z30_CALL_NO_2_KEY",
                "Z30_DESCRIPTION",
                "Z30_NOTE_OPAC",
                "Z30_NOTE_CIRCULATION",
                "Z30_NOTE_INTERNAL",
                "Z30_ORDER_NUMBER",
                "Z30_INVENTORY_NUMBER",
                "Z30_INVENTORY_NUMBER_DATE",
                "Z30_LAST_SHELF_REPORT_DATE",
                "Z30_PRICE",
                "Z30_SHELF_REPORT_NUMBER",
                "Z30_ON_SHELF_DATE",
                "Z30_ON_SHELF_SEQ",
                "Z30_REC_KEY_2",
                "Z30_REC_KEY_3",
                "Z30_PAGES",
                "Z30_ISSUE_DATE",
                "Z30_EXPECTED_ARRIVAL_DATE",
                "Z30_ARRIVAL_DATE",
                "Z30_ITEM_STATISTIC",
                "Z30_ITEM_PROCESS_STATUS",
                "Z30_COPY_ID",
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
                "Z30_IP_LAST_RETURN_V6",
                "prefix",
                "call_number",
                "suffix",
               "folio_location",
               "loanType"]
    # used to get th right database
    global inst
    # define inst as global value to be used in barcode parse as well
    inst = input("enter three character school code> ")
    query_results = Query(cx_Oracle.connect(pw["user"], pw["password"], pw["server"]), inst)
    # will yield a constructor that will be called until it returns no results
    query_results = query_results.item_query()
    now = datetime.now()
    now = now.strftime("%m-%d-%H%M")
    # create output file- will append since there will be multiple writes
    f = open(f"items-{inst}-{now}.tsv", 'a', newline='', encoding='utf-8')
    writer = DictWriter(f, fieldnames=headers, delimiter='\t')
    writer.writeheader()
    for row in query_results:
        for line in row:
            try:
                writer.writerow(line)
            except AttributeError:
                continue
            except UnicodeEncodeError:
                print(line["Z30_BARCODE"], "unicode error")
                continue
    f.close()
    query_results.close()
