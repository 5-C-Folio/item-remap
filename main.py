import cx_Oracle
from csv import DictWriter, DictReader
import json
from datetime import datetime
from functools import lru_cache

class dictMap():
    def __init__(self, file):
        self.file = file

    def __str__(self):
      return json.dumps(self.read_map, indent=4)


    @property
    def read_map(self):
        readobject = []
        locations = open(self.file, 'r')
        read_map = DictReader(locations, delimiter='\t')
        for row in read_map:
            readobject.append(row)
        return readobject


    @lru_cache()
    def get_loc(self, legCode ):
        for row in self.read_map:
            if row['legacy_code'] == legCode:
                return row["folio_code"]

def lc_parser(callNo):
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
    barcode = barcode.replace(" ", "")
    if len(barcode) < 15:
        barcode = f"{barcode}-{schoolCode}"
    print(barcode)
    return {"Z30_BARCODE":barcode}


def parse(row):
    callNo = row['Z30_CALL_NO']
    barcode = barcode_parse(row["Z30_BARCODE"],inst)
    row.update(barcode)
    x = locations_map.get_loc(f"{row['Z30_SUB_LIBRARY']} {row['Z30_COLLECTION'].replace(' ','')}")
    row.update({"folio_location": x})
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
    def __init__(self, connection, inst):
        self.connection = connection
        self.inst = inst

    def make_dict_factory(self, cursor):
        columnNames = [d[0] for d in cursor.description]

        def create_row(*args):
            return dict(zip(columnNames, args))
        return create_row

    def item_query(self):
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
    locations = ('c:\\Users\\aneslin\\Documents\\migration_five_colleges\\mapping_files\\locations.tsv')
    locations_map = dictMap(locations)
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
                "Z30_ENUMERATION_A",
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
               "folio_location"]
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
    f = open(f"items-{inst}-{now}.csv", 'a', newline='', encoding='utf-8')
    writer = DictWriter(f, fieldnames=headers)
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
