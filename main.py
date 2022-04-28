from csv import DictWriter, DictReader
from datetime import datetime
from time import perf_counter
from functools import lru_cache
import json
import os
from typing import Type
import cx_Oracle
# cx_oracle requies installation of the oracle instant client. Depending on system setup, it may require the path
cx_Oracle.init_oracle_client(lib_dir=r"C:\\oracle\\INSTantclient_12_1")
try:
    from passwords import logIn
    from data import headers, deleteList
except FileNotFoundError:
    print("password or data files not found")

@lru_cache(4)
def call_no_type(aleph_call):
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
        call_number = ctypes[aleph_call]
    except KeyError:
        call_number = 'None'
    return call_number


def del_dict(values : list, row: dict):
    for fields in values:
        if fields:
            row.pop(fields)
    return row


def field_merge(fields : list):
    merge_list = []
    for item in fields:
        if item:
            merge_list.append(item)
    if len(merge_list) > 0:
        merged_fields = " ".join(merge_list)
        return merged_fields

class DictMap:
    '''Take the mapping file and parse it into a dict to allow for matching '''
    def __init__(self, file, aleph_key, folio_value, extra_aleph_key=None,):
        self.file = file
        self.aleph_key = aleph_key
        self.folio_value = folio_value
        self.extra_aleph_key = extra_aleph_key
        self.lookup_dict={}
        self.dictionify()

    def __str__(self):
        # str method. no use, just good practice
        return json.dumps(self.lookup_dict, indent=4)
    def dictionify(self):
        try:
            with open(self.file, 'r',encoding='utf8') as mapfile:
                read_map = DictReader(mapfile, delimiter='\t')
                for row in read_map:
                    if self.extra_aleph_key:
                        self.lookup_dict[row[self.aleph_key]+row[self.extra_aleph_key]] = row[self.folio_value]
                    else:
                        self.lookup_dict[row[self.aleph_key]] = row[self.folio_value]
        except FileNotFoundError:
            print(f"'{self.file}' not found. Check name and path")
            exit()

    @lru_cache(8)
    def matchx(self, legacy_code, fallback):
        try:
            legacy_code = legacy_code.rstrip()
            folio_map = self.lookup_dict[legacy_code]
        except (AttributeError, KeyError, TypeError):
            folio_map = fallback
        return folio_map

def lc_parser(call_num):
    # split call numbers.  if it's an H or I, it's the main call number, if k, then prefix. Anything else suffix.  Return
    call_num_split = call_num.split('$$')
    call_num_dict = {}
    suffix = []
    call_number = []
    prefix = []
    for subfield in call_num_split:
        if len(subfield) > 0:
            if subfield[0] == 'h':
                call_number.append(subfield[1:])
            elif subfield[0] == "i":
                call_number.append(subfield[1:])
            elif subfield[0].strip() == "k":
                prefix.append((subfield[1:]))
            else:
                suffix.append(subfield[1:])

    call_num_dict["suffix"] = ' '.join(suffix)
    call_num_dict["call_number"] = ' '.join(call_number)
    call_num_dict["prefix"] = ' '.join(prefix)
    return call_num_dict


def barcode_parse(barcode,school_code):
    # remove whitespace in barcodes. If the barcode doesn't match standard barcode length, append school code
        barcode = barcode.replace(" ", "")
        try:
            if len(barcode) < 15 :
                barcode = f"{barcode}-{school_code}"        
            return {"Z30_BARCODE":barcode}
            
        except TypeError:
            print("hello")
            return {"Z30_BARCODE":'ERROR'}

        



def parse(row):
    #call all functions to fix results 
   
    barcode = barcode_parse(row["Z30_BARCODE"],INST)
    row.update(barcode)
    
    materialLookup = singleMatch_materials.matchx(row["Z30_MATERIAL"], row["Z30_MATERIAL"])
    row.update({"material_type": materialLookup})
    try:
        loantype = loantype_map.matchx(row["Z30_SUB_LIBRARY"]+row["Z30_ITEM_STATUS"],'Non-circulating')
    except TypeError:
        loantype = 'Non-circulating'
       
    row.update( {"loanType": loantype})
    item_policy = item_policy_map.matchx(row["Z30_ITEM_PROCESS_STATUS"], "Available")
    row.update({"item_status": item_policy})
    #include the or row["Z30_TEMP_LOCATION"] == "N" if you want both temp and non temp
    if row["Z30_TEMP_LOCATION"] == "Y":
        callNo = row['Z30_CALL_NO']
        call_number_type = call_no_type(row["Z30_CALL_NO_TYPE"])
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
    del_dict(deleteList, row)
    try:
        return row
    except AttributeError:
        print("problem", row['Z30_BARCODE'])


class Query:
    '''Query the Aleph database to return all items at a given INSTitution.  Call cleanup as a map function'''
    def __init__(self, connection, INST):
        self.connection = connection
        self.INST = INST

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
        where substr(KEY,-5)='{self.INST}50'), {self.INST}50.z30
        where substr(KEY,1,15)=Z30_REC_KEY
        --last line is limit for testing
        --and ROWNUM < 100000
        ''')
        numrows = 800000
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
    # added directory of location mapping- this means changes to locations should happen here
    dir = os.path.dirname(__file__)
    print(dir)
    locations = os.path.join(dir,'mapping_files\\locations.tsv')   
    locations_map = DictMap(locations,'legacy_code', 'folio_code')
    loanTypes =  os.path.join(dir,'mapping_files\\loan_types.tsv')
    loantype_map = DictMap(loanTypes,'Z30_SUB_LIBRARY','folio_name', extra_aleph_key= 'Z30_ITEM_STATUS')
    materialsTypes = os.path.join(dir,'mapping_files\\material_types.tsv')
    singleMatch_materials = DictMap(materialsTypes,"Z30_MATERIAL","folio_name")
    item_policies = os.path.join(dir,'mapping_files\\item_statuses.tsv')
    item_policy_map = DictMap(item_policies,"legacy_code", "folio_name")
    
    # used to get th right database
    global INST
    # define INST as global value to be used in barcode parse as well
    INST = input("enter three character school code> ")
    try:
        print("connecting to DB")
        query_results = Query(cx_Oracle.connect(logIn["user"], logIn["password"], logIn["dsn"]), INST)
    except cx_Oracle.DatabaseError:
        print("The Oracle Connection is not working. Check your connection, VPN and server address")
        exit()
    # will yield a constructor that will be called until it returns no results
    query_results = query_results.item_query()
    now = datetime.now()
    now = now.strftime("%m-%d-%H%M")
    # create output file- will append since there will be multiple writes
    f = open(f"output/items-{INST}-{now}.tsv", 'a', newline='', encoding='utf-8')
    writer = DictWriter(f, fieldnames=headers, delimiter='\t')
    writer.writeheader()
    COUNT = 0
    s = perf_counter()
    for row in query_results:
        for line in row:
            try:
                COUNT +=1
                writer.writerow(line)
                if COUNT % 500000 == 0:
                    print(COUNT)
            except AttributeError:
                print("Error- NoneType?")
                continue
            except UnicodeEncodeError:
                print(line["Z30_BARCODE"], "unicode error")
                continue
    f.close()
    st = perf_counter()
    print(f"{COUNT} records processed in {st - s:0.4f} seconds")
    query_results.close()
