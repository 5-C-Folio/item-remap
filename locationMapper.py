from csv import DictReader
from functools import lru_cache
import json




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

if __name__ == "__main__":
        x = dictMap('c:\\Users\\aneslin\\Documents\\migration_five_colleges\\mapping_files\\locations.tsv')
        y = x.get_loc('ACASP AFAS')
        print(y)
        x.get_loc('ACFST AFMIC')
