import random
import json
import copy
from record import GradesRecord
from seq_ind_file import SeqIndFile

CONFIG_PATH = "configs/config.json"
with open(CONFIG_PATH, "r") as json_config:
    CONFIG = json.load(json_config)

MAX_KEY = CONFIG["MAX_KEY"]

seq_ind_file = SeqIndFile("data/database.dat", "data/overflow.dat", "data/index_file.dat")

key_save = ""
record_save = None

with open("data/keys.txt", "w") as key_file:
    for i in range(50):
        key = str(random.randrange(MAX_KEY)).rjust(len(str(MAX_KEY)), "0")
        key_file.write(key + "\n")
        record = GradesRecord(key)
        seq_ind_file.add_record(record)
        if i == 10:
            key_save = key
        if i == 12:
            record_save = copy.deepcopy(record)

print(f"deleting record: {key_save}")
seq_ind_file.delete_record(key_save)
print(f"updating record: {record_save}")
record_save.id = "183710"
seq_ind_file.update_record(record_save)
seq_ind_file.index_file.dump_to_file()
seq_ind_file.print_records()