import random
import json
from database import Database
from record import GradesRecord
from seq_ind_file import SeqIndFile

CONFIG_PATH = "configs/config.json"
with open(CONFIG_PATH, "r") as json_config:
    CONFIG = json.load(json_config)

MAX_KEY = CONFIG["MAX_KEY"]

seq_ind_file = SeqIndFile("data/database.dat", "data/overflow.dat", "data/index_file.dat")
with open("data/keys.txt", "r") as key_file:
    keys = key_file.readlines()
    keys = [key.rstrip("\n") for key in keys]

key_save = ""
with open("data/keys.txt", "w") as key_file:
    for i in range(150):
        key = str(random.randrange(MAX_KEY)).rjust(len(str(MAX_KEY)), "0")
        # key = input("Key: ")
        #if key == "":
        #    break
        key_file.write(key + "\n")
        record = GradesRecord(key)
        seq_ind_file.add_record(record)
"""

for key in keys:
    key_pressed = input("Continue")
    if key_pressed == "q":
        break
    record = GradesRecord(key)
    seq_ind_file.add_record(record)
    seq_ind_file.database.read_all_pages()
    print("\nAll:")
    seq_ind_file.database.read_all_records()
"""

# seq_ind_file.database.read_all_pages()
#seq_ind_file.print_records()
#print(f"key_save: {key_save}")

#seq_ind_file.reorganize()
#seq_ind_file.print_records()
#seq_ind_file.database.read_all_pages()
seq_ind_file.index_file.dump_to_file()
seq_ind_file.print_records()