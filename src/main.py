import random
import json
from database import Database
from record import GradesRecord
from seq_ind_file import SeqIndFile

CONFIG_PATH = "configs/config.json"
with open(CONFIG_PATH, "r") as json_config:
    CONFIG = json.load(json_config)

MAX_KEY = CONFIG["MAX_KEY"]

seq_ind_file = SeqIndFile("data/database1.dat", "data/index_file.dat")
for _ in range(30):
    key = str(random.randrange(MAX_KEY)).rjust(len(str(MAX_KEY)), "0")
    record = GradesRecord(key)
    seq_ind_file.add_record(record)

seq_ind_file.database.read_all_pages()
seq_ind_file.index_file.dump_to_file()
