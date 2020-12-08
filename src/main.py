import random
from database import Database
from record import GradesRecord
from seq_ind_file import SeqIndFile

"""
database = Database("data/database1.dat")
database.clear_database()
for _ in range(20):
    key = str(random.randrange(100)).rjust(5, "0")
    record = GradesRecord(key)
    database.add_record(record)

print(f"Number of pages: {database.number_of_pages()}")
"""

seq_ind_file = SeqIndFile("data/database1.dat", "data/index_file.dat")
for _ in range(20):
    key = str(random.randrange(100)).rjust(5, "0")
    record = GradesRecord(key)
    seq_ind_file.add_record(record)
