from index_file import IndexFile
from database import Database
from record import GradesRecord

class SeqIndFile:
    def __init__(self, database_path: str, index_file_path: str):
        self.database = Database(database_path)
        self.index_file = IndexFile(index_file_path)
        self.database.clear_database()
        self.index_file.clear_index_file()

    def add_record(self, record: GradesRecord):
        page_index = self.database.add_record(record)
        self.index_file.add_index(record.key, page_index)

if __name__ == "__main__":
    seq_ind_file = SeqIndFile("data/database.dat", "data/index_file.dat")
