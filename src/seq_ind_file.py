from index_file import IndexFile
from database import Database
from record import GradesRecord

class SeqIndFile:
    def __init__(self, database_path: str, index_file_path: str):
        self.database = Database(database_path)
        self.index_file = IndexFile(index_file_path)

    def add_record(self, record: GradesRecord):
        page_number = self.index_file.get_page_to_insert(record)
        overflow = self.database.add_record(record, page_number)

if __name__ == "__main__":
    seq_ind_file = SeqIndFile("data/database.dat", "data/index_file.dat")
