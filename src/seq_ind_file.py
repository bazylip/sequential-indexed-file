from index_file import IndexFile
from database import Database

class SeqIndFile:
    def __init__(self, database_path: str, index_file_path: str):
        self.database = Database(database_path)
        self.index_file = IndexFile(index_file_path)


if __name__ == "__main__":
    seq_ind_file = SeqIndFile("data/database.dat", "data/index_file.dat")
