import os
import json
import typing
from record import GradesRecord

CONFIG_PATH = "configs/config.json"
with open(CONFIG_PATH, "r") as json_config:
    CONFIG = json.load(json_config)

PAGE_SIZE = CONFIG["BLOCKING_FACTOR"] * CONFIG["RECORD_SIZE"]


class Database:
    def __init__(self, path: str):
        self.path = path

    def clear_database(self):
        open(self.path, "w").close()

    def get_record(self, key: str, page_index: int) -> typing.Optional[GradesRecord]:
        page = self.read_page(page_index).decode().split("\n")
        found_records = [s for s in page if s.startswith(key)]
        print(page)

        assert len(found_records) <= 1, "Multiple records with same key found!"
        if len(found_records) == 0:
            return None

        record = found_records[0].split(" ")
        key, pointer, id, grades, deleted = record[0], \
                                            (record[1].split(":")[0], record[1].split(":")[1]), \
                                            int(record[2]), \
                                            [record[i] for i in [3, 4, 5]], \
                                            bool(int(record[6].split(":")[1]))

        return GradesRecord(key, pointer, id, grades, deleted)

    def read_page(self, page_index: int) -> bytes:
        with open(self.path, "rb") as database:
            database.seek(page_index * PAGE_SIZE, os.SEEK_SET)
            return database.read(PAGE_SIZE)

    def write_page(self, page_index: int, page: bytes):
        with open(self.path, "rb+") as database:
            database.seek(page_index * PAGE_SIZE, os.SEEK_SET)
            database.write(page)


if __name__ == "__main__":
    database = Database("data/database.dat")
    page = database.read_page(1)
    print(database.get_record("00002", 1))
