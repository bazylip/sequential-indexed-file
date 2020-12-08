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
        self.last_key = 0
        self.last_page_index = 0

    def clear_database(self):
        open(self.path, "w").close()

    def get_record(self, key: str, page_index: int) -> typing.Optional[GradesRecord]:
        page = self.read_page(page_index).decode().split("\n")
        found_records = [s for s in page if s.startswith(key)]

        assert len(found_records) <= 1, "Multiple records with same key found!"
        if len(found_records) == 0:
            return None

        record = found_records[0].split(" ")
        key, pointer, id, grades, deleted = record[0], \
                                            (int(record[1].split(":")[0]), int(record[1].split(":")[1])), \
                                            int(record[2]), \
                                            [record[i] for i in [3, 4, 5]], \
                                            bool(int(record[6].split(":")[1]))

        return GradesRecord(key, id, grades, pointer, deleted)

    def add_record(self, record: GradesRecord) -> int:
        pointer = (0, 0)
        record.add_overflow(pointer)

        page = self.read_page(self.last_page_index)
        while(len(page + record.to_bytes()) > PAGE_SIZE):
            self.last_page_index += 1
            page = self.read_page(self.last_page_index)

        self.write_page(self.last_page_index, page + record.to_bytes())
        return self.last_page_index

    def read_page(self, page_index: int) -> bytes:
        with open(self.path, "rb") as database:
            database.seek(page_index * PAGE_SIZE, os.SEEK_SET)
            return database.read(PAGE_SIZE)

    def write_page(self, page_index: int, page: bytes):
        with open(self.path, "rb+") as database:
            database.seek(page_index * PAGE_SIZE, os.SEEK_SET)
            database.write(page)

    def number_of_pages(self):
        i = 0
        while page := self.read_page(i):
            i += 1
        return i


if __name__ == "__main__":
    database = Database("data/database.dat")
    page = database.read_page(1)
    print(page)
    database.write_page(1, page)
    print(f"Find record 00002: {database.get_record('00002', 1)}")
    print(f"Number of pages: {database.number_of_pages()}")
