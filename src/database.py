import os
import json
import typing
from record import GradesRecord

CONFIG_PATH = "configs/config.json"
with open(CONFIG_PATH, "r") as json_config:
    CONFIG = json.load(json_config)

RECORD_SIZE = CONFIG["RECORD_SIZE"]
BLOCKING_FACTOR = CONFIG["BLOCKING_FACTOR"]
INITIAL_NO_OF_PAGES = CONFIG["INITIAL_NO_OF_PAGES"]
PAGE_SIZE = RECORD_SIZE * BLOCKING_FACTOR
PADDING_SYMBOL = b"\0" if CONFIG["PADDING_SYMBOL"] == "null" else CONFIG["PADDING_SYMBOL"].encode()


class Database:
    def __init__(self, path: str):
        self.path = path
        self.clear_database()
        self.initialize_empty_pages()

    def clear_database(self):
        open(self.path, "w").close()

    def initialize_empty_pages(self):
        for i in range(INITIAL_NO_OF_PAGES):
            empty_page = PADDING_SYMBOL*(PAGE_SIZE)
            self.write_page(i, empty_page)

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

    def add_record(self, record: GradesRecord, page_index: int) -> bool:
        record.add_overflow((0,0))  # TODO: fix this

        page = self.read_page(page_index)
        """
        if record.key > self.highest_key:
            if self.page_size(page + record.to_bytes()) > PAGE_SIZE:
                page_index += 1
            record_str = str(record).rstrip("\n")
            print(f"Adding record: {record_str} at the end of page {page_index}")
            self.write_page(page_index, page + record.to_bytes())
            return False
        """
        if self.page_size(page + record.to_bytes()) > PAGE_SIZE:
            record_str = str(record).rstrip("\n")
            print(f"overflow: {record_str}")
            return True

        offset = self.get_offset(page, record)
        page = self.update_page(page, offset, record)
        record_str = str(record).rstrip("\n")
        print(f"Adding record: {record_str} to page {page_index} at offset {offset}")
        self.write_page(page_index, page)
        return False

    def read_page(self, page_index: int) -> bytes:
        with open(self.path, "rb") as database:
            database.seek(page_index * PAGE_SIZE, os.SEEK_SET)
            return database.read(PAGE_SIZE)

    def read_all_pages(self):
        i = 0
        while page := self.read_page(i):
            print(f"PAGE {i}:")
            print(page.decode())
            i += 1

    def write_page(self, page_index: int, page: bytes):
        with open(self.path, "rb+") as database:
            database.seek(page_index * PAGE_SIZE, os.SEEK_SET)
            database.write(page)

    def update_page(self, page: bytes, offset: int, record: GradesRecord):
        #print(f"page before: {page}")
        page = page[:offset * RECORD_SIZE] + record.to_bytes() + page[offset * RECORD_SIZE:]
        #print(f"page after: {page}")
        return page

    def page_size(self, page: bytes):
        return len(page.replace(PADDING_SYMBOL, b""))

    def get_offset(self, page: bytes, record: GradesRecord) -> typing.Optional[int]:
        key = record.key.encode()
        page = page.replace(PADDING_SYMBOL, b"")
        if len(set(page)) <= 1:  # page is empty
            return 0
        #print(f"page: {page}")
        page_keys = [k.split(b" ")[0] for k in page.split(b"\n")][:-1]
        page_keys = [k for k in page_keys if k]  # remove empty strings
        #print(f"searching for key: {key}")
        #print(f"in page keys: {page_keys}")

        for i, page_key in enumerate(page_keys):
            if key < page_key:
                #print(f"next key: {page_key}, index: {i}")
                return i
        print(f"at the end, page keys: {page_keys}, len: {len(page_keys)}")
        return len(page_keys)

    def number_of_pages(self):
        i = 0
        while self.read_page(i):
            i += 1
        return i


if __name__ == "__main__":
    database = Database("data/database.dat")
    page = database.read_page(1)
    print(page)
    database.write_page(1, page)
    print(f"Find record 00002: {database.get_record('00002', 1)}")
    print(f"Number of pages: {database.number_of_pages()}")
