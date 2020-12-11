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
MAX_KEY = CONFIG["MAX_KEY"]
MAX_OVERFLOW_PAGE_NO = CONFIG["MAX_OVERFLOW_PAGE_NO"]
PAGE_SIZE = RECORD_SIZE * BLOCKING_FACTOR
PADDING_SYMBOL = b"\0" if CONFIG["PADDING_SYMBOL"] == "null" else CONFIG["PADDING_SYMBOL"].encode()


class Overflow:
    def __init__(self, path: str):
        self.path = path
        self.current_page_index = 0
        self.current_offset = 0
        self.clear_overflow()

    def clear_overflow(self):
        open(self.path, "w").close()

    def read_page(self, page_index: int):
        with open(self.path, "br") as overflow:
            overflow.seek(page_index * PAGE_SIZE, os.SEEK_SET)
            return overflow.read(PAGE_SIZE)

    def write_page(self, page_index: int, page: bytes):
        with open(self.path, "br+") as overflow:
            overflow.seek(page_index * PAGE_SIZE, os.SEEK_SET)
            overflow.write(page)

    def get_new_pointer(self) -> typing.Tuple[int, int]:
        return self.current_page_index, self.current_offset

    def add_record(self, record: GradesRecord):
        page = self.read_page(self.current_page_index)
        page = page[:self.current_offset * RECORD_SIZE] + record.to_bytes() + page[self.current_offset * RECORD_SIZE:]
        self.write_page(self.current_page_index, page)
        pointer = self.get_new_pointer()
        self.current_offset += 1
        if self.current_offset == BLOCKING_FACTOR:
            self.current_page_index += 1
            self.current_offset = 0
        return pointer


class Database:
    def __init__(self, database_path: str, overflow_path: str):
        self.path = database_path
        self.overflow = Overflow(overflow_path)
        self.clear_database()
        self.initialize_empty_pages()
        self.add_dummy_record()

    def clear_database(self):
        open(self.path, "w").close()

    def initialize_empty_pages(self):
        for i in range(INITIAL_NO_OF_PAGES):
            empty_page = PADDING_SYMBOL*PAGE_SIZE
            self.write_page(i, empty_page)

    def add_dummy_record(self):
        first_key = "".rjust(len(str(MAX_KEY)), "0")
        dummy_record = GradesRecord(first_key)
        self.add_record(dummy_record, 0)

    def generate_record_from_string_list(self, record: typing.List[str]):
        key = record[0]
        pointer = (-1, -1) if (record[1].split(":")[0], record[1].split(":")[1]) == ("x", "x") else (int(record[1].split(":")[0]), int(record[1].split(":")[1]))
        id = int(record[2])
        grades = [record[i] for i in [3, 4, 5]]
        deleted = bool(int(record[6].split(":")[1]))

        return GradesRecord(key, id, grades, pointer, deleted)


    def get_record_by_key(self, key: str, page_index: int) -> typing.Optional[GradesRecord]:
        page = self.read_page(page_index).decode().split("\n")
        found_records = [s for s in page if s.startswith(key)]

        assert len(found_records) <= 1, "Multiple records with same key found!"
        if len(found_records) == 0:
            return None

        record = found_records[0].split(" ")
        return self.generate_record_from_string_list(record)

    def get_record_at_offset(self, offset: int, page: bytes) -> typing.Optional[GradesRecord]:
        page = page.replace(PADDING_SYMBOL, b"")
        page = page.decode().split("\n")
        page = [record for record in page if record]
        if not page or len(page) <= offset:
            return None
        record = page[offset].split(" ")
        return self.generate_record_from_string_list(record)

    def add_record(self, record: GradesRecord, page_index: int) -> bool:
        page = self.read_page(page_index)

        if self.page_size(page + record.to_bytes()) > PAGE_SIZE:
            record_str = str(record).rstrip("\n")
            print(f"overflow: {record_str}")
            self.set_overflow(page, page_index, record)
            return True

        offset = self.get_offset(page, record)
        page = self.update_page(page, offset, record)
        record_str = str(record).rstrip("\n")
        print(f"ADDING RECORD: {record_str} to page {page_index} at offset {offset}")
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

    def update_page(self, page: bytes, offset: int, record: GradesRecord, replace: bool = False):
        if not replace:
            page = page[:offset * RECORD_SIZE] + record.to_bytes() + page[offset * RECORD_SIZE:]
            return page[:PAGE_SIZE]
        else:
            page = page[:offset * RECORD_SIZE] + record.to_bytes() + page[offset * RECORD_SIZE + RECORD_SIZE:]
            return page

    def page_size(self, page: bytes):
        return len(page.replace(PADDING_SYMBOL, b""))

    def get_offset(self, page: bytes, record: GradesRecord) -> int:
        key = record.key.encode()
        page = page.replace(PADDING_SYMBOL, b"")
        if len(set(page)) <= 1:  # page is empty
            return 0
        page_keys = [k.split(b" ")[0] for k in page.split(b"\n")][:-1]
        page_keys = [k for k in page_keys if k]  # remove empty strings

        for i, page_key in enumerate(page_keys):
            if key < page_key:
                return i
        return len(page_keys)

    def get_last_offset_of_page(self, page: bytes) -> int:
        page = page.replace(PADDING_SYMBOL, b"")
        if len(set(page)) <= 1:  # page is empty
            return 0
        page_keys = [k.split(b" ")[0] for k in page.split(b"\n")][:-1]
        page_keys = [k for k in page_keys if k]  # remove empty strings
        return len(page_keys)-1

    def number_of_pages(self):
        i = 0
        while self.read_page(i):
            i += 1
        return i

    def set_overflow(self, page: bytes, current_page_index: int, overflow_record: GradesRecord):
        overflow_pointer = self.overflow.get_new_pointer()
        record_to_update_page_index, record_to_update_offset = self.resolve_previous_record_in_main_area(page, current_page_index, overflow_record)
        print(f"RECORD: {overflow_record} prev_page_index: {record_to_update_page_index}, prev_offset: {record_to_update_offset}")
        print(f"OVERFLOW POINTER: {overflow_pointer}")
        if record_to_update_page_index != current_page_index:
            page = self.read_page(record_to_update_page_index)
            record_to_update_offset = self.get_last_offset_of_page(page)

        if overflow_pointer[0] > MAX_OVERFLOW_PAGE_NO:
            print(f"OVERFLOW POINTER {overflow_pointer[0]} TOO LARGE, REORGANIZE!")
            return

        while record_to_update := self.get_record_at_offset(record_to_update_offset, page) is None:
            record_to_update_page_index -= 1
            page = self.read_page(record_to_update_page_index)
            record_to_update_offset = self.get_last_offset_of_page(page)

        record_to_update = self.get_record_at_offset(record_to_update_offset, page)
        print(f"record to update in main area: {record_to_update}")

        if record_to_update.pointer == (-1, -1):  # record currently points to nothing
            record_to_update.pointer = overflow_pointer
            page = self.update_page(page, record_to_update_offset, record_to_update, replace=True)
            self.write_page(record_to_update_page_index, page)
        else:  # traverse the list
            update_record_in_main_area = True
            previous_record = record_to_update
            previous_page = page
            previous_page_index, previous_offset = record_to_update_page_index, record_to_update_offset

            next_record_page_index = record_to_update.pointer[0]
            next_record_offset = record_to_update.pointer[1]
            next_page = self.overflow.read_page(next_record_page_index)
            next_record = self.get_record_at_offset(next_record_offset, next_page)
            print(f"next in list: {next_record}")

            while overflow_record > next_record and next_record.pointer != (-1, -1):
                update_record_in_main_area = False
                previous_record = next_record
                previous_page = next_page
                previous_page_index, previous_offset = next_record_page_index, next_record_offset


                next_record_page_index = next_record.pointer[0]
                next_record_offset = next_record.pointer[1]
                if next_record_page_index != previous_page_index:
                    next_page = self.overflow.read_page(next_record_page_index)
                next_record = self.get_record_at_offset(next_record_offset, next_page)
                print(f"next in list: {next_record}")

            if next_record < overflow_record and next_record.pointer == (-1, -1):
                print(f"DUPA PREVIOUS PAGE: {next_page}")
                next_record.pointer = overflow_pointer
                next_page = self.update_page(next_page, next_record_offset, next_record, replace=True)
                print(f"DUPA NEW PAGE: {next_page}")

                self.overflow.write_page(next_record_page_index, next_page)
            else:
                print(f"PREVIOUS PAGE: {previous_page}")
                previous_record.pointer = overflow_pointer
                previous_page = self.update_page(previous_page, previous_offset, previous_record, replace=True)
                print(f"NEW PAGE: {previous_page}")
                if update_record_in_main_area:
                    self.write_page(previous_page_index, previous_page)
                else:
                    self.overflow.write_page(previous_page_index, previous_page)
                overflow_record.pointer = (next_record_page_index, next_record_offset)
        self.overflow.add_record(overflow_record)

    def resolve_previous_record_in_main_area(self, page: bytes, current_page_index: int, record: GradesRecord):
        offset = self.get_offset(page, record) - 1
        if offset == -1:
            current_page_index -= 1
        return current_page_index, offset

    def read_all_records(self):
        i = 0
        while page := self.read_page(i):
            for offset in range(BLOCKING_FACTOR):
                record = self.get_record_at_offset(offset, page)
                if record is not None:
                    print(record, end="")
                    while record.pointer != (-1, -1):
                        overflow_page_index, overflow_offset = record.pointer[0], record.pointer[1]
                        overflow_page = self.overflow.read_page(overflow_page_index)
                        record = self.get_record_at_offset(overflow_offset, overflow_page)
                        print(record, end="")
            i += 1

if __name__ == "__main__":
    database = Database("data/database.dat")
    page = database.read_page(1)
    print(page)
    database.write_page(1, page)
    print(f"Find record 00002: {database.get_record_by_key('00002', 1)}")
    print(f"Number of pages: {database.number_of_pages()}")
