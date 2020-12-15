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
ALPHA = CONFIG["ALPHA"]
PRINT_DEBUG = CONFIG["PRINT_DEBUG"]
PRINT_VERBOSE_RECORDS = CONFIG["PRINT_VERBOSE_RECORDS"]
PRINT_DISK_OPERATIONS = CONFIG["PRINT_DISK_OPERATIONS"]
PAGE_SIZE = RECORD_SIZE * BLOCKING_FACTOR
PADDING_SYMBOL = b"\0" if CONFIG["PADDING_SYMBOL"] == "null" else CONFIG["PADDING_SYMBOL"].encode()


class Overflow:
    """Overflow area of the file"""
    def __init__(self, path: str):
        self.path = path
        self.current_page_index = 0
        self.current_offset = 0
        self.current_disk_operations = 0
        self.clear_overflow()

    def clear_overflow(self) -> None:
        """
        Remove contents in overflow path
        :return: None
        """
        open(self.path, "w").close()

    def read_page(self, page_index: int) -> bytes:
        """
        Read page at index from overflow
        :param page_index: Index to read page from
        :return: Page in bytes
        """
        self.current_disk_operations += 1
        with open(self.path, "br") as overflow:
            overflow.seek(page_index * PAGE_SIZE, os.SEEK_SET)
            return overflow.read(PAGE_SIZE)

    def write_page(self, page_index: int, page: bytes) -> None:
        """
        Read page at index to overflow
        :param page_index: Index to write page at
        :param page: Page to be written
        :return: None
        """
        self.current_disk_operations += 1
        with open(self.path, "br+") as overflow:
            overflow.seek(page_index * PAGE_SIZE, os.SEEK_SET)
            overflow.write(page)

    def get_new_pointer(self) -> typing.Tuple[int, int]:
        """
        Get current pointer of overflow
        :return: Page and offset of current pointer
        """
        return self.current_page_index, self.current_offset

    def add_record(self, record: GradesRecord) -> typing.Tuple[int, int]:
        """
        Add record to overflow and return its pointer
        :param record: Record to add
        :return: Pointer of newly added record
        """
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
    """Area of the file where records are stored (main area + overflow)"""
    def __init__(self, database_path: str, overflow_path: str):
        global MAX_OVERFLOW_PAGE_NO
        global ALPHA
        with open(CONFIG_PATH, "r") as json_config:
            CONFIG = json.load(json_config)
        MAX_OVERFLOW_PAGE_NO = CONFIG["MAX_OVERFLOW_PAGE_NO"]
        ALPHA = CONFIG["ALPHA"]
        self.path = database_path
        self.overflow = Overflow(overflow_path)
        self.number_of_records = 0
        self.current_disk_operations = 0
        self.disk_operations = 0
        self.dummy_record_key = None
        self.dummy_record = None
        self.clear_database()
        self.initialize_empty_pages()
        self.add_dummy_record()
        self.dummy_record = True

    def clear_database(self) -> None:
        """
        Remove contents in database path
        :return: None
        """
        open(self.path, "w").close()

    def initialize_empty_pages(self, number_of_pages: int = INITIAL_NO_OF_PAGES) -> None:
        """
        Initialize main area with empty pages
        :param number_of_pages: Number of empty pages
        :return: None
        """
        for i in range(number_of_pages):
            empty_page = PADDING_SYMBOL*PAGE_SIZE
            self.write_page(i, empty_page)

    def add_dummy_record(self) -> None:
        """
        Add dummy record with first possible key to beginning of database
        :return: None
        """
        self.dummy_record_key = "".rjust(len(str(MAX_KEY)), "0")
        dummy_record = GradesRecord(self.dummy_record_key)
        print(f"adding dummy record: {dummy_record}")
        self.add_record(dummy_record, 0)

    def generate_record_from_string_list(self, record: typing.List[str]) -> GradesRecord:
        """
        Generate new record from provided list of strings
        :param record: List of strings
        :return: New record created from the list
        """
        key = record[0]
        pointer = (-1, -1) if (record[1].split(":")[0], record[1].split(":")[1]) == ("x", "x") else (int(record[1].split(":")[0]), int(record[1].split(":")[1]))
        id = int(record[2])
        grades = [record[i] for i in [3, 4, 5]]
        deleted = bool(int(record[6].split(":")[1]))

        return GradesRecord(key, id, grades, pointer, deleted)

    def get_record_by_key(self, key: str, page: bytes) -> typing.Optional[GradesRecord]:
        """
        Retrieve record from the page by key
        :param key: Key of record
        :param page: Page where the record will be searched for
        :return: Record if present, None otherwise
        """
        page = page.decode().split("\n")
        found_records = [s for s in page if s.startswith(key)]

        assert len(found_records) <= 1, "Multiple records with same key found!"
        if len(found_records) == 0:
            return None

        record = found_records[0].split(" ")
        return self.generate_record_from_string_list(record)

    def get_record_at_offset(self, offset: int, page: bytes) -> typing.Optional[GradesRecord]:
        """
        Retrieve record from the page at offset
        :param offset: Offset in the page
        :param page: Page where the record will be searched for
        :return: Record at offset
        """
        page = page.replace(PADDING_SYMBOL, b"")
        page = page.decode().split("\n")
        page = [record for record in page if record]
        if not page or len(page) <= offset:
            return None
        record = page[offset].split(" ")
        return self.generate_record_from_string_list(record)

    def add_record(self, record: GradesRecord, page_index: int) -> bool:
        """
        Add new record to page at index
        :param record: New record to be added
        :param page_index: Page number where the record should be inserted to
        :return: True if file should be reorganized, False otherwise
        """
        page = self.read_page(page_index)
        self.number_of_records += 1
        self.current_disk_operations = 0
        self.overflow.current_disk_operations = 0

        if self.dummy_record is not None and record.key == self.dummy_record_key:
            self.dummy_record = False
            print(f"updating dummy record: {record}, index: {page_index}")
            self.update_record(record, 0)
            return False

        if self.page_size(page + record.to_bytes()) > PAGE_SIZE:
            record_str = str(record).rstrip("\n")
            self.set_overflow(page, page_index, record)
            if PRINT_DEBUG: print(f"OVERFLOW: {record_str}")
            if PRINT_DISK_OPERATIONS: print(f"DISK OPERATIONS: {self.current_disk_operations + self.overflow.current_disk_operations}")
            self.disk_operations += self.overflow.current_disk_operations
            return self.overflow.current_page_index > MAX_OVERFLOW_PAGE_NO

        offset = self.get_offset(page, record)
        page = self.update_page(page, offset, record)
        record_str = str(record).rstrip("\n")
        self.write_page(page_index, page)
        if PRINT_DEBUG: print(f"ADDING RECORD: {record_str} to page {page_index} at offset {offset}")
        if PRINT_DISK_OPERATIONS: print(f"DISK OPERATIONS: {self.current_disk_operations + self.overflow.current_disk_operations}")
        return self.overflow.current_page_index > MAX_OVERFLOW_PAGE_NO

    def update_record(self, new_record: GradesRecord, page_number: int) -> None:
        """
        Update record at page index, records are compared by key
        :param new_record: New record with same key as old record
        :param page_number: Page number where the record will be searched for
        :return: None
        """
        self.current_disk_operations = 0
        self.overflow.current_disk_operations = 0

        for record, record_page, record_page_number, offset, in_overflow in self.get_all_records(page_number):
            if record.key == new_record.key:
                record_str = str(record).rstrip("\n")
                if PRINT_DEBUG: print(f"UPDATING RECORD: {record_str}, {'in overflow ' if in_overflow else 'in main area'} in page {record_page_number} at offset {offset}")
                record.id = new_record.id
                record.grades = new_record.grades
                record_page = self.update_page(record_page, offset, record, replace=True)
                accessor = self.overflow if in_overflow else self
                accessor.write_page(record_page_number, record_page)
                break

        if PRINT_DISK_OPERATIONS: print(f"DISK OPERATIONS: {self.current_disk_operations + self.overflow.current_disk_operations}")
        self.disk_operations += self.overflow.current_disk_operations

    def delete_record(self, key: str, page_number: int) -> None:
        """
        Delete record with key at page index
        :param key: Key of record to be deleted
        :param page_number: Page number where the record will be searched for
        :return: None
        """
        self.current_disk_operations = 0
        self.overflow.current_disk_operations = 0

        for record, record_page, record_page_number, offset, in_overflow in self.get_all_records(page_number):
            if record.key == key:
                record_str = str(record).rstrip("\n")
                if PRINT_DEBUG: print(f"DELETING RECORD: {record_str}, {'in overflow ' if in_overflow else 'in main area'} in page {record_page_number} at offset {offset}")
                record.deleted = True
                record_page = self.update_page(record_page, offset, record, replace=True)
                accessor = self.overflow if in_overflow else self
                accessor.write_page(record_page_number, record_page)
                break

        if PRINT_DISK_OPERATIONS: print(f"DISK OPERATIONS: {self.current_disk_operations + self.overflow.current_disk_operations}")
        self.disk_operations += self.overflow.current_disk_operations

    def read_page(self, page_index: int) -> bytes:
        """
        Read page at index
        :param page_index: Index of page from which it will be read
        :return: Page in bytes
        """
        self.current_disk_operations += 1
        self.disk_operations += 1
        with open(self.path, "rb") as database:
            database.seek(page_index * PAGE_SIZE, os.SEEK_SET)
            return database.read(PAGE_SIZE)

    def read_all_pages(self) -> None:
        """
        Print contents of all pages
        :return: None
        """
        i = 0
        while page := self.read_page(i):
            print(f"PAGE {i}:")
            print(page.decode())
            i += 1

    def write_page(self, page_index: int, page: bytes) -> None:
        """
        Write new page at index or replace old when if it exists
        :param page_index: Index of page where it will be written
        :param page: New page to be written
        :return: None
        """
        self.current_disk_operations += 1
        self.disk_operations += 1
        with open(self.path, "rb+") as database:
            database.seek(page_index * PAGE_SIZE, os.SEEK_SET)
            database.write(page)

    def update_page(self, page: bytes, offset: int, record: GradesRecord, replace: bool = False) -> bytes:
        """
        Update a page at offset
        :param page: Page to be updated
        :param offset: Offset at which to update the page
        :param record: Record to write in page at offset
        :param replace: True if existing record should be replaced, False if it should be appended
        :return: Updated page
        """
        if not replace:
            page = page[:offset * RECORD_SIZE] + record.to_bytes() + page[offset * RECORD_SIZE:]
            return page[:PAGE_SIZE]
        else:
            page = page[:offset * RECORD_SIZE] + record.to_bytes() + page[offset * RECORD_SIZE + RECORD_SIZE:]
            return page

    def page_size(self, page: bytes) -> int:
        """
        Return size of a page
        :param page: Page to get size of
        :return: Size of page
        """
        return len(page.replace(PADDING_SYMBOL, b""))

    def get_offset(self, page: bytes, record: GradesRecord) -> int:
        """
        Get offset of record in page
        :param page: Page where the record is present
        :param record: Record to get offset of
        :return: Offset of record in page
        """
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
        """
        Get offset of last existing record in page
        :param page: Page to get last offset
        :return: Offset of last existing record
        """
        page = page.replace(PADDING_SYMBOL, b"")
        if len(set(page)) <= 1:  # page is empty
            return 0
        page_keys = [k.split(b" ")[0] for k in page.split(b"\n")][:-1]
        page_keys = [k for k in page_keys if k]  # remove empty strings
        return len(page_keys)-1

    def number_of_pages(self) -> int:
        """
        Get number of pages in whole file
        :return: Number of pages in main area + overflow
        """
        accessors, pages = [self, self.overflow], 0
        for accessor in accessors:
            i = 0
            while accessor.read_page(i):
                i += 1
            pages += i
        return pages

    def set_overflow(self, page: bytes, current_page_index: int, overflow_record: GradesRecord) -> None:
        """
        Set overflow of record which is supposed to be added to a page
        :param page: Page of main area where the file should be added to
        :param current_page_index: Index of page
        :param overflow_record: Record to be added to file
        :return: None
        """
        overflow_pointer = self.overflow.get_new_pointer()
        record_to_update_page_index, record_to_update_offset = self.resolve_previous_record_in_main_area(page, current_page_index, overflow_record)
        if record_to_update_page_index != current_page_index:
            page = self.read_page(record_to_update_page_index)
            record_to_update_offset = self.get_last_offset_of_page(page)

        while record_to_update := self.get_record_at_offset(record_to_update_offset, page) is None:
            record_to_update_page_index -= 1
            page = self.read_page(record_to_update_page_index)
            record_to_update_offset = self.get_last_offset_of_page(page)

        record_to_update = self.get_record_at_offset(record_to_update_offset, page)

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

            while overflow_record > next_record and next_record.pointer != (-1, -1):  # go through the list
                update_record_in_main_area = False
                previous_record = next_record
                previous_page = next_page
                previous_page_index, previous_offset = next_record_page_index, next_record_offset


                next_record_page_index = next_record.pointer[0]
                next_record_offset = next_record.pointer[1]
                if next_record_page_index != previous_page_index:
                    next_page = self.overflow.read_page(next_record_page_index)
                next_record = self.get_record_at_offset(next_record_offset, next_page)

            if next_record < overflow_record and next_record.pointer == (-1, -1):  # if end of list is still lower than new record
                next_record.pointer = overflow_pointer
                next_page = self.update_page(next_page, next_record_offset, next_record, replace=True)

                self.overflow.write_page(next_record_page_index, next_page)
            else:
                previous_record.pointer = overflow_pointer
                previous_page = self.update_page(previous_page, previous_offset, previous_record, replace=True)
                if update_record_in_main_area:
                    self.write_page(previous_page_index, previous_page)
                else:
                    self.overflow.write_page(previous_page_index, previous_page)
                overflow_record.pointer = (next_record_page_index, next_record_offset)

        self.overflow.add_record(overflow_record)

    def resolve_previous_record_in_main_area(self, page: bytes, current_page_index: int, record: GradesRecord) -> typing.Tuple[int, int]:
        """
        Get page index and offset of preceding record in main area
        :param page: Page where the record is stored
        :param current_page_index: Index of page
        :param record: Record to be resolved
        :return: Page index and offset of preceding record in main area
        """
        offset = self.get_offset(page, record) - 1
        if offset == -1:
            current_page_index -= 1
        return current_page_index, offset

    def get_all_records(self, starting_page_num: int = 0) -> typing.Generator:
        """
        Yield all records in file
        :param starting_page_num: Page to start
        :return: Record, number of page, current offset, is in overflow
        """
        while page := self.read_page(starting_page_num):
            for offset in range(BLOCKING_FACTOR):
                record = self.get_record_at_offset(offset, page)
                if record is not None:
                    yield record, page, starting_page_num, offset, False
                    overflow_page_index, overflow_page = None, None
                    while record.pointer != (-1, -1):
                        previous_page_index = overflow_page_index
                        overflow_page_index, overflow_offset = record.pointer[0], record.pointer[1]  # TODO: fix this not to read page each time
                        if previous_page_index != overflow_page_index:
                            overflow_page = self.overflow.read_page(overflow_page_index)
                        record = self.get_record_at_offset(overflow_offset, overflow_page)
                        yield record, overflow_page, overflow_page_index, overflow_offset, True
            starting_page_num += 1

    def print_all_records(self, starting_page_num: int = 0, only_existing: bool = True) -> None:
        """
        Print all records in file
        :param starting_page_num: Page from which to start printing
        :param only_existing: Only print non-deleted records
        :return: None
        """
        for record, page, page_number, offset, is_overflow in self.get_all_records(starting_page_num):
            if only_existing and record.deleted:
                continue
            if self.dummy_record and record.key == self.dummy_record_key:
                continue
            if PRINT_VERBOSE_RECORDS:
                print(f"page: {page_number}, offset: {offset}, overflow: {is_overflow}".ljust(40) +  f"{record}", end="")
            else:
                print(record, end="")
