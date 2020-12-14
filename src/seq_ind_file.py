import json
import math
import copy
import os
from index_file import IndexFile
from database import Database
from record import GradesRecord

CONFIG_PATH = "configs/config.json"
with open(CONFIG_PATH, "r") as json_config:
    CONFIG = json.load(json_config)

BLOCKING_FACTOR = CONFIG["BLOCKING_FACTOR"]
INITIAL_NO_OF_PAGES = CONFIG["INITIAL_NO_OF_PAGES"]
MAX_KEY = CONFIG["MAX_KEY"]
RECORD_SIZE = CONFIG["RECORD_SIZE"]
PRINT_DEBUG = CONFIG["PRINT_DEBUG"]
MAX_OVERFLOW_PAGE_NO = CONFIG["MAX_OVERFLOW_PAGE_NO"]
ALPHA = CONFIG["ALPHA"]


class SeqIndFile:
    """Sequential-indexed file"""
    def __init__(self, database_path: str, overflow_path: str, index_file_path: str):
        global MAX_OVERFLOW_PAGE_NO
        global ALPHA
        with open(CONFIG_PATH, "r") as json_config:
            CONFIG = json.load(json_config)
        MAX_OVERFLOW_PAGE_NO = CONFIG["MAX_OVERFLOW_PAGE_NO"]
        ALPHA = CONFIG["ALPHA"]
        self.database = Database(database_path, overflow_path)
        self.index_file = IndexFile(index_file_path)
        self.index_file.initialize_indexes()

    def add_record(self, record: GradesRecord) -> None:
        """
        Add new record to file, reorganize afterwards if needed
        :param record: Record to be added
        :return: None
        """
        page_number = self.index_file.get_page_to_insert(record)
        reorganize = self.database.add_record(record, page_number)
        if reorganize:
            self.reorganize()

    def get_record(self, key: str) -> GradesRecord:
        """
        Get record with matching key
        :param key: Key of record to be returned
        :return: Record with matching key
        """
        page_number = self.index_file.get_page_of_key(key)
        page = self.database.read_page(page_number)
        return self.database.get_record_by_key(key, page)

    def delete_record(self, key: str) -> None:
        """
        Delete record with matching key
        :param key: Key of record to be deleted
        :return: None
        """
        page_number = self.index_file.get_page_of_key(key)
        self.database.delete_record(key, page_number)

    def update_record(self, new_record: GradesRecord) -> None:
        """
        Update record with key matching to new one
        :param new_record: New record to replace the old one with matching key
        :return: None
        """
        page_number = self.index_file.get_page_of_key(new_record.key)
        self.database.update_record(new_record, page_number)

    def reorganize(self) -> None:
        """
        Reorganize file
        :return: None
        """
        old_paths = [self.database.path, self.database.overflow.path, self.index_file.path]
        new_paths = [old_path.split(".")[0] + "_reorg." + old_path.split(".")[1] for old_path in old_paths]

        new_number_of_pages = math.ceil(self.database.number_of_records / (BLOCKING_FACTOR * ALPHA))
        new_database, new_index_file = Database(new_paths[0], new_paths[1]), IndexFile(new_paths[2])
        new_database.initialize_empty_pages(new_number_of_pages)

        page = b""
        current_page_index = 0
        num = 0
        for record, _, _, _, _ in self.database.get_all_records():
            if not new_index_file.entries:
                new_index_file.add_index(record.key, 0)
            if record.deleted:
                continue
            if new_database.page_size(page) >= RECORD_SIZE * BLOCKING_FACTOR * ALPHA:
                new_database.write_page(current_page_index, page)
                current_page_index += 1
                page = b""
                new_index_file.add_index(record.key, current_page_index)
            new_record = copy.deepcopy(record)
            new_record.add_overflow((-1, -1))
            page += new_record.to_bytes()
            num += 1
        new_database.write_page(current_page_index, page)

        for old_path in old_paths:
            os.remove(old_path)
        for i, new_path in enumerate(new_paths):
            os.rename(new_path, old_paths[i])
        new_database.path = old_paths[0]
        new_database.overflow.path = old_paths[1]
        new_index_file.path = old_paths[2]
        new_database.dummy_record = self.database.dummy_record
        new_database.dummy_record_key = self.database.dummy_record_key
        new_database.disk_operations = self.database.disk_operations
        self.database = new_database
        self.index_file = new_index_file
        if PRINT_DEBUG: print("REORGANIZED!")
        new_index_file.dump_to_file()

    def print_records(self, only_existing: bool = True) -> None:
        """
        Print all record from file
        :param only_existing: Print only non-deleted records
        :return: None
        """
        self.database.print_all_records(0, only_existing)
