import bisect
import json
import math
from record import GradesRecord

CONFIG_PATH = "configs/config.json"
with open(CONFIG_PATH, "r") as json_config:
    CONFIG = json.load(json_config)

MAX_KEY = CONFIG["MAX_KEY"]
INITIAL_NO_OF_PAGES = CONFIG["INITIAL_NO_OF_PAGES"]


class Index:
    """Single index - key and page number"""
    def __init__(self, key: str, page_number: int):
        self.key = key
        self.page_number = page_number

    def __str__(self):
        return self.key + " " + str(self.page_number) + "\n"

    def __lt__(self, other):
        return self.key < other.key


class IndexFile:
    """File with indexes"""
    def __init__(self, path: str):
        self.path = path
        self.entries = []
        self.clear_index_file()

    def clear_index_file(self) -> None:
        """
        Remove contents of index file
        :return: None
        """
        open(self.path, "w").close()

    def dump_to_file(self) -> None:
        """
        Save current indexed to file
        :return: None
        """
        with open(self.path, "w") as index_file:
            for entry in self.entries:
                index_file.write(str(entry))

    def initialize_indexes(self) -> None:
        """
        Initialize indexes at file creation
        :return: None
        """
        for page_no, key in enumerate(range(0, MAX_KEY, math.ceil(MAX_KEY/INITIAL_NO_OF_PAGES))):
            new_index = Index(str(key).rjust(len(str(MAX_KEY)), "0"), page_no)
            self.entries.append(new_index)

    def add_index(self, key: str, page_index: int) -> None:
        """
        Add new index to file
        :param key: Key of record
        :param page_index: Page of record
        :return: None
        """
        index = Index(key, page_index)
        self.entries.append(index)

    def get_page_of_key(self, key: str) -> int:
        """
        Get page number of record with matching key
        :param key: Key of record to get page of
        :return: Page of record
        """
        assert self.entries, "There are no entries in index file"

        keys = [entry.key for entry in self.entries]
        previous_index = bisect.bisect_left(keys, key) - 1
        return self.entries[previous_index].page_number

    def get_page_to_insert(self, record: GradesRecord) -> int:
        """
        Get page number of last smallest key
        :param record: Record to get page number for
        :return: Page number of last smallest key or 0 if there are no entries yet
        """
        if not self.entries:
            return 0

        sorted_entries = sorted(self.entries, key=lambda entry: entry.key)
        previous_index = bisect.bisect_left(sorted_entries, record)-1
        return sorted_entries[previous_index].page_number
