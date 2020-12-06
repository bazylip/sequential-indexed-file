import os
import json
from record import Record

CONFIG_PATH = "configs/config.json"
with open(CONFIG_PATH, "r") as json_config:
    CONFIG = json.load(json_config)

PAGE_SIZE = CONFIG["BLOCKING_FACTOR"] * CONFIG["RECORD_SIZE"]


class Database:
    def __init__(self, path: str):
        self.path = path

    def clear_database(self):
        open(self.path, "w").close()

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
    database.write_page(1)
