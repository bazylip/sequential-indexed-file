import bisect

class Index:
    def __init__(self, key: str, page_number: int):
        self.key = key
        self.page_number = page_number

    def __str__(self):
        return self.key + " " + str(self.page_number) + "\n"


class IndexFile:
    def __init__(self, path: str):
        self.path = path

    def clear_index_file(self):
        open(self.path, "w").close()

    def add_index(self, key: str):
        index = Index(key, page_number)
        with open(self.path, "a") as index_file:
            index_file.write(str(index))

    def get_previous_key(self, key: str):
        with open(self.path, "r") as index_file:
            indexes = index_file.readlines()
            indexes.sort()
            indexes = [s.split(" ")[0] for s in indexes]
            return indexes[bisect.bisect(indexes, key) - 2]

    def get_page(self, key: str):
        with open(self.path, "r") as index_file:
            indexes = index_file.readlines()
            return [s for s in indexes if key in s][0].rstrip("\n")[-1]


if __name__ == "__main__":
    index_file = IndexFile("data/index_file.dat")
    print(index_file.get_page("00005"))
    print(index_file.get_previous_key("00005"))
