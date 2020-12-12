import typing
import random
import json

CONFIG_PATH = "configs/config.json"
with open(CONFIG_PATH, "r") as json_config:
    CONFIG = json.load(json_config)

AVAILABLE_GRADES = ["2.0", "3.0", "3.5", "4.0", "4.5", "5.0"]
ID_BOUND = (100000, 999999)


class GradesRecord:
    def __init__(self, key: str, id_: int = None, grades: typing.List[str] = None, pointer: typing.Tuple[int, int] = (-1, -1), deleted: bool = False):
        self.key = key
        self.pointer = pointer
        self.deleted: bool = deleted
        if grades is not None:
            for grade in grades:
                assert grade in AVAILABLE_GRADES, f"Provided grade {grade} is not valid"
            self.id = id_
            self.grades = grades
        else:
            self.id = str(random.randint(ID_BOUND[0], ID_BOUND[1]))
            self.grades = [random.choice(AVAILABLE_GRADES) for _ in range(3)]

    def add_overflow(self, pointer: typing.Tuple[int, int]):
        self.pointer = pointer

    def __str__(self):
        pointer_str = "x:x " if self.pointer == (-1, -1) else str(self.pointer[0]) + ":" + str(self.pointer[1]) + " "
        return self.key + " " + \
               pointer_str + \
               str(self.id) + " " + \
               " ".join(self.grades) + \
               " D:" + str(1 if self.deleted else 0) + "\n"

    def to_bytes(self):
        return str(self).encode()

    def __len__(self):
        return len(self.to_bytes())

    def __lt__(self, other):
        return self.key < other.key


if __name__ == "__main__":
    open("data/database.dat", "w").close()
    for i in range(6):
        student = GradesRecord("1")
        print(student.to_bytes())
        print(len(student.to_bytes()))
        with open("data/database.dat", "ab") as database:
            database.write(student.to_bytes())
