import typing
import random
import json

CONFIG_PATH = "configs/config.json"
with open(CONFIG_PATH, "r") as json_config:
    CONFIG = json.load(json_config)

AVAILABLE_GRADES = ["2.0", "3.0", "3.5", "4.0", "4.5", "5.0"]
ID_BOUND = (100000, 999999)


class GradesRecord:
    def __init__(self, key: str, pointer: (int, int) = (0, 0), id: int = None, grades: typing.List[str] = None, deleted: bool = False):
        self.key = key
        self.pointer = pointer
        self.deleted = deleted
        if grades is not None:
            for grade in grades:
                assert grade in AVAILABLE_GRADES, f"Provided grade {grade} is not valid"
            self.id = id
            self.grades = grades
        else:
            self.id = str(random.randint(ID_BOUND[0], ID_BOUND[1]))
            self.grades = [random.choice(AVAILABLE_GRADES) for _ in range(3)]

    def __str__(self):
        return self.key + " " + str(self.pointer[0]) + ":" + str(self.pointer[1]) + " D:" + str(1 if self.deleted else 0) + " " + str(self.id) + " " + " ".join(self.grades) + "\n"

    def to_bytes(self):
        return str(self).encode()

    def __len__(self):
        return len(self.to_bytes())

if __name__ == "__main__":
    open("data/database.dat", "w").close()
    for i in range(6):
        student = GradesRecord("0000"+str(i), (0, 1), 183710, ["4.0", "4.5", "5.0"])
        print(student.to_bytes())
        print(len(student.to_bytes()))
        with open("data/database.dat", "ab") as database:
            database.write(student.to_bytes())
