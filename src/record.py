import typing
import random
import json

CONFIG_PATH = "configs/config.json"
with open(CONFIG_PATH, "r") as json_config:
    CONFIG = json.load(json_config)

AVAILABLE_GRADES = ["2.0", "3.0", "3.5", "4.0", "4.5", "5.0"]
ID_BOUND = (100000, 999999)


class GradesRecord:
    def __init__(self, key: str, pointer: (int, int), id: int = None, grades: typing.List[str] = None, deleted: bool = False):
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

    def grades_average(self) -> float:
        """
        Calculate average of grades in the record

        :return: Average of grades
        """
        return np.mean([float(grade) for grade in self.grades])

    def __str__(self):
        return self.key + " " + str(self.pointer[0]) + ":" + str(self.pointer[1]) + " " + str(self.deleted) + " " + str(self.id) + " " + " ".join(self.grades)

    def to_bytes(self):
        return str.encode(str(self))

    def __len__(self):
        return len(self.to_bytes())


student = GradesRecord("00001", (0, 1), 183710, ["4.0", "4.5", "5.0"])
print(student.to_bytes())
print(type(student.to_bytes()))
