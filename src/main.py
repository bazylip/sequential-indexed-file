import random
import json
import itertools
import copy
import time
from record import GradesRecord
from seq_ind_file import SeqIndFile

CONFIG_PATH = "configs/config.json"
with open(CONFIG_PATH, "r") as json_config:
    CONFIG = json.load(json_config)

MAX_KEY = CONFIG["MAX_KEY"]
N_RANDOM_DATA = CONFIG["N_RANDOM_DATA"]
PRINT_AFTER_EACH_OPERATION = CONFIG["PRINT_AFTER_EACH_OPERATION"]
PRINT_DISK_OPERATIONS = CONFIG["PRINT_DISK_OPERATIONS"]


def generate_random_data(print_at_end: bool = True):
    """Initialize sequential-indexed file with random data"""
    seq_ind_file = SeqIndFile("data/database.dat", "data/overflow.dat", "data/index_file.dat")
    for i in range(N_RANDOM_DATA):
        key = str(random.randrange(MAX_KEY)).rjust(len(str(MAX_KEY)), "0")
        record = GradesRecord(key)
        seq_ind_file.add_record(record)

        #with open("data/experiment_data.txt", "a") as experiment_data:
        #    experiment_data.write(f"A {record.key} {record.id} {record.grades[0]} {record.grades[1]} {record.grades[2]}\n")

        if PRINT_AFTER_EACH_OPERATION:
            print(f"\nFILE AFTER OPERATION NUMBER {i + 1}")
            seq_ind_file.print_records()

    if print_at_end:
        print("\nFINAL FILE:")
        seq_ind_file.print_records()

    if PRINT_DISK_OPERATIONS:
        print(f"OVERALL DISK OPERATIONS: {seq_ind_file.database.disk_operations}")

    return seq_ind_file.database.disk_operations


def load_data_from_file(data_source: str = "data/input.txt", print_at_end: bool = True):
    """Load data from file and create sequential-indexed file from it"""
    seq_ind_file = SeqIndFile("data/database.dat", "data/overflow.dat", "data/index_file.dat")
    commands = {"A": seq_ind_file.add_record, "U": seq_ind_file.update_record, "D": seq_ind_file.delete_record}

    with open(data_source) as input_file:
        for i, input_line in enumerate(input_file.readlines()):
            input_line = input_line.rstrip("\n").split(" ")
            command = commands.get(input_line[0])
            arguments = input_line[1].rjust(len(str(MAX_KEY)), "0") if input_line[0] == "D" else GradesRecord(input_line[1].rjust(len(str(MAX_KEY)), "0"), int(input_line[2]), [input_line[i] for i in [3, 4, 5]])
            command(arguments)
            if PRINT_AFTER_EACH_OPERATION:
                print(f"\nFILE AFTER OPERATION NUMBER {i+1}")
                seq_ind_file.print_records()

    if print_at_end:
        print("\nFINAL FILE:")
        seq_ind_file.print_records()

    if PRINT_DISK_OPERATIONS:
        print(f"OVERALL DISK OPERATIONS: {seq_ind_file.database.disk_operations}")

    return seq_ind_file.database.disk_operations


def experiment():
    """Run experiment and save results to file"""
    alphas = [i*0.1 for i in range(1, 10)]
    max_overflow_no_of_pages = [i for i in range(1, 10)]
    results = []

    for alpha, max_overflow_pages in itertools.product(alphas, max_overflow_no_of_pages):
        new_config = copy.deepcopy(CONFIG)
        new_config["ALPHA"] = alpha
        new_config["MAX_OVERFLOW_PAGE_NO"] = max_overflow_pages
        with open(CONFIG_PATH, "w") as json_config:
            json.dump(new_config, json_config)
        start = time.time()
        disk_operations = load_data_from_file(data_source="data/experiment/experiment_data.txt", print_at_end=False)
        end = time.time()
        print(f"ALPHA: {alpha}, MAX OVERFLOW PAGES: {max_overflow_pages}, DISK OP: {disk_operations}, TIME: {end-start}")
        results.append(disk_operations)

    with open(CONFIG_PATH, "w") as json_config:
        json.dump(CONFIG, json_config, indent=4)


method_function = {1: generate_random_data, 2: load_data_from_file, 3: experiment}

if __name__ == "__main__":
    print("1. Generate random data")
    print("2. Load data from file")
    print("3. Run experiment")
    choice = int(input("Choose data loading method: "))

    method_function.get(choice)()
