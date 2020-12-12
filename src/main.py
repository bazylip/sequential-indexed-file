import random
import json
from record import GradesRecord
from seq_ind_file import SeqIndFile

CONFIG_PATH = "configs/config.json"
with open(CONFIG_PATH, "r") as json_config:
    CONFIG = json.load(json_config)

MAX_KEY = CONFIG["MAX_KEY"]
N_RANDOM_DATA = CONFIG["N_RANDOM_DATA"]
PRINT_AFTER_EACH_OPERATION = CONFIG["PRINT_AFTER_EACH_OPERATION"]

def generate_random_data():
    for i in range(N_RANDOM_DATA):
        key = str(random.randrange(MAX_KEY)).rjust(len(str(MAX_KEY)), "0")
        record = GradesRecord(key)
        seq_ind_file.add_record(record)
        if PRINT_AFTER_EACH_OPERATION:
            print(f"\nFILE AFTER OPERATION NUMBER {i + 1}")
            seq_ind_file.print_records()

    print("\nFINAL FILE:")
    seq_ind_file.print_records()

def load_data_from_file():
    commands = {"A": seq_ind_file.add_record, "U": seq_ind_file.update_record, "D": seq_ind_file.delete_record}

    with open("data/input.txt") as input_file:
        for i, input_line in enumerate(input_file.readlines()):
            input_line = input_line.rstrip("\n").split(" ")
            command = commands.get(input_line[0])
            arguments = input_line[1].rjust(len(str(MAX_KEY)), "0") if input_line[0] == "D" else GradesRecord(input_line[1].rjust(len(str(MAX_KEY)), "0"), int(input_line[2]), [input_line[i] for i in [3, 4, 5]])
            command(arguments)
            if PRINT_AFTER_EACH_OPERATION:
                print(f"\nFILE AFTER OPERATION NUMBER {i+1}")
                seq_ind_file.print_records()

    print("\nFINAL FILE:")
    seq_ind_file.print_records()

def experiment():
    pass

seq_ind_file = SeqIndFile("data/database.dat", "data/overflow.dat", "data/index_file.dat")
method_function = {1: generate_random_data, 2: load_data_from_file, 3: experiment}

if __name__ == "__main__":
    print("1. Generate random data")
    print("2. Load data from file")
    print("3. Run experiment")
    choice = int(input("Choose data loading method: "))

    method_function.get(choice)()
