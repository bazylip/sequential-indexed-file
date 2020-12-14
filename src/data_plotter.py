import matplotlib.pyplot as plt
import numpy as np
from matplotlib import cm

for experiment in ["10000", "1000", "delete_1000", "update_1000"]:

    RESULTS_PATH = f"data/experiment/experiment_results_{experiment}.txt"

    with open(RESULTS_PATH, "r") as results, open("data/experiment/results.txt", "w") as output:
        for line in results.readlines():
            line_list = line.split(" ")
            output.write(line_list[1][:-1] + " " + line_list[5][:-1] + " " + line_list[8][:-1] + " " + line_list[10])

    data = []
    with open("data/experiment/results.txt", "r") as results:
        for line in results.readlines():
            line = line.rstrip("\n").split(" ")
            data.append([float(line[0]), int(line[1]), float(line[2]), float(line[3])])

    alphas = np.arange(0.1, 0.8, 0.1)
    max_pages = np.arange(1, 10, 1)

    disk_operations = [probe[2] for probe in data]
    time = [probe[3] for probe in data]
    X, Y = np.meshgrid(max_pages, alphas)
    disk_operations = np.array(disk_operations).reshape(len(alphas), len(max_pages))
    time = np.array(time).reshape(len(alphas), len(max_pages))
    fig = plt.figure()
    ax = fig.gca(projection='3d')
    surface = ax.plot_surface(X, Y, time, cmap=cm.coolwarm,
                              linewidth=0, antialiased=False)
    ax.set_xlabel('Max overflow pages')
    ax.set_ylabel('Alpha')
    ax.set_zlabel('Time')
    plt.savefig(f"data/plots/plot_time_{experiment}.png")

    fig = plt.figure()
    ax = fig.gca(projection='3d')
    surface = ax.plot_surface(X, Y, disk_operations, cmap=cm.coolwarm,
                              linewidth=0, antialiased=False)
    ax.set_xlabel('Max overflow pages')
    ax.set_ylabel('Alpha')
    ax.set_zlabel('Disk operations')

    plt.savefig(f"data/plots/plot_disk_{experiment}.png")
