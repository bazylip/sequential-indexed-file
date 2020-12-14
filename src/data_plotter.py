import matplotlib.pyplot as plt
import numpy as np
from matplotlib import cm

RESULTS_PATH = "data/experiment/experiment_results_delete_1000.txt"

with open(RESULTS_PATH, "r") as results, open("data/experiment/results.txt", "w") as output:
    for line in results.readlines():
        line_list = line.split(" ")
        output.write(line_list[1][:-1] + " " + line_list[5][:-1] + " " + line_list[8][:-1] + " " + line_list[10])

data = []
with open("data/experiment/results.txt", "r") as results:
    for line in results.readlines():
        line = line.rstrip("\n").split(" ")
        data.append([float(line[0]), int(line[1]), float(line[2]), float(line[3])])

alphas = np.arange(0.1, 1, 0.1)
max_pages = np.arange(1, 10, 1)
max_pages, alphas = np.meshgrid(max_pages, alphas)
disk_operations = [probe[2] for probe in data]
time = [probe[3] for probe in data]
disk_operations = np.array(disk_operations).reshape(len(alphas), len(max_pages))
time = np.array(time).reshape(len(alphas), len(max_pages))

fig = plt.figure()
ax = fig.gca(projection='3d')
surface = ax.plot_surface(max_pages, alphas, time, cmap=cm.coolwarm,
                          linewidth=0, antialiased=False)
ax.set_xlabel('Max overflow pages')
ax.set_ylabel('Alpha')
ax.set_zlabel('Time')
plt.savefig("data/plots/plot_time_delete_1000.png")

fig = plt.figure()
ax = fig.gca(projection='3d')
surface = ax.plot_surface(max_pages, alphas, disk_operations, cmap=cm.coolwarm,
                          linewidth=0, antialiased=False)
ax.view_init(30, 60)
ax.set_xlabel('Max overflow pages')
ax.set_ylabel('Alpha')
ax.set_zlabel('Disk operations')

plt.savefig("data/plots/plot_disk_delete_1000.png")
