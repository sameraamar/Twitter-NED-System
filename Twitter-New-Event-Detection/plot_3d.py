import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D

# from http://matplotlib.org/examples/mplot3d/
# from matplotlib.ticker import LinearLocator, FormatStrFormatter
from matplotlib import cm

# from mpl_toolkits.mplot3d import Axes3D
# from http://matplotlib.org/examples/mplot3d/
import matplotlib.pyplot as plt
import numpy as np


def plot_trisuf3d(mX, mY, mZ, title, xlabel, ylabel, zlabel):
    x = np.asarray(mX)
    y = np.asarray(mY)
    z = np.asarray(mZ)

    fig = plt.figure()
    # ax = fig.gca(projection='3d')
    ax = Axes3D(fig)
    ax.set_title(title)
    xmin = np.min(mX)
    ymin = np.min(mY)
    xmax = np.max(mX)
    ymax = np.max(mY)

    #ax.set_xlim(xmin, xmax)
    #ax.set_ylim(ymin, ymax)
    #ax.set_zlim(0, min(2.0 * max(mZ), 1.0))

    surf = ax.plot_trisurf(x, y, z, cmap=cm.jet, linewidth=0.2)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.set_zlabel(zlabel)
    fig.colorbar(surf, shrink=0.5, aspect=5)

    plt.show()
    plt.close()

if __name__ == '__main__':

    file = open('C:\\Users\\t-saaama\\Dropbox\\Samer\\LSHApproach\\measures.csv')
    proc = []
    tables = []
    seconds = []

    firstline = True
    for line in file:
        if firstline:
            firstline = False
            continue
        vals = line.split(',')
        proc.append(int(vals[4]))
        tables.append(int(vals[5]))
        seconds.append(float(vals[1]))

    print(proc)
    print(tables)
    print(seconds)

    plot_trisuf3d(proc, tables, seconds, 'Performance', 'Processes', 'Tables', 'Seconds')
