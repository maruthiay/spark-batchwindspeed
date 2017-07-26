import numpy as np
import matplotlib.pyplot as plt



def get_xyz_from_csv_file_np(csv_file_path): 
    x, y, z = np.loadtxt(csv_file_path, delimiter=', ', dtype=np.int).T
    plt_z = np.zeros((y.max()+1, x.max()+1))
    plt_z[y, x] = z

    return plt_z


def draw_heatmap(plt_z, j):
    # Generate y and x values from the dimension lengths
    plt_y = np.arange(plt_z.shape[0])
    plt_x = np.arange(plt_z.shape[1])
    z_min = -50
    z_max = 50 

    plot_name = "demo"

    color_map = plt.cm.gist_heat 
    fig, ax = plt.subplots()
    cax = ax.pcolor(plt_x, plt_y, plt_z, cmap=color_map, vmin=z_min, vmax=z_max)
    ax.set_xlim(plt_x.min(), plt_x.max())
    ax.set_ylim(plt_y.min(), plt_y.max())
    fig.colorbar(cax).set_label(plot_name, rotation=270) 
    ax.set_title(plot_name)  
    ax.set_aspect('equal')
    #return figure
    figure = plt.gcf()
    fname = "out_%i.png" %j
    plt.savefig(fname)

for k in range(0,50):
    fname = "out_%i.txt" %k
    res = get_xyz_from_csv_file_np(fname)
    draw_heatmap(res, k)
