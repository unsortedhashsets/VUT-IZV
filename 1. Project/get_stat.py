import download as do
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime
from matplotlib import gridspec
import argparse
import os

regions_colors = {
    "PHA": "xkcd:aqua",
    "STC": "xkcd:azure",
    "JHC": "xkcd:beige",
    "PLK": "xkcd:coral",
    "ULK": "xkcd:gold",
    "HKK": "xkcd:grey",
    "JHM": "xkcd:indigo",
    "MSK": "xkcd:lightblue",
    "OLK": "xkcd:olive",
    "ZLK": "xkcd:teal",
    "VYS": "xkcd:yellowgreen",
    "PAK": "xkcd:lavender",
    "LBK": "xkcd:navy",
    "KVK": "xkcd:sienna",
}

"""
    gasfdhadfhadfhfda
"""
def parse_arguments():
    parser = argparse.ArgumentParser(description = 'Process command line arguments.')
    parser.add_argument('-r',
                        '--regions',
                        nargs = '+',
                        default = None,
                        choices = ["PHA", "STC", "JHC", "PLK", "ULK", "HKK", "JHM", "MSK", "OLK", "ZLK", "VYS", "PAK","LBK","KVK"],
                        help = 'Sequence of regions: PHA JHC ULK etc..')
    parser.add_argument('-l',
                        '--fig_location',
                        default = None,
                        type = dir_path,
                        help = 'Directory to save image')
    parser.add_argument('-n',
                        '--fig_name',
                        default = 'noname.png',
                        help = 'Image name with format: eps, jpeg, jpg, pdf, pgf, png, ps, raw, rgba, svg, svgz, tif, tiff')
    parser.add_argument('-s',
                        '--show_figure',
                        action = "store_true",
                        help = 'Use command to show figure')
    return parser.parse_args()

"""
    gasfdhadfhadfhfda
"""
def dir_path(path):
    if os.path.isdir(path):
        return path
    else:
        try:
            os.makedirs(path)
        except OSError:
            print (f"Creation of the directory {path} failed")
            exit(-1)
        else:
            print (f"Successfully created the directory {path}")

"""
    gasfdhadfhadfhfda
"""
def round1000(x):
    if x < 1000:
        return 1000
    else:
        return x if x % 1000 == 0 else x + 1000 - x % 1000

"""
    gasfdhadfhadfhfda
"""
def plot_stat(data_source,
              fig_location = None,
              fig_name ='noname.png',
              show_figure = False):

    regions = np.unique(data_source[1][0])
    years = np.unique(data_source[1][4].astype('datetime64[Y]'))
    axs=[]
    average_marks = []
    fig = plt.figure(figsize=(1*len(regions) if 1*len(regions) > 4 else 4,10))
    gs1 = gridspec.GridSpec(len(years), 1)
    today = np.datetime64(datetime.now())
    max_value = 0

    for i, year in enumerate(years):
        marks=[]
        for region in regions:
            marks.append(np.count_nonzero(np.logical_and(
                            data_source[1][0] == region,
                            data_source[1][4].astype('datetime64[Y]') == year)))
        
        if  (today.astype('datetime64[Y]') == year or
             today.astype('datetime64[M]') == np.datetime64(f'{year+1}-01')):
            for region in regions:
                average_marks.append(np.count_nonzero(np.logical_and(
                                        data_source[1][0] == region,
                                        data_source[1][4].astype('datetime64[Y]') != year))/(len(years)-1))

        average_marks = [x for _,x in sorted(zip(marks,average_marks), reverse=True)]
        regions = [x for _,x in sorted(zip(marks,regions), reverse=True)]
        colors = [regions_colors[x] for x in regions]
        marks = sorted(marks, reverse=True)
        
        max_value = max(max_value, marks[0])
        
        axs.append(fig.add_subplot(gs1[i]))
        
        axs[-1].set_ylim([0, (max_value+max_value/5)])
        axs[-1].set_yticks(np.arange(0, (max_value+max_value/5), round1000((max_value+max_value/5)/5)))
        axs[-1].bar(regions, marks, color=colors)
        axs[-1].set_xlabel('Regions')
        axs[-1].set_ylabel('Incidents')
        axs[-1].yaxis.grid(True, linestyle='--', which='major', color='grey', alpha=.25)

        rects = axs[-1].patches
        for rect, label in zip(rects, marks):
            height = rect.get_height()
            axs[-1].text(rect.get_x() + rect.get_width() / 2, height + 5, label, ha='center', va='bottom')

        if  (today.astype('datetime64[Y]') == year or
             today.astype('datetime64[M]') == np.datetime64(f'{year+1}-01')):
            axs[-1].bar(regions, average_marks, color=colors, alpha=0.20, edgecolor='r', linewidth=1.5)
            axs[-1].set_title(f'Incidents by regions in year {year} \n with average based on previous years')
        else:
            axs[-1].set_title(f'Incidents by regions in year {year}')

    fig.tight_layout()
    fig.align_labels()
    
    if fig_location:
        try:
            plt.savefig(f'{fig_location}/{fig_name}', bbox_inches='tight')
        except ValueError:
            print(f"ERROR: wrong image dtype, supported: eps, jpeg, jpg, pdf, pgf, png, ps, raw, rgba, svg, svgz, tif, tiff")
            exit(-1)

    if show_figure:
        plt.show()
    
    plt.close(fig)

"""
    gasfdhadfhadfhfda
"""    
if __name__ == "__main__":
    parsed_args = parse_arguments()

    plot_stat(do.DataDownloader().get_list(parsed_args.regions), 
                    parsed_args.fig_location,
                    parsed_args.fig_name,
                    parsed_args.show_figure)