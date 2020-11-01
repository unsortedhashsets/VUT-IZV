"""
| Project Implementation for IZV 2020/2021
| Script get_stat.py
| Date: 30.10.2020
| Author: Mikhail Abramov
| xabram00@stud.fit.vutbr.cz
"""

import os
import numpy
import argparse
import matplotlib.pyplot as plt

from datetime import datetime
from matplotlib import gridspec
from download import DataDownloader

"""
Colors variables dictionary: {region:color}
"""
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


def parse_arguments():
    """
    Init argparse object and add arguments

    Returns
    -------
    parser.parse_args()
        - options, an object containing values for all of your options
        - args, the list of positional arguments leftover after parsing options
    """

    parser = argparse.ArgumentParser(description = 'Process command line arguments.')
    parser.add_argument('-r',
                        '--regions',
                        nargs = '+',
                        default = None,
                        choices = ["PHA", "STC", "JHC", "PLK", "ULK", "HKK", "JHM",
                                   "MSK", "OLK", "ZLK", "VYS", "PAK", "LBK", "KVK"],
                        help = 'Sequence of regions: PHA JHC ULK etc..')
    parser.add_argument('-l',
                        '--fig_location',
                        default = None,
                        type = dir_path,
                        help = 'Directory to save image')
    parser.add_argument('-s',
                        '--show_figure',
                        default = False,
                        action = "store_true",
                        help = 'Use command to show figure')
    parser.add_argument('-f',
                        '--folder',
                        default = "data",
                        help = 'Folder to download and store data')
    parser.add_argument('-u',
                        '--url',
                        default = "https://ehw.fit.vutbr.cz/izv/",
                        help = 'URL to download data')
    parser.add_argument('-c',
                        '--cache_filename',
                        default = "data_{}.pkl.gz",
                        help = 'Cache files name')

    return parser.parse_args()


def dir_path(path):
    """
    Validate path argument value and create it

    Parameters
    ----------
    path : str
        A path to store generated figure

    Returns
    -------
    path : str
        A path to store generated figure
    """
    
    try:
        head_tail = os.path.split(path)
        if os.path.isdir(head_tail[0]):
            return path
        else:
            os.makedirs(head_tail[0])
    except OSError:
        print (f"Creation of the directory {head_tail[0]} failed")
        exit(-1)
    else:
        print (f"Successfully created the directory {head_tail[0]}")
        return path
            

def round1000(x):
    """
    Round X on 1000 

    Parameters
    ----------
    x : int
        Some int value to be rounded

    Returns
    -------
    x : int
        Rounded int or if it < 1000 -> 1000
    """

    if x < 1000:
        return 1000
    else:
        return x if x % 1000 == 0 else x + 1000 - x % 1000


def plot_stat(data_source,
              fig_location = None,
              show_figure = False):
    """
    Create plot with statistis of accidents on the roads by year and region

    Parameters
    ----------
    data_source : tuple(list[str], list[np.ndarray])
        Object containing processed statistics
    fig_location : str
        If “fig_location” is set, the image will be saved in the given address.
        If the folder where the image is to be saved does not exist, creates it.
        The default value is 'None'.
    show_figure : Boolean
        If the parameter is 'True', the graph will be displayed in the window
        The default value is 'False'.
    """
    # Define number of regions and years
    regions = numpy.unique(data_source[1][0])
    years = numpy.unique(data_source[1][4].astype('datetime64[Y]'))

    # Init fig. object with grid spec. based on region and years 
    fig = plt.figure(figsize=(1*len(regions) if 1*len(regions) > 4 else 4,
                              2*len(years)))
    gs1 = gridspec.GridSpec(len(years), 1)

    # Additional help values
    today = numpy.datetime64(datetime.now())
    axs=[]
    max_value = 0
    average_accidents = []

    # Creates N number of graphs for N provided years
    for i, year in enumerate(years):
        accidents=[]

        # Fill accidents list with values by regions
        for region in regions:
            accidents.append(numpy.count_nonzero(numpy.logical_and(
                            data_source[1][0] == region,
                            data_source[1][4].astype('datetime64[Y]') == year)))

        # Fill average_accidents list with values by regions only for last year
        if  (today.astype('datetime64[Y]') == year or
             today.astype('datetime64[M]') == numpy.datetime64(f'{year+1}-01')):
            for region in regions:
                average_accidents.append(numpy.count_nonzero(numpy.logical_and(
                                        data_source[1][0] == region,
                                        data_source[1][4].astype('datetime64[Y]') != year))/(len(years)-1))

        # Sort all values
        average_accidents = [x for _,x in sorted(zip(accidents,average_accidents), reverse=True)]
        regions = [x for _,x in sorted(zip(accidents,regions), reverse=True)]
        colors = [regions_colors[x] for x in regions]
        accidents = sorted(accidents, reverse=True)
        
        # Get max value for all plot frames
        max_value = max(max_value, accidents[0])
        
        # Add subplot object
        axs.append(fig.add_subplot(gs1[i]))
        
        # Actual subplot settings
        axs[-1].set_ylim([0, (max_value+max_value/5)])
        axs[-1].set_yticks(numpy.arange(0, (max_value+max_value/5), round1000((max_value+max_value/5)/5)))
        axs[-1].bar(regions, accidents, color=colors)
        axs[-1].set_xlabel('Regions')
        axs[-1].set_ylabel('Accidents')
        axs[-1].yaxis.grid(True, linestyle='--', which='major', color='grey', alpha=.25)
        rects = axs[-1].patches
        for rect, label in zip(rects, accidents):
            height = rect.get_height()
            axs[-1].text(rect.get_x() + rect.get_width() / 2, height + 5, label, ha='center', va='bottom')
        if  (today.astype('datetime64[Y]') == year or
             today.astype('datetime64[M]') == numpy.datetime64(f'{year+1}-01')):
            axs[-1].bar(regions, average_accidents, color=colors, alpha=0.20, edgecolor='r', linewidth=1.5)
            axs[-1].set_title(f'Accidents by regions in year {year} \n with average based on previous years')
        else:
            axs[-1].set_title(f'Accidents by regions in year {year}')

    # Additional figure settings
    fig.tight_layout()
    fig.align_labels()
    
    # Save figure
    if fig_location:
        try:
            plt.savefig(f'{fig_location}', bbox_inches='tight')
            print (f"Successfully saved the figure {fig_location}")
        except ValueError:
            print(f"ERROR: wrong image dtype, supported: eps, jpeg, jpg, pdf, pgf, png, ps, raw, rgba, svg, svgz, tif, tiff")
            exit(-1)

    # Show figure
    if show_figure:
        plt.show()
    
    plt.close(fig)

if __name__ == "__main__":
    """
    Main
    """
    
    parsed_args = parse_arguments()

    plot_stat(DataDownloader(parsed_args.url,
                             parsed_args.folder,
                             parsed_args.cache_filename, 
                            ).get_list(parsed_args.regions), 
              parsed_args.fig_location,
              parsed_args.show_figure)