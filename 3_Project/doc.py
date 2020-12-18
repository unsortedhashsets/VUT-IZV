#!/usr/bin/env python3.8
# coding=utf-8

"""
| Project Implementation for IZV 2020/2021
| Script doc.py
| Date: 13.12.2020
| Author: Mikhail Abramov
| xabram00@stud.fit.vutbr.cz
"""

from matplotlib import pyplot as plt
from matplotlib import dates as mdates
import contextily as ctx
import geopandas
import pandas as pd
import seaborn as sns
import numpy as np
import os
import gzip
import pickle
import datetime as dt

def make_dataframe(filename: str, verbose: bool = False) -> pd.DataFrame:
    """
    make_dataframe
        - Reading the incoming dataframe from the pickle.gz file.
        - Rename 'p2p' column to date and change datatype to datetime64[ns].
        - Copy region column.
        - Replace empty strings to np.NaN and save as category datatype
          for columns ('p1','h','i','k','l','n','o','p','q','r','s','t').
        - All other columns convert to datatype - integer/float.

    Parameters
    ----------
    filename : str
        Directory and filename of dataframe in pickle.gz format
    verbose : bool
        Verbose parameter to print out information about dataframes size

    Returns
    -------
    df : pd.DataFrame
        new dataframe, lightweight and ready for further processing
    """

    # Reading the incoming dataframe from the pickle.gz file
    try:
        with gzip.open(filename) as cache:
            raw_df = pickle.load(cache)
        df = pd.DataFrame()
    except:
        raise OSError(f"ERROR: {filename} not found or has the wrong format")

    # Copy columns with new datatype
    try:
        for i in raw_df:
            # Copy date
            if i == 'p2a':
                df['date'] = pd.to_datetime(raw_df[i], errors='coerce')
            # Copy regions and id
            elif i in ['p1', 'region']:
                df[i] = raw_df[i]
            # Copy strings
            elif raw_df[i].dtypes == 'object':
                df[i] = raw_df[i].replace(r'^\s*$', np.NaN, regex=True
                                          ).astype("category")
            # Copy ints/floats
            else:
                df[i] = pd.to_numeric(raw_df[i],
                                      downcast='signed',
                                      errors='coerce')
        # Verbose condition.
        if verbose:
            print("-----> Start get_dataframe verbose <-----")
            os = round(raw_df.memory_usage(deep=True).sum()/1_048_576, 1)
            ns = round(df.memory_usage(deep=True).sum()/1_048_576, 1)
            print(f'orig_size={os} MB')
            print(f'new_size={ns} MB')
            print(df)
            print("-----> Edn   get_dataframe verbose <-----")
        return df
    except:
        raise NotImplementedError(f"ERROR: OoOops something went wrong...")

def make_geo(df: pd.DataFrame, verbose: bool = False) -> geopandas.GeoDataFrame:
    """
    make_geo
        - Delete rows with NaN values from columns `d` and `e`.
        - Create geometry column as point from `d` and `e` values.
        - Convert coordinates points in WGS 84 (3857) from S-JTSK (5514)
        - Rewrite new GPS coordinates to columns `d` and `e`.

    Parameters
    ----------
    df : pd.DataFrame
        Incoming dataframe
    verbose : bool
        Verbose parameter to print out information about gdf

    Returns
    -------
    gdf : geopandas.GeoDataFrame
        New dataframe with new format and prepared coordinates column
    """

    # Delete rows with NaN values from columns `d` and `e`
    df = df.dropna(subset=['d', 'e'])
    # Create geometry column as point from `d` and `e` values
    gdf = geopandas.GeoDataFrame(df,
                                 geometry=geopandas.points_from_xy(df['d'],
                                                                   df['e']),
                                 crs="EPSG:5514")
    # Convert coordinates points in WGS 84 (3857) from S-JTSK (5514)
    gdf = gdf.to_crs(epsg=3857)
    # Verbose condition.
    if verbose:
        print("-----> Start make_geo verbose <-----")
        print(gdf)
        print("-----> End   make_geo verbose <-----")
    return gdf


def make_map(gdf: geopandas.GeoDataFrame,
                 fig_location: str = None,
                 show_figure: bool = False):
    """
    plot_cluster
        - Prepare appropriate dataframe
        - Show/Save map with accident's clusterization and info bar

    Parameters
    ----------
    gdf: geopandas.GeoDataFrame
        Incoming dataframe
    fig_location : str
        Directory and filename to save figure
    show_figure : bool
        True/False parameter to choose possibility to show the figure
    """

    # Select needed columns and rows
    start_2020 = dt.datetime.strptime('2020-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
    gdf = gdf.loc[(df['date'] >= start_2020)]
    gdf = gdf.loc[(df['p12'] >= 201) & (df['p12'] <= 209)]
    gdf = gdf[['geometry','p12','date']]
    print(gdf)
    # Prepare figure, ax
    fig = plt.figure(figsize=(16, 8))
    ax = fig.add_subplot()

    for var in zip([201,202,203,204,205,206,207,208,209],
                 ['blue','orange','green','red','purple',
                  'purple','brown','pink','gray','olive']):

        tmp_gdf = gdf.loc[(gdf['p12'] == var[0])]
        # Put coordinates on the subplot
        tmp_gdf.plot(ax=ax,
                     markersize=1,
                     color=f'tab:{var[1]}',
                     alpha=0.5,
                     legend=False)

    legend_list = ['nepřizpůsobení rychlosti intenzitě (hustotě) provozu',
                   'nepřizpůsobení rychlosti viditelnosti',
                   'nepřizpůsobení rychlosti vlastnostem vozidla a nákladu',
                   'nepřizpůsobení rychlosti stavu vozovky',
                   'nepřizpůsobení rychlosti dopravně technickému stavu vozovky',
                   'překročení předepsané rychlosti stanovené pravidly',
                   'překročení rychlosti stanovené dopravní značkou',
                   'nepřizpůsobení rychlosti bočnímu, nárazovému větru',
                   'jiný druh nepřiměřené rychlosti']

    # Add legend    
    ax.legend(legend_list,
              fancybox=True,
              shadow=True,
              loc='center left',
              bbox_to_anchor=(1, 0.5),
              markerscale=6)

    # Adjust maximum x/y axis
    ax.set_ylim(6_200_000, 6_640_000)
    ax.set_xlim(1_340_000, 2_110_000)
    # Put the background map
    ctx.add_basemap(ax, source=ctx.providers.OpenStreetMap.Mapnik)
    # Turn off axis
    ax.axis("off")
    # Add titles
    ax.set_title(f'Accidents in JHM region')
    # Add figure settings
    fig.tight_layout()

    # Save figure
    if fig_location:
        try:
            plt.savefig(fig_location, bbox_inches='tight')
        except ValueError:
            raise ValueError("""ERROR: wrong image dtype, supported:
    eps, jpeg, jpg, pdf, pgf, png, ps, raw, rgba, svg, svgz, tif, tiff""")
    # Show figure
    if show_figure:
        plt.show()
    plt.close()


if __name__ == "__main__":
    df = make_dataframe("accidents.pkl.gz", verbose=False)
    gdf = make_geo(df, verbose=False)
    make_map(gdf, "doc-map.png", True)