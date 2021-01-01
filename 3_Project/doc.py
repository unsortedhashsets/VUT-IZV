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
            rdf = pickle.load(cache)
            rdf['p2a'] = rdf['p2a'].astype('datetime64[M]').copy()
            start_2020 = dt.datetime.strptime('2020-01-01 00:00:00',
                                              '%Y-%m-%d %H:%M:%S')
            rdf = rdf.loc[(rdf['p2a'] >= start_2020)]
            rdf = rdf[rdf['p12'].isin([201, 202, 203, 204,
                                       205, 206, 207, 208, 209])]
        df = pd.DataFrame()
    except:
        raise OSError(f"ERROR: {filename} not found or has the wrong format")

    # Copy columns with new datatype
    try:
        for i in rdf:
            # Copy date
            if i == 'p2a':
                df['date'] = rdf['p2a']
            # Copy regions and id
            elif i in ['p1', 'region']:
                df[i] = rdf[i]
            # Copy strings
            elif rdf[i].dtypes == 'object':
                df[i] = rdf[i].replace(r'^\s*$',
                                       np.NaN,
                                       regex=True).astype("category")
            # Copy ints/floats
            else:
                df[i] = pd.to_numeric(rdf[i],
                                      downcast='signed',
                                      errors='coerce')
        # Verbose condition.
        if verbose:
            print("-----> Start get_dataframe verbose <-----")
            os = round(rdf.memory_usage(deep=True).sum()/1_048_576, 1)
            ns = round(df.memory_usage(deep=True).sum()/1_048_576, 1)
            print(f'orig_size={os} MB')
            print(f'new_size={ns} MB')
            print(df)
            print("-----> Edn   get_dataframe verbose <-----")
        return df
    except:
        raise NotImplementedError(f"ERROR: OoOops something went wrong...")


def make_geo(rdf: pd.DataFrame,
             verbose: bool = False) -> geopandas.GeoDataFrame:
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
    rdf = rdf[['d', 'e', 'p12']]
    rdf = rdf.dropna(subset=['d', 'e'])
    # Create geometry column as point from `d` and `e` values
    gdf = geopandas.GeoDataFrame(rdf,
                                 geometry=geopandas.points_from_xy(rdf['d'],
                                                                   rdf['e']),
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
    make_map
        - Show/Save map with accidents

    Parameters
    ----------
    gdf: geopandas.GeoDataFrame
        Incoming dataframe
    fig_location : str
        Directory and filename to save figure
    show_figure : bool
        True/False parameter to choose possibility to show the figure
    """

    print('\n--------- Prepare Map ---------\n')

    # Prepare figure, ax
    fig = plt.figure(figsize=(16, 8))
    ax = fig.add_subplot()

    # Put coordinates on the subplot
    for var in zip([201, 202, 203, 204, 205, 206, 207, 208, 209],
                   ['blue', 'orange', 'green', 'red', 'purple',
                   'olive', 'brown', 'pink', 'gray']):

        tmp_gdf = gdf.loc[(gdf['p12'] == var[0])]
        # Put coordinates on the subplot
        tmp_gdf.plot(ax=ax,
                     markersize=5,
                     color=f'tab:{var[1]}',
                     alpha=0.5,
                     legend=False)

    # Adjust maximum x/y axis
    ax.set_ylim(6_200_000, 6_640_000)
    ax.set_xlim(1_340_000, 2_110_000)
    # Put the background map
    ctx.add_basemap(ax, source=ctx.providers.OpenStreetMap.Mapnik)
    # Turn off axis
    ax.axis("off")
    # Add figure settings
    fig.tight_layout()

    # Save figure
    if fig_location:
        try:
            plt.savefig(fig_location, bbox_inches='tight')
            print(f'Map saved - {fig_location}')
        except ValueError:
            raise ValueError("""ERROR: wrong image dtype, supported:
    eps, jpeg, jpg, pdf, pgf, png, ps, raw, rgba, svg, svgz, tif, tiff""")
    else:
        print('Map was not saved')
    # Show figure
    if show_figure:
        plt.show()
    plt.close()

    print('\n--------- Map Done ---------\n')


def make_table(df: pd.DataFrame):
    """
    make_table
        - Stdout table with accidents

    Parameters
    ----------
    df: pd.DataFrame
        Incoming dataframe
    """
    print('\n--------- Prepare Table ---------\n')
    df = df[['date', 'p12', 'p13a', 'p13b', 'p13c']]
    df = df.groupby(['p12'], as_index=False).agg({
                                    'p13a': 'sum',
                                    'p13b': 'sum',
                                    'p13c': 'sum'})
    df.columns = ['Reason', 'Deaths', 'Severely injured', 'Slightly injured']
    df.insert(1, "Marker", ['blue', 'orange', 'green', 'red', 'purple',
                            'olive', 'brown', 'pink', 'gray'], True)
    for i in df:
        if i == 'Reason':
            df[i] = df[i].astype(str)
            df[i].replace({'201': 'non-adaptation to traffic intensity',
                           '202': 'non-adaptation to visibility',
                           '203': 'non-adaptation to vehicle characteristics',
                           '204': 'non-adaptation to road traffic condition',
                           '205': 'non-adaptation to road condition',
                           '206': 'speeding (rules)',
                           '207': 'speeding (road sign)',
                           '208': 'non-adaptation to crosswind',
                           '209': 'another kind of speeding'
                           }, inplace=True)
        elif i in ['Deaths', 'Severely injured', 'Slightly injured']:
            df[i] = df[i].astype(int)
    print(df.to_string(index=False))
    print('\n--------- Table Done ---------\n')


def make_plot(df: pd.DataFrame,
              fig_location: str = None,
              show_figure: bool = False):
    """
    make_plot
        - Show/Save plot with accidents

    Parameters
    ----------
    gdf: geopandas.GeoDataFrame
        Incoming dataframe
    fig_location : str
        Directory and filename to save figure
    show_figure : bool
        True/False parameter to choose possibility to show the figure
    """
    print('\n--------- Prepare Plot ---------\n')
    # Select needed columns
    df = df[['date', 'p12']]
    # Create crosstab
    df = pd.crosstab(index=[df['date']], columns=df['p12'])
    # Rename p12 columns
    df = df.rename(columns={
                    201: 'non-adaptation to traffic intensity',
                    202: 'non-adaptation to visibility',
                    203: 'non-adaptation to vehicle and load characteristics',
                    204: 'non-adaptation to road traffic condition',
                    205: 'non-adaptation to road condition',
                    206: 'speeding (rules)',
                    207: 'speeding (road sign)',
                    208: 'non-adaptation to crosswind',
                    209: 'another kind of speeding'
                            }
                   )
    # Stack to get stacked view
    df = df.stack().rename_axis(index={'p12': 'variable'}
                                ).rename('Number').reset_index()

    # Set sns style
    sns.set_style("darkgrid")
    # Create grid for subplots
    p = sns.FacetGrid(df,
                      height=3,
                      aspect=3)
    # Put subplots on the grid
    p.map(sns.lineplot, 'date', 'Number', 'variable', palette="deep")
    # Make global settings for subplots
    p.set_titles('{col_name}')
    p.set(xlabel='Accidents month', ylabel='Accidents number')
    p.axes.flat[0].xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))

    # Save figure
    if fig_location:
        try:
            plt.savefig(fig_location, bbox_inches='tight')
            print(f'Plot saved - {fig_location}')
        except ValueError:
            raise ValueError("""ERROR: wrong image dtype, supported:
    eps, jpeg, jpg, pdf, pgf, png, ps, raw, rgba, svg, svgz, tif, tiff""")
    else:
        print('Plot was not saved')
    # Show figure
    if show_figure:
        plt.show()
    plt.close()
    print(df)
    print('\n--------- Plot Done ---------\n')


def make_counts(df: pd.DataFrame):
    """
    make_counts
        - Stdout counts with accidents

    Parameters
    ----------
    df: pd.DataFrame
        Incoming dataframe
    """
    print('\n--------- Prepare Counts ---------\n')
    tac = len(df.index)
    df = df[['p13a', 'p13b', 'p13c', 'p53']]
    rs = df.sum(axis=0, skipna=True)
    rc = df.astype(bool).sum(axis=0)

    print("Total:")
    print(tac, '\n')

    print("Sum:")
    print(rs, '\n')

    print("Count:")
    print(rc, '\n')

    awd = int(round(rc[0]/tac, 2)*100)
    awsEi = int(round(rc[1]/tac, 2)*100)
    awsLi = int(round(rc[2]/tac, 2)*100)

    print(f'{awd}% accidents with death/s')
    print(f'{awsEi}% accidents with severe injury/ies')
    print(f'{awsLi}% accidents with slight injury/ies\n')

    print(f'''each {int(round(100/(awsEi+awd),0))}\'
                    th accident ends with death case or sever injury''')
    print(f'''each {int(round(100/(awsLi),0))}\'
                    rd accident ends with slight injury\n''')

    print(f'Total damage to vehicles: {round(rs[3]/10_000, 2)} mln. CZK')
    print('\n--------- Counts Done ---------\n')

if __name__ == "__main__":
    df = make_dataframe("accidents.pkl.gz", verbose=False)
    gdf = make_geo(df, verbose=False)
    make_map(gdf, "map.png", False)
    make_table(df)
    make_plot(df, "fig.png", False)
    make_counts(df)
