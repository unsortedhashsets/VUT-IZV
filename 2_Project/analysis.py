#!/usr/bin/env python3.8
# coding=utf-8

from matplotlib import pyplot as plt
from matplotlib import dates as mdates
import pandas as pd
import seaborn as sns
import numpy as np
import os
import gzip
import pickle


def get_dataframe(filename: str, verbose: bool = False) -> pd.DataFrame:
    """
    get_dataframe
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
            os = round(raw_df.memory_usage(deep=True).sum()/1_048_576, 1)
            ns = round(df.memory_usage(deep=True).sum()/1_048_576, 1)
            print(f'orig_size={os} MB')
            print(f'new_size={ns} MB')
        return df
    except:
        raise NotImplementedError(f"ERROR: OoOops something went wrong...")


def plot_conseq(df: pd.DataFrame, fig_location: str = None,
                show_figure: bool = False):
    """
    plot_conseq
        - Prepare appropriate dataframe with functions:
            pd.melt, groupby, agg(sum/count).
        - Show/Save bar blot for each parameter:
            p13a, p13b, p13c, total accidents by regions.

    Parameters
    ----------
    df : pd.DataFrame
        Incoming dataframe
    fig_location : str
        Directory and filename to save figure
    show_figure : bool
        True/False parameter to choose possibility to show the figure
    """

    # Select needed columns
    df = df[['p1', 'region', 'p13a', 'p13b', 'p13c']]
    # Detect some -1 value in p13a, p13b and p13c and replace to 0
    df = df.replace({'p13a': -1, 'p13b': -1, 'p13c': -1}, 0)
    # Rename column region for future needs
    df = df.rename(columns={'region': 'Regions'})
    # Group future variables value and aggregate it in the needed way
    df = df.groupby(['Regions'], as_index=False).agg({
                                    'p13a': 'sum',
                                    'p13b': 'sum',
                                    'p13c': 'sum',
                                    'p1': 'count'})
    # Melt dataframe to see variable and value in better view form
    df = pd.melt(df,
                 id_vars='Regions',
                 var_name='variable',
                 value_name='Number',
                 value_vars=['p13a', 'p13b', 'p13c', 'p1'])
    # Get right region order
    order = df.loc[df['variable'] == 'p1'
                   ].sort_values(['Number'], ascending=False)['Regions']

    # Set sns style
    sns.set_style("darkgrid")
    # Create grid for subplots
    p = sns.FacetGrid(df,
                      row="variable",
                      sharex=False,
                      sharey=False,
                      height=3.5,
                      aspect=3)
    # Put subplots on the grid
    p.map(sns.barplot, 'Regions', 'Number', order=order, palette="deep")
    # Make individual settings for subplots
    for ax, title in zip(p.axes.flat,
                         ['Number of people who died in the accident (p13a)',
                          'Number of people who were severely injured (p13b)',
                          'Number of people who were slightly injured (p13c)',
                          'The total number of accidents in the region']
                         ):
        # Set suplots title
        ax.set_title(title)
        if title != 'The total number of accidents in the region':
            ax.xaxis.set_visible(False)
        # Set maximum Y value and print value on the top of each bar
        height = 0
        for p in ax.patches:
                height = max(height, p.get_height())
                ax.set_ylim([0, height+height/8])
                ax.annotate(f'{int(p.get_height())}',
                            xy=(p.get_x() + p.get_width() / 2, p.get_height()),
                            xytext=(0, 3),
                            textcoords="offset points",
                            ha='center',
                            va='bottom')

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


def plot_damage(df: pd.DataFrame, fig_location: str = None,
                show_figure: bool = False):
    """
    plot_damage
        - Prepare appropriate dataframe with functions:
                pd.cut, groupby, agg(sum/count).
        - Show the number of accidents depending on damage to vehicles (p53)
          stated in thousands CZK, what will be divided into several classes.

    Parameters
    ----------
    df : pd.DataFrame
        Incoming dataframe
    fig_location : str
        Directory and filename to save figure
    show_figure : bool
        True/False parameter to choose possibility to show the figure
    """

    # Select needed regions and columns
    df = df[['region', 'p12', 'p53']]
    df = df.loc[df['region'].isin(['JHM', 'HKK', 'PLK', 'PHA'])]
    # Prepare p12 bins
    p12 = ['Not caused by the driver',
           'Speeding',
           'Incorrect overtaking',
           'Not giving priority in driving',
           'Wrong way of driving',
           'Technical defect of the vehicle']
    bins = [(99, 100),
            (200, 209),
            (300, 311),
            (400, 414),
            (500, 516),
            (600, 616)]
    index = pd.IntervalIndex.from_tuples(bins)
    df['p12'] = pd.CategoricalIndex(pd.cut(df['p12'], index)
                                    ).rename_categories(
                                     {interval: name for interval,
                                      name in zip(index.values, p12)})
    # Prepare p53b bins
    p53 = ['<50',
           '50-199',
           '200-499',
           '500-1000',
           '>1000']
    # Convert data from hundreds to thousends 500h -> 50th 
    bins = [(-1, 499.99),
            (499.99, 1999.99),
            (1999.99, 4999.99),
            (4999.99, 10000),
            (10000, float('inf'))]
    index = pd.IntervalIndex.from_tuples(bins)
    df['p53b'] = pd.CategoricalIndex(pd.cut(df['p53'], index)
                                     ).rename_categories(
                                      {interval: name for interval,
                                       name in zip(index.values, p53)})
    # Group by objects to get better view
    df = df.groupby(['region', 'p53b', 'p12']
                    ).agg({'p53': 'count'}).reset_index()

    # Set sns style
    sns.set_style("darkgrid")
    # Create grid for subplots
    p = sns.FacetGrid(df,
                      col="region",
                      col_wrap=2,
                      sharex=False,
                      sharey=False,
                      height=7,
                      aspect=0.75)
    # Put subplots on the grid
    p.map(sns.barplot,
          'p53b',
          'p53',
          'p12',
          order=p53,
          hue_order=p12,
          palette="deep")
    # Make individual settings for subplots
    for ax in p.axes.flat:
        ax.set_yscale('log')
        ax.xaxis.set_visible(True)
        ax.yaxis.set_visible(True)
        ax.set_yticks([1.e+00, 1.e+01, 1.e+02, 1.e+03, 1.e+04, 1.e+05])
        ax.set_ylim((0.5, (1.e+05)-1))
    # Make global settings for subplots
    p.add_legend(title='Accident reason')
    p.set_titles('{col_name}')
    p.set(xlabel='Damage [thousand Kc]', ylabel='Number')
    plt.subplots_adjust(hspace=.15, wspace=.15)

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


def plot_surface(df: pd.DataFrame, fig_location: str = None,
                 show_figure: bool = False):
    """
    plot_surface
        - Prepare appropriate dataframe with ( Variant 2) functions:
            pd.crosstab, pd.rename, pd.stack.
        - Show/Save a line graph that will show for each month
          (X axis - date column) the number of accidents
          at different conditions of the road surface (P16).

    Parameters
    ----------
    df : pd.DataFrame
        Incoming dataframe
    fig_location : str
        Directory and filename to save figure
    show_figure : bool
        True/False parameter to choose possibility to show the figure
    """

    # Select needed regions and columns
    df = df[['region', 'p16', 'date']]
    df = df.loc[df['region'].isin(['JHM', 'HKK', 'PLK', 'PHA'])]
    df['date'] = df['date'].astype('datetime64[M]').copy()
    # Detect some -1 value in p16 replace to 0
    df = df.replace({'p16': -1}, 0)
    # Create crosstab
    df = pd.crosstab(index=[df['region'], df['date']], columns=df['p16'])
    # Rename p16 columns
    df = df.rename(columns={
                    0: 'other state',
                    1: 'dry surface - unpolluted',
                    2: 'dry surface - polluted',
                    3: 'wet surface',
                    4: 'mud on the road',
                    5: 'icing on the road, snow passed - sprinkled',
                    6: 'icing on the road, snow passed - not sprinkled',
                    7: 'spilled oil, diesel, etc. on the road',
                    8: 'continuous snow layer, slush',
                    9: 'sudden change in road condition'
                            }
                   )
    # Stack to get stacked view
    df = df.stack().rename_axis(index={'p16': 'variable'}
                                ).rename('Number').reset_index()

    # Set sns style
    sns.set_style("darkgrid")
    # Create grid for subplots
    p = sns.FacetGrid(df,
                      col="region",
                      col_wrap=2,
                      sharex=True,
                      sharey=False,
                      height=3,
                      aspect=2)
    # Put subplots on the grid
    p.map(sns.lineplot, 'date', 'Number', 'variable', palette="deep")
    # Make global settings for subplots
    p.add_legend(title='Road condition')
    p.set_titles('{col_name}')
    p.set(xlabel='Accidents date', ylabel='Accidents number')
    p.axes.flat[0].set_xticks(list(p.axes.flat[0].get_xticks())+[18628.])
    p.axes.flat[0].set_xlim(16714.25, 18650.75)
    p.axes.flat[0].xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
    # Make individual settings for subplots
    for i, ax in enumerate(p.axes.flat):
        if i % 2 != 0:
            ax.set_ylabel("")

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
    pass
    # zde je ukazka pouziti, tuto cast muzete modifikovat podle libosti
    # skript nebude pri testovani pousten primo, ale budou volany konkreni ¨
    # funkce.
    df = get_dataframe("accidents.pkl.gz", verbose=True)
    plot_conseq(df, fig_location="01_nasledky.png", show_figure=False)
    plot_damage(df, "02_priciny.png", False)
    plot_surface(df, "03_stav.png", False)
