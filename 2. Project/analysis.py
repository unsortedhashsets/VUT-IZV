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
# muzete pridat libovolnou zakladni knihovnu ci knihovnu predstavenou na prednaskach
# dalsi knihovny pak na dotaz

# Ukol 1: nacteni dat
def get_dataframe(filename: str, verbose: bool = False) -> pd.DataFrame:
    with gzip.open(filename) as cache:
        raw_df = pickle.load(cache)
    df = pd.DataFrame()
    for i in raw_df:
        if i == 'p2a':
            df['date'] = pd.to_datetime(raw_df[i], errors='coerce')
        elif i == 'region':
            df[i] = raw_df[i].replace(r'^\s*$', np.NaN, regex=True)
        elif i in ['p1','h','i','k','l','n','o','p','q','r','s','t']:
            df[i] = raw_df[i].replace(r'^\s*$', np.NaN, regex=True).astype("category")
        else:
            df[i] = pd.to_numeric(raw_df[i], downcast='signed', errors='coerce')
    if verbose:
        print(f'orig_size={round(raw_df.memory_usage(index=True, deep=True).sum()/1_048_576,1)} MB')
        print(f'new_size={round(df.memory_usage(index=True, deep=True).sum()/1_048_576,1)} MB')
    return df

# Ukol 2: následky nehod v jednotlivých regionech
def plot_conseq(df: pd.DataFrame, fig_location: str = None,
                show_figure: bool = False):
    df = df.rename(columns={'region': 'Regions'})
    # groupping values
    df = df.groupby(['Regions'], as_index=False).agg({
                                    'p13a' : 'sum',
                                    'p13b' : 'sum',
                                    'p13c' : 'sum',
                                    'p1'   : 'count'})
    # melting values
    df = pd.melt(df, id_vars    = 'Regions',
                     var_name   = 'variable',
                     value_name = 'Number',
                     value_vars = ['p13a','p13b','p13c','p1'])
    # fix order
    order = df.loc[df['variable'] == 'p1'].sort_values(['Number'], ascending=False)['Regions']
    # set style
    sns.set_style("darkgrid")
    # create grid
    p = sns.FacetGrid(df,
                      row="variable",
                      sharex=False,
                      sharey=False,
                      height=3.5,
                      aspect=3)
    # set plots on grid
    p.map(sns.barplot,'Regions','Number', order = order, palette="deep")
    # setup plots
    for ax, title in zip(p.axes.flat, [ 'Number of people who died in the accident (p13a)',
                                        'Number of people who were severely injured (p13b)',
                                        'Number of people who were slightly injured (p13c)',
                                        'The total number of accidents in the region'
                                      ]):
        ax.set_title(title)
        if title != 'The total number of accidents in the region':
            ax.xaxis.set_visible(False)
        height = 0
        for p in ax.patches:
                    height = max(height, p.get_height())
                    ax.set_ylim([0, height+height/8])
                    ax.annotate( f'{int(p.get_height())}',
                                    xy=(p.get_x() + p.get_width() / 2, p.get_height()),
                                    xytext=(0, 3),
                                    textcoords="offset points",
                                    ha='center', va='bottom')
    # Save figure
    if fig_location:
        try:
            plt.savefig(fig_location, bbox_inches='tight')
        except ValueError:
            raise ValueError(f"ERROR: wrong image dtype, supported: eps, jpeg, jpg, pdf, pgf, png, ps, raw, rgba, svg, svgz, tif, tiff")
    # Show figure
    if show_figure:
        plt.show()
    
    plt.close()

# Ukol3: příčina nehody a škoda
def plot_damage(df: pd.DataFrame, fig_location: str = None,
                show_figure: bool = False):
    # Select needed regions and columns
    df = df[['region','p12','p53']]
    df = df.loc[df['region'].isin(['JHM','HKK','PLK','PHA'])]

    # Prepare p12
    p12 = ['Not caused by the driver','Speeding','Incorrect overtaking',
             'Not giving priority in driving','Wrong way of driving','Technical defect of the vehicle']
    bins = [(99, 100), (200, 209), (300, 311), (400, 414), (500, 516), (600, 616)]
    index = pd.IntervalIndex.from_tuples(bins)
    df['p12'] = pd.CategoricalIndex(pd.cut(df['p12'], index)).rename_categories({interval: name for interval, name in zip(index.values, p12)})

    # Prepare p53b
    p53 = ['<50','50-199','200-499','500-1000','>1000']
    bins = [(float('-inf'), 49.99), (49.99, 199.99), (199.99, 499.99), (499.99, 1000), (1000, float('inf'))]
    index = pd.IntervalIndex.from_tuples(bins)
    df['p53b'] = pd.CategoricalIndex(pd.cut(df['p53'], index)).rename_categories({interval: name for interval, name in zip(index.values, p53)})
    
    # group objects
    df = df.groupby(['region','p53b','p12']).agg({'p53' : 'count'}).reset_index()

    sns.set_style("darkgrid")
    p = sns.FacetGrid(df,
                      col="region",
                      col_wrap=2,
                      sharex=False,
                      sharey=False,
                      height=7,
                      aspect=0.75)
    # set plots on grid
    p.map(sns.barplot, 'p53b', 'p53', 'p12', order=p53, hue_order=p12, palette="deep")

    for ax in p.axes.flat:
        ax.set_yscale('log')
        ax.xaxis.set_visible(True)
        ax.yaxis.set_visible(True)
        ax.set_yticks([1.e+00,1.e+01,1.e+02,1.e+03,1.e+04,1.e+05])
        ax.set_ylim((0.5,(1.e+05)-1))

    p.add_legend(title='Accident reason')
    p.set_titles('{col_name}')
    p.set(xlabel = 'Damage [thousand Kc]', ylabel = 'Number')
    plt.subplots_adjust(hspace=.15, wspace=.15)   
    

    # Save figure
    if fig_location:
        try:
            plt.savefig(fig_location, bbox_inches='tight')
        except ValueError:
            raise ValueError(f"ERROR: wrong image dtype, supported: eps, jpeg, jpg, pdf, pgf, png, ps, raw, rgba, svg, svgz, tif, tiff")
    # Show figure
    if show_figure:
        plt.show()
    
    plt.close()


# Ukol 4: povrch vozovky
def plot_surface(df: pd.DataFrame, fig_location: str = None,
                 show_figure: bool = False):
    df = df[['region','p16','date']]
    df = df.loc[df['region'].isin(['JHM','HKK','PLK','PHA'])]
    df['date']=df['date'].astype('datetime64[M]').copy()

    df = pd.crosstab(index=[df['region'],df['date']], columns=df['p16'])
    df = df.rename(columns={0: 'other state',
                            1: 'dry surface - unpolluted',
                            2: 'dry surface - polluted',
                            3: 'wet surface',
                            4: 'mud on the road',
                            5: 'icing on the road, snow passed - sprinkled',
                            6: 'icing on the road, snow passed - not sprinkled',
                            7: 'spilled oil, diesel, etc. on the road',
                            8: 'continuous snow layer, slush',
                            9: 'sudden change in road condition',
                           }).reset_index()
    df = pd.melt(df, id_vars    = ['region','date'],
                     var_name   = 'variable',
                     value_name = 'Number',
                     value_vars = ['other state',
                                   'dry surface - unpolluted',
                                   'dry surface - polluted',
                                   'wet surface',
                                   'mud on the road',
                                   'icing on the road, snow passed - sprinkled',
                                   'icing on the road, snow passed - not sprinkled',
                                   'spilled oil, diesel, etc. on the road',
                                   'continuous snow layer, slush',
                                   'sudden change in road condition'])

    sns.set_style("darkgrid")
    p = sns.FacetGrid(df,
                      col="region",
                      col_wrap=2,
                      sharex=True,
                      sharey=False,
                      height=3,
                      aspect=2)
    p.map(sns.lineplot, 'date', 'Number', 'variable', palette="deep")

    p.add_legend(title='Road condition')
    p.set_titles('{col_name}')
    p.set(xlabel = 'Accidents date', ylabel = 'Accidents number')
    p.axes.flat[0].set_xticks(list(p.axes.flat[0].get_xticks())+[18628.])
    p.axes.flat[0].set_xlim(16714.25, 18650.75)
    p.axes.flat[0].xaxis.set_major_formatter(mdates.DateFormatter('%Y'))

    for i,ax in enumerate(p.axes.flat):
        if i%2 != 0:
            ax.set_ylabel("")

    #plt.subplots_adjust(hspace=.15, wspace=.15)   

    # Save figure
    if fig_location:
        try:
            plt.savefig(fig_location, bbox_inches='tight')
        except ValueError:
            raise ValueError(f"ERROR: wrong image dtype, supported: eps, jpeg, jpg, pdf, pgf, png, ps, raw, rgba, svg, svgz, tif, tiff")
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
    plot_conseq(df, fig_location="01_nasledky.png", show_figure=True)
    plot_damage(df, "02_priciny.png", True)
    plot_surface(df, "03_stav.png", True)
