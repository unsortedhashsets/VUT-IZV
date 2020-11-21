#!/usr/bin/env python3.8
# coding=utf-8

from matplotlib import pyplot as plt
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
            df[i] = pd.to_datetime(raw_df[i], errors='coerce')
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
    dfs = [pd.melt(df, id_vars=['region'], var_name='type', value_name='value', value_vars=[X]).groupby(["region"]).agg({'value': "sum"})
            for X in ['p13a','p13b','p13c']]
    dfs.append(pd.DataFrame(index=dfs[0].index)) 
    dfs[3]['value'] =  dfs[0]['value'] +  dfs[1]['value'] +  dfs[2]['value']

    dfs[3] = dfs[3].sort_values(by=['value'], ascending=False)
    dfs[0] = dfs[0].reindex(dfs[3].index)
    dfs[1] = dfs[1].reindex(dfs[3].index)
    dfs[2] = dfs[2].reindex(dfs[3].index)

    plt.style.use('seaborn')
    fig, axes = plt.subplots(4, 1, figsize=(8, 12))
    ax = axes.flatten()
    colors = ["xkcd:aqua" ,"xkcd:azure","xkcd:beige",
              "xkcd:coral","xkcd:gold", "xkcd:grey",
              "xkcd:indigo","xkcd:lightblue","xkcd:olive",
              "xkcd:teal","xkcd:yellowgreen","xkcd:lavender",
              "xkcd:navy","xkcd:sienna",
    ]
    for j, (i, df) in enumerate(zip(ax,dfs)):
        if j == 0:
            i.set_title('Number of people who died in the accident (p13a)')
            i.xaxis.set_visible(False)
        elif j == 1:
            i.set_title('Number of people who were severely injured (p13b)')
            i.xaxis.set_visible(False)
        elif j == 2:
            i.set_title('Number of people who were slightly injured (p13c)')
            i.xaxis.set_visible(False)
        elif j == 3:
            i.set_title('The total number of accidents in the region')
            i.set_xlabel('Regions')
        i.set_ylim([0, (max(df['value'])+max(df['value'])/5)])
        i.bar(df.index, df['value'], color=colors)
        i.set_ylabel('Accidents')
        i.yaxis.grid(True, linestyle='--', which='major', color='grey', alpha=.25)

        for rect, label in zip(i.patches, df['value']):
            height = rect.get_height()
            i.text(rect.get_x() + rect.get_width() / 2, height + 5, int(label), ha='center', va='bottom')
    # Save figure
    if fig_location:
        try:
            plt.savefig(fig_location, bbox_inches='tight')
        except ValueError:
            raise ValueError(f"ERROR: wrong image dtype, supported: eps, jpeg, jpg, pdf, pgf, png, ps, raw, rgba, svg, svgz, tif, tiff")
    # Show figure
    if show_figure:
        plt.show()
    
    plt.close(fig)

# Ukol3: příčina nehody a škoda
def plot_damage(df: pd.DataFrame, fig_location: str = None,
                show_figure: bool = False):
    
    # Prepare p12
    names = ['Not caused by the driver','Speeding','Incorrect overtaking',
             'Not giving priority in driving','Wrong way of driving','Technical defect of the vehicle']
    bins = [(99, 100), (200, 209), (300, 311), (400, 414), (500, 516), (600, 616)]
    index = pd.IntervalIndex.from_tuples(bins)
    to_name = {interval: name for interval, name in zip(index.values, names)}
    df['p12'] = pd.CategoricalIndex(pd.cut(df['p12'], index)).rename_categories(to_name)
    # Prepare p53
    names = ['<50','50-200','200-500','500-1000','>1000']
    bins = [(float('-inf'), 49.99), (49.99, 199.99), (199.99, 499.99), (499.99, 1000), (1000, float('inf'))]
    index = pd.IntervalIndex.from_tuples(bins)
    to_name = {interval: name for interval, name in zip(index.values, names)}
    df['p53'] = pd.CategoricalIndex(pd.cut(df['p53'], index)).rename_categories(to_name)
    print(df['p53'])

    fig, axes = plt.subplots(2, 2, figsize=(8, 12))
    ax = axes.flatten()

    ax[0] = sns.histplot(data=df['p12'])
    # Save figure
    if fig_location:
        try:
            plt.savefig(fig_location, bbox_inches='tight')
        except ValueError:
            raise ValueError(f"ERROR: wrong image dtype, supported: eps, jpeg, jpg, pdf, pgf, png, ps, raw, rgba, svg, svgz, tif, tiff")
    # Show figure
    if show_figure:
        plt.show()
    
    plt.close(fig)


# Ukol 4: povrch vozovky
def plot_surface(df: pd.DataFrame, fig_location: str = None,
                 show_figure: bool = False):
    pass


if __name__ == "__main__":
    pass
    # zde je ukazka pouziti, tuto cast muzete modifikovat podle libosti
    # skript nebude pri testovani pousten primo, ale budou volany konkreni ¨
    # funkce.
    df = get_dataframe("accidents.pkl.gz", verbose=True)
    #plot_conseq(df, fig_location="01_nasledky.png", show_figure=True)
    plot_damage(df, "02_priciny.png", True)
    plot_surface(df, "03_stav.png", True)
