#!/usr/bin/python3.8
# coding=utf-8

"""
| Project Implementation for IZV 2020/2021
| Script geo.py
| Date: 08.12.2020
| Author: Mikhail Abramov
| xabram00@stud.fit.vutbr.cz
"""

import pandas as pd
import geopandas
import matplotlib.pyplot as plt
import contextily as ctx
import sklearn.cluster as skl
import numpy as np
from matplotlib import gridspec
from mpl_toolkits.axes_grid1 import make_axes_locatable


def make_geo(df: pd.DataFrame) -> geopandas.GeoDataFrame:
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
    return gdf


def plot_geo(gdf: geopandas.GeoDataFrame,
             fig_location: str = None,
             show_figure: bool = False):
    """
    plot_conseq
        - Prepare appropriate dataframe
        - Show/Save two maps with coordinates of accidents for JHM region

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
    gdf = gdf.loc[gdf['region'].isin(['JHM'])]
    gdf = gdf[['p5a', 'geometry']]

    # Prepare figure, grid, and list of axes
    fig = plt.figure(figsize=(14, 10))
    gs = gridspec.GridSpec(1, 2)
    axs = []
    # Iterate each subplot with settings
    for i, var in enumerate([('tab:red', 'in settlements'),
                            ('tab:green', 'outside settlements')]):
        # Add subplot to axes list
        axs.append(fig.add_subplot(gs[i]))
        # Put coordinates on the subplot
        gdf[gdf["p5a"] == i+1].plot(ax=axs[i], markersize=3, color=var[0])
        # Adjust maximum x/y axis
        axs[i].set_ylim(6_205_000, 6_390_000)
        axs[i].set_xlim(1_725_000, 1_972_500)
        # Put the background map
        ctx.add_basemap(axs[i], source=ctx.providers.Stamen.TonerLite)
        # Turn off axis
        axs[i].axis("off")
        # Add titles
        axs[i].set_title(f'Accidents in JHM region: {var[1]}')
    # Add figure settings
    fig.tight_layout()
    fig.align_labels()

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


def plot_cluster(gdf: geopandas.GeoDataFrame,
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
    gdf = gdf.loc[gdf['region'].isin(['JHM'])]
    gdf = gdf[['geometry']]

    # Find Kmeans
    kdf = pd.DataFrame()
    kdf['X'] = gdf.centroid.x
    kdf['Y'] = gdf.centroid.y
    kmeans = skl.MiniBatchKMeans(n_clusters=15).fit(kdf)
    kdf = pd.DataFrame(kmeans.labels_,
                       columns=['Cluster']).groupby(
                               ['Cluster']).agg(
                               {'Cluster': ['count']})
    kdf.columns = kdf.columns.droplevel(0)

    # Prepare figure, ax
    fig = plt.figure(figsize=(8, 10))
    ax = fig.add_subplot()
    # Put coordinates on the subplot
    gdf.plot(ax=ax, markersize=0.25, color='tab:gray')
    # Adjust maximum x/y axis
    ax.set_ylim(6_205_000, 6_390_000)
    ax.set_xlim(1_725_000, 1_972_500)
    # plot the centroids
    sc = ax.scatter(
        kmeans.cluster_centers_[:, 0],
        kmeans.cluster_centers_[:, 1],
        s=kdf['count']/5,
        c=kdf['count'],
        alpha=0.6,
        cmap='viridis'
    )
    # Put the background map
    ctx.add_basemap(ax, source=ctx.providers.Stamen.TonerLite)
    # Turn off axis
    ax.axis("off")
    # Add titles
    ax.set_title(f'Accidents in JHM region')
    # Set up colorbar
    divider = make_axes_locatable(ax)
    cax = divider.append_axes('right', size='5%', pad=0.05)
    fig.colorbar(sc, cax=cax)
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
    # zde muzete delat libovolne modifikace
    gdf = make_geo(pd.read_pickle("accidents.pkl.gz"))
    plot_geo(gdf, "geo1.png", False)
    plot_cluster(gdf, "geo2.png", False)
