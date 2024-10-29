
import os
import logging

import numpy as np
from plotly.express.colors import sample_colorscale
import pandas as pd
import geopandas as gpd
import pydeck as pdk


logging_level_str = os.getenv("LOGGING_LEVEL", "ERROR")


def create_values(
        min_value: float,
        max_value: float) -> list[float]:  

    vals = np.arange(
        start=min_value,
        stop=max_value+0.01,
        step=0.01)

    vals = np.round(vals, decimals=2)

    return vals


def create_values_colorscale(
        values: list[float],
        colorscale: str = "Plotly3"):

    normed_vals = values / (max(values) + 0.01)

    colorscale = sample_colorscale(
        colorscale=colorscale,
        samplepoints=normed_vals)

    return colorscale


def convert_plotly_to_rgb(
        plotly_str: str):
    
    color = plotly_str.removeprefix("rgb(")
    color = color.removesuffix(")")

    color = color.split(", ")
    color = [int(_) for _ in color]

    return color


def get_color_dict(
        min_value: float,
        max_value: float,
        colorscale: str):
    
    values = create_values(min_value=min_value, max_value=max_value)

    colorscale = create_values_colorscale(
        values=values,
        colorscale=colorscale)

    for i, color in enumerate(colorscale):

        colorscale[i] = convert_plotly_to_rgb(color)


    return dict(zip(values, colorscale))


def add_color_to_data(
        data: pd.DataFrame,
        min_value: float,
        max_value: float,
        value_col: str = "value",
        colorscale: str = "Plotly3",
        color_dict: dict | None = None) -> pd.DataFrame:
    """
    Adds color column to given data.

    Parameters:
    -------------
    data: pd.DataFrame
        The data to add color to.

    min_value: float
        Minimum value to use for colorscale.

    max_value: float
        Maximum value to use for colorscale.

    value_col: str = "value"
        Value column to use for scaling.

    colorscale: str = "Plotly3"
        Plotly colorscale to use.

    color_dict: dict[str, list[int, int, int]]
        Dictionary to containing pairs of value: [r, g, b]
        to use as colorscale. Will overwrite "colorscale"
        parameter.
    """

    logging.info("Adding color")

    if not color_dict:
        color_dict = get_color_dict(
            min_value=min_value,
            max_value=max_value,
            colorscale=colorscale)

    data["adjusted_value"] = data[value_col].copy()
    data.loc[data["adjusted_value"] > max_value, "adjusted_value"] = max_value
    data.loc[data["adjusted_value"] < min_value, "adjusted_value"] = min_value

    data["color"] = data["adjusted_value"].round(2).map(color_dict)

    data.drop(columns="adjusted_value", inplace=True)

    logging.info("Done")

    return data


def create_viewstate():
    """
    Creates viewstate
    """

    viewstate = pdk.ViewState(
        latitude=50.7751,
        longitude=6.0838,
        zoom=13,
        pitch=0)

    return viewstate


def create_poylgon_pydeck(
        data: pd.DataFrame,
        tooltip: dict[str, str] | None = None) -> pdk.Deck:
    """
    Creates pydeck with PathLayer

    Parameters:
    -------------
    data: pd.DataFrame
        The data to create the pydeck for

    tooltip: dict[str, str]
        The tooltip to display on the pydeck
    """

    logging.info("Creating deck")

    layer = pdk.Layer(
        type="PolygonLayer",
        data=data,
        pickable=True,
        filled=True,
        # wireframe=True,
        get_fill_color="color",
        get_line_color=[255, 255, 255],
        get_polygon="geom")

    viewstate = create_viewstate()

    deck = pdk.Deck(
        layers=[layer],
        initial_view_state=viewstate,
        tooltip=tooltip)

    logging.info("Done")

    return deck

def conv_to_list(geom):

    xx, yy = geom.exterior.coords.xy

    return [[[x, y] for x, y in zip(xx.tolist(), yy.tolist())]]


def read_prepare_data(filepath):

    logging.info("Reading preparing data")

    gdf = gpd.read_file(filepath)
    
    gdf = gdf.set_geometry("geometry")

    gdf.to_crs("wgs84", inplace=True)

    gdf["geom"] = gdf["geometry"].apply(conv_to_list)

    logging.info("Done")

    return gdf


def main():

    files = [] # enter the paths to your files containing the walkability scores here

    for file in files:

        logging.info("Working on " + file)

        gdf = read_prepare_data(file)

        gdf = add_color_to_data(
            data=gdf,
            min_value=gdf["walk_score"].min(),
            max_value=gdf["walk_score"].max(),
            value_col="walk_score")

        deck = create_poylgon_pydeck(
            data=gdf,
            tooltip={"text": "Score: {score}"})

        logging.info("Saving deck")

        filename = "your_filename_here" # enter the filename here (without .html)
        deck.to_html(filename + ".html")

        logging.info("Done.")



if __name__ == "__main__":


    if logging_level_str == "INFO":
        logging_level = logging.INFO
    elif logging_level_str == "WARNING":
        logging_level = logging.WARNING
    elif logging_level_str == "ERROR":
        logging_level = logging.ERROR

    # logging
    logging.basicConfig(
        level=logging_level,
        format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
        datefmt='%d-%m-%Y %H:%M:%S')

    main()
