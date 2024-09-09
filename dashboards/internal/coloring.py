import numpy as np
from plotly.express.colors import sample_colorscale


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


KFZ_COLOR_DICT = {
    1: [249, 85, 48],
    2: [249, 228, 48],
    # 2: [48, 215, 249],
    3: [107, 249, 48]}
