from dash import Dash, dash_table, dcc, html, Input, Output, callback
import pandas as pd
import polars as pl
from homehuntr import common
import plotly.express as px

fs, token = common.get_gcp_fs()

summary_df = pl.read_delta(
    "gs://homehuntr-storage/delta/gold/summary",
    storage_options={"SERVICE_ACCOUNT": token},
)


obt = pl.read_delta(
    "gs://homehuntr-storage/delta/gold/obt",
    storage_options={"SERVICE_ACCOUNT": token},
)


# TODO: precompute this table
summary_table_df = (
    summary_df.select(
        "building_address",
        "neighborhood",
        "times_saved",
        "price",
        "url",
        "apartment_score",
        "transit_score",
    )
    .unique()
    .group_by(
        "building_address",
        "neighborhood",
        "times_saved",
        "price",
        "url",
        "apartment_score",
    )
    .agg(pl.mean("transit_score").alias("transit_score"))
    .with_columns(pl.col("building_address").str.split(by=",").alias("address_split"))
    .with_columns(
        address_part=pl.col("address_split")
        .map_elements(lambda x: x[0])
        .str.to_uppercase()
    )
    .with_columns(space_split=pl.col("address_part").str.split(by=" "))
    .with_columns(
        address=(pl.col("space_split").map_elements(lambda x: x[0:-1]).list.join(" "))
    )
    .filter(pl.col("address").is_not_null())
    .filter(pl.col("transit_score").is_not_null())
    .filter(pl.col("apartment_score").is_not_null())
    .with_columns(
        address=pl.concat_str(
            pl.lit('<p><a style="text-decoration: none;" href="'),
            pl.col("url"),
            pl.lit('">'),
            pl.col("address"),
            pl.lit("</a></p>"),
        )
    )
    .with_columns(apartment_score=pl.col("apartment_score") * 100)
    .with_columns(apartment_score=pl.col("apartment_score").floor())
    .with_columns(transit_score=pl.col("transit_score") * 100)
    .with_columns(transit_score=pl.col("transit_score").floor())
    .with_columns(price=pl.col("price").map_elements(lambda x: "${:,}".format(x)))
    .with_columns(
        score_combined=pl.col("apartment_score") * 0.6 + pl.col("transit_score") * 0.4
    )
    .with_columns(rank=pl.col("score_combined").rank(method="ordinal", descending=True))
    .select(
        "rank",
        "address",
        "neighborhood",
        "times_saved",
        "price",
        "apartment_score",
        "transit_score",
    )
    .sort(pl.col("apartment_score"), descending=True)
    .rename(
        {
            "rank": "🏆",
            "address": "📍",
            "price": "💰",
            "neighborhood": "🏘️",
            "times_saved": "📌",
            "apartment_score": "🏠",
            "transit_score": "🚂",
        }
    )
    .to_pandas()
)


travel_time_df = (
    obt.select("building_address", "destination_name", "duration_min")
    .unique()
    .filter(pl.col("duration_min").is_not_null())
    .with_columns(pl.col("destination_name").str.split(by=",").alias("address_split"))
    .with_columns(
        destination=pl.col("address_split")
        .map_elements(lambda x: x[0])
        .str.to_uppercase()
    )
    .drop("address_split")
    .with_columns(pl.col("building_address").str.split(by=",").alias("address_split"))
    .with_columns(
        address_part=pl.col("address_split")
        .map_elements(lambda x: x[0])
        .str.to_uppercase()
    )
    .with_columns(space_split=pl.col("address_part").str.split(by=" "))
    .with_columns(
        origin=(pl.col("space_split").map_elements(lambda x: x[0:-1]).list.join(" "))
    )
    .select("origin", "destination", "duration_min")
)


all_origins = travel_time_df.select("origin").unique().to_dict()["origin"].to_list()


app = Dash(__name__)
app.layout = html.Div(
    [
        html.H1(
            "🏠🎯",
            style={
                "marginTop": "0",
                "paddingTop": "20px",
                "paddingBottom": "0px",
                "font-family": "sans-serif",
            },
        ),
        html.Hr(),
        dash_table.DataTable(
            id="datatable-interactivity",
            columns=[
                {
                    "id": x,
                    "name": x,
                    "presentation": "markdown",
                }
                if x == "📍"
                else {"id": x, "name": x}
                for x in summary_table_df.columns
            ],
            data=summary_table_df.to_dict("records"),
            editable=False,
            filter_action="native",
            sort_action="native",
            sort_mode="multi",
            column_selectable=False,
            row_selectable=False,
            row_deletable=False,
            selected_columns=[],
            selected_rows=[],
            page_action="none",
            style_as_list_view=True,
            style_cell={
                "padding": "5px",
                "font_size": "11px",
                "font-family": "sans-serif",
                "textAlign": "left",
                "border": "none",
            },
            fixed_rows={"headers": True},
            style_header={
                "backgroundColor": "white",
                "fontWeight": "bold",
                "border": "none",
            },
            style_table={"height": "300px", "overflowY": "auto", "border": "none"},
            markdown_options={"html": True},
        ),
        html.Div(id="datatable-interactivity-container"),
        html.Hr(),
        html.H3("🚂 Minutes to Destination", style={"font-family": "sans-serif"}),
        dcc.Dropdown(
            id="dropdown",
            options=all_origins,
            value=all_origins[0],
            clearable=False,
            style={
                "width": "100%",
                "textAlign": "left",
                "font-family": "sans-serif",
                "font-size": "10px",
            },
            optionHeight=20,
        ),
        dcc.Graph(
            id="graph",
            style={
                "overflow": "auto",
                "height": "400px",
                "width": "100%",
                "border": "none",
            },
        ),
        html.Hr(),
    ]
)


@callback(Output("graph", "figure"), Input("dropdown", "value"))
def update_bar_chart(origin: str):
    travel_time_selected_df = (
        travel_time_df.filter(pl.col("origin") == pl.lit(origin))
        .with_columns(duration_min=pl.col("duration_min").floor().cast(pl.Int64))
        .select("origin", "destination", "duration_min")
    )

    travel_time_other_avg_df = (
        travel_time_df.filter(pl.col("origin") != pl.lit(origin))
        .group_by("destination")
        .agg(pl.mean("duration_min").alias("duration_min"))
        .with_columns(duration_min=pl.col("duration_min").floor().cast(pl.Int64))
        .with_columns(origin=pl.lit("avg"))
        .select("origin", "destination", "duration_min")
    )

    travel_time_combined_df = (
        pl.concat([travel_time_selected_df, travel_time_other_avg_df])
        .join(
            travel_time_other_avg_df.rename(
                {"duration_min": "avg_duration_min"}
            ).select("destination", "avg_duration_min"),
            on="destination",
            how="inner",
        )
        .with_columns(
            average_comparison=pl.when(
                pl.col("avg_duration_min") >= pl.col("duration_min")
            )
            .then(pl.lit("faster"))
            .otherwise(pl.lit("slower"))
        )
        .with_columns(
            average_comparison=pl.when(pl.col("origin") == pl.lit("avg"))
            .then(pl.lit("baseline"))
            .otherwise(pl.col("average_comparison"))
        )
        .sort(pl.col("avg_duration_min"))
        .to_pandas()
    )

    fig = px.bar(
        travel_time_combined_df,
        y="destination",
        x="duration_min",
        color="average_comparison",
        barmode="overlay",
        color_discrete_map={
            "baseline": "rgba(0, 0, 0, 0.01)",
            "faster": "rgba(53, 240, 140, 1)",
            "slower": "rgba(250, 92, 0, 1)",
        },
        text="duration_min",
        pattern_shape="origin",
        pattern_shape_map={origin: "/", "avg": None},
    )

    fig.update_traces(
        textfont_size=14,
        textposition="outside",
        marker_line_color="black",
        marker_line_width=3,
    )

    for i in fig.data:
        i["marker"]["opacity"] = 1
        if i["name"] == "baseline, avg":
            i["text"] = None

    fig.update_yaxes(title="")

    fig.update_xaxes(
        title="",
        showticklabels=False,
    )

    fig.update_layout(
        yaxis={"tickfont": {"size": 10}},
        xaxis={"titlefont": {"size": 10}},
        margin={"l": 0, "r": 10, "b": 0, "t": 0},
        showlegend=False,
        plot_bgcolor="rgba(0, 0, 0, 0)",
        hovermode=False,
    )

    return fig


@callback(
    Output("datatable-interactivity", "style_data_conditional"),
    Input("datatable-interactivity", "selected_columns"),
)
def update_styles(selected_columns):
    return [
        {"if": {"column_id": i}, "background_color": "#D2F3FF"}
        for i in selected_columns
    ]


if __name__ == "__main__":
    app.run(debug=True)
