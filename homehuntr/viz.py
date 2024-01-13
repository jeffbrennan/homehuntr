from dash import Dash, dash_table, dcc, html, Input, Output, callback
import pandas as pd
import polars as pl
from homehuntr import common

fs, token = common.get_gcp_fs()

summary_df = pl.read_delta(
    "gs://homehuntr-storage/delta/gold/summary",
    storage_options={"SERVICE_ACCOUNT": token},
)


# TODO: precompute this table
summary_table_df = (
    summary_df.select(
        "building_address", "price", "url", "apartment_score", "transit_score"
    )
    .unique()
    .group_by("building_address", "price", "url", "apartment_score")
    .agg(pl.mean("transit_score").alias("transit_score"))
    .with_columns(pl.col("building_address").str.split(by=",").alias("address_split"))
    .with_columns(
        address=pl.col("address_split").map_elements(lambda x: x[0]).str.to_uppercase()
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
    .select("rank", "address", "price", "apartment_score", "transit_score")
    .sort(pl.col("apartment_score"), descending=True)
    .rename(
        {
            "rank": "🏆",
            "address": "📍",
            "price": "💰",
            "apartment_score": "🏠",
            "transit_score": "🚂",
        }
    )
    .to_pandas()
)


app = Dash(__name__)

app.layout = html.Div(
    [
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
            },
            fixed_rows={"headers": True},
            style_header={"backgroundColor": "white", "fontWeight": "bold"},
            style_table={"height": "300px", "overflowY": "auto"},
            markdown_options={"html": True},
        ),
        html.Div(id="datatable-interactivity-container"),
    ]
)


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
