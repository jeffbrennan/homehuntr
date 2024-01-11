from typing import OrderedDict
from polars import Struct, Field, Utf8, List, Int64, Float64


def get_direction_schema():
    schema = schema = OrderedDict(
        {
            "geocoded_waypoints": List(
                Struct(
                    [
                        Field("geocoder_status", Utf8),
                        Field("place_id", Utf8),
                        Field("types", List(Utf8)),
                    ]
                )
            ),
            "routes": List(
                Struct(
                    [
                        Field(
                            "bounds",
                            Struct(
                                [
                                    Field(
                                        "northeast",
                                        Struct(
                                            [
                                                Field("lat", Float64),
                                                Field("lng", Float64),
                                            ]
                                        ),
                                    ),
                                    Field(
                                        "southwest",
                                        Struct(
                                            [
                                                Field("lat", Float64),
                                                Field("lng", Float64),
                                            ]
                                        ),
                                    ),
                                ]
                            ),
                        ),
                        Field("copyrights", Utf8),
                        Field(
                            "legs",
                            List(
                                Struct(
                                    [
                                        Field(
                                            "arrival_time",
                                            Struct(
                                                [
                                                    Field("text", Utf8),
                                                    Field("time_zone", Utf8),
                                                    Field("value", Int64),
                                                ]
                                            ),
                                        ),
                                        Field(
                                            "departure_time",
                                            Struct(
                                                [
                                                    Field("text", Utf8),
                                                    Field("time_zone", Utf8),
                                                    Field("value", Int64),
                                                ]
                                            ),
                                        ),
                                        Field(
                                            "distance",
                                            Struct(
                                                [
                                                    Field("text", Utf8),
                                                    Field("value", Int64),
                                                ]
                                            ),
                                        ),
                                        Field(
                                            "duration",
                                            Struct(
                                                [
                                                    Field("text", Utf8),
                                                    Field("value", Int64),
                                                ]
                                            ),
                                        ),
                                        Field("end_address", Utf8),
                                        Field(
                                            "end_location",
                                            Struct(
                                                [
                                                    Field("lat", Float64),
                                                    Field("lng", Float64),
                                                ]
                                            ),
                                        ),
                                        Field("start_address", Utf8),
                                        Field(
                                            "start_location",
                                            Struct(
                                                [
                                                    Field("lat", Float64),
                                                    Field("lng", Float64),
                                                ]
                                            ),
                                        ),
                                        Field(
                                            "steps",
                                            List(
                                                Struct(
                                                    [
                                                        Field(
                                                            "distance",
                                                            Struct(
                                                                [
                                                                    Field("text", Utf8),
                                                                    Field(
                                                                        "value", Int64
                                                                    ),
                                                                ]
                                                            ),
                                                        ),
                                                        Field(
                                                            "duration",
                                                            Struct(
                                                                [
                                                                    Field("text", Utf8),
                                                                    Field(
                                                                        "value", Int64
                                                                    ),
                                                                ]
                                                            ),
                                                        ),
                                                        Field(
                                                            "end_location",
                                                            Struct(
                                                                [
                                                                    Field(
                                                                        "lat", Float64
                                                                    ),
                                                                    Field(
                                                                        "lng", Float64
                                                                    ),
                                                                ]
                                                            ),
                                                        ),
                                                        Field(
                                                            "html_instructions", Utf8
                                                        ),
                                                        Field(
                                                            "polyline",
                                                            Struct(
                                                                [Field("points", Utf8)]
                                                            ),
                                                        ),
                                                        Field(
                                                            "start_location",
                                                            Struct(
                                                                [
                                                                    Field(
                                                                        "lat", Float64
                                                                    ),
                                                                    Field(
                                                                        "lng", Float64
                                                                    ),
                                                                ]
                                                            ),
                                                        ),
                                                        Field(
                                                            "steps",
                                                            List(
                                                                Struct(
                                                                    [
                                                                        Field(
                                                                            "distance",
                                                                            Struct(
                                                                                [
                                                                                    Field(
                                                                                        "text",
                                                                                        Utf8,
                                                                                    ),
                                                                                    Field(
                                                                                        "value",
                                                                                        Int64,
                                                                                    ),
                                                                                ]
                                                                            ),
                                                                        ),
                                                                        Field(
                                                                            "duration",
                                                                            Struct(
                                                                                [
                                                                                    Field(
                                                                                        "text",
                                                                                        Utf8,
                                                                                    ),
                                                                                    Field(
                                                                                        "value",
                                                                                        Int64,
                                                                                    ),
                                                                                ]
                                                                            ),
                                                                        ),
                                                                        Field(
                                                                            "end_location",
                                                                            Struct(
                                                                                [
                                                                                    Field(
                                                                                        "lat",
                                                                                        Float64,
                                                                                    ),
                                                                                    Field(
                                                                                        "lng",
                                                                                        Float64,
                                                                                    ),
                                                                                ]
                                                                            ),
                                                                        ),
                                                                        Field(
                                                                            "html_instructions",
                                                                            Utf8,
                                                                        ),
                                                                        Field(
                                                                            "maneuver",
                                                                            Utf8,
                                                                        ),
                                                                        Field(
                                                                            "polyline",
                                                                            Struct(
                                                                                [
                                                                                    Field(
                                                                                        "points",
                                                                                        Utf8,
                                                                                    )
                                                                                ]
                                                                            ),
                                                                        ),
                                                                        Field(
                                                                            "start_location",
                                                                            Struct(
                                                                                [
                                                                                    Field(
                                                                                        "lat",
                                                                                        Float64,
                                                                                    ),
                                                                                    Field(
                                                                                        "lng",
                                                                                        Float64,
                                                                                    ),
                                                                                ]
                                                                            ),
                                                                        ),
                                                                        Field(
                                                                            "travel_mode",
                                                                            Utf8,
                                                                        ),
                                                                    ]
                                                                )
                                                            ),
                                                        ),
                                                        Field("travel_mode", Utf8),
                                                        Field(
                                                            "transit_details",
                                                            Struct(
                                                                [
                                                                    Field(
                                                                        "arrival_stop",
                                                                        Struct(
                                                                            [
                                                                                Field(
                                                                                    "location",
                                                                                    Struct(
                                                                                        [
                                                                                            Field(
                                                                                                "lat",
                                                                                                Float64,
                                                                                            ),
                                                                                            Field(
                                                                                                "lng",
                                                                                                Float64,
                                                                                            ),
                                                                                        ]
                                                                                    ),
                                                                                ),
                                                                                Field(
                                                                                    "name",
                                                                                    Utf8,
                                                                                ),
                                                                            ]
                                                                        ),
                                                                    ),
                                                                    Field(
                                                                        "arrival_time",
                                                                        Struct(
                                                                            [
                                                                                Field(
                                                                                    "text",
                                                                                    Utf8,
                                                                                ),
                                                                                Field(
                                                                                    "time_zone",
                                                                                    Utf8,
                                                                                ),
                                                                                Field(
                                                                                    "value",
                                                                                    Int64,
                                                                                ),
                                                                            ]
                                                                        ),
                                                                    ),
                                                                    Field(
                                                                        "departure_stop",
                                                                        Struct(
                                                                            [
                                                                                Field(
                                                                                    "location",
                                                                                    Struct(
                                                                                        [
                                                                                            Field(
                                                                                                "lat",
                                                                                                Float64,
                                                                                            ),
                                                                                            Field(
                                                                                                "lng",
                                                                                                Float64,
                                                                                            ),
                                                                                        ]
                                                                                    ),
                                                                                ),
                                                                                Field(
                                                                                    "name",
                                                                                    Utf8,
                                                                                ),
                                                                            ]
                                                                        ),
                                                                    ),
                                                                    Field(
                                                                        "departure_time",
                                                                        Struct(
                                                                            [
                                                                                Field(
                                                                                    "text",
                                                                                    Utf8,
                                                                                ),
                                                                                Field(
                                                                                    "time_zone",
                                                                                    Utf8,
                                                                                ),
                                                                                Field(
                                                                                    "value",
                                                                                    Int64,
                                                                                ),
                                                                            ]
                                                                        ),
                                                                    ),
                                                                    Field(
                                                                        "headsign", Utf8
                                                                    ),
                                                                    Field(
                                                                        "headway", Int64
                                                                    ),
                                                                    Field(
                                                                        "line",
                                                                        Struct(
                                                                            [
                                                                                Field(
                                                                                    "agencies",
                                                                                    List(
                                                                                        Struct(
                                                                                            [
                                                                                                Field(
                                                                                                    "name",
                                                                                                    Utf8,
                                                                                                ),
                                                                                                Field(
                                                                                                    "phone",
                                                                                                    Utf8,
                                                                                                ),
                                                                                                Field(
                                                                                                    "url",
                                                                                                    Utf8,
                                                                                                ),
                                                                                            ]
                                                                                        )
                                                                                    ),
                                                                                ),
                                                                                Field(
                                                                                    "color",
                                                                                    Utf8,
                                                                                ),
                                                                                Field(
                                                                                    "icon",
                                                                                    Utf8,
                                                                                ),
                                                                                Field(
                                                                                    "name",
                                                                                    Utf8,
                                                                                ),
                                                                                Field(
                                                                                    "short_name",
                                                                                    Utf8,
                                                                                ),
                                                                                Field(
                                                                                    "text_color",
                                                                                    Utf8,
                                                                                ),
                                                                                Field(
                                                                                    "url",
                                                                                    Utf8,
                                                                                ),
                                                                                Field(
                                                                                    "vehicle",
                                                                                    Struct(
                                                                                        [
                                                                                            Field(
                                                                                                "icon",
                                                                                                Utf8,
                                                                                            ),
                                                                                            Field(
                                                                                                "name",
                                                                                                Utf8,
                                                                                            ),
                                                                                            Field(
                                                                                                "type",
                                                                                                Utf8,
                                                                                            ),
                                                                                        ]
                                                                                    ),
                                                                                ),
                                                                            ]
                                                                        ),
                                                                    ),
                                                                    Field(
                                                                        "num_stops",
                                                                        Int64,
                                                                    ),
                                                                ]
                                                            ),
                                                        ),
                                                    ]
                                                )
                                            ),
                                        ),
                                        Field("traffic_speed_entry", List(Utf8)),
                                        Field("via_waypoint", List(Utf8)),
                                    ]
                                )
                            ),
                        ),
                        Field("overview_polyline", Struct([Field("points", Utf8)])),
                        Field("summary", Utf8),
                        Field("warnings", List(Utf8)),
                        Field("waypoint_order", List(Utf8)),
                    ]
                )
            ),
            "status": Utf8,
        }
    )
    return schema
