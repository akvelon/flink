#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Generate the G11 2D fixture matrix without a spatial-library dependency."""

import argparse
import hashlib
import json
import math
import struct
from pathlib import Path

NAMES = {1: "POINT", 2: "LINESTRING", 3: "POLYGON", 4: "MULTIPOINT",
         5: "MULTILINESTRING", 6: "MULTIPOLYGON", 7: "GEOMETRYCOLLECTION"}


def encode(value, byte_order):
    kind, coordinates = value
    prefix = "<" if byte_order == "little" else ">"
    header = struct.pack(prefix + "BI", int(byte_order == "little"), kind)
    count = lambda number: struct.pack(prefix + "I", number)
    point = lambda xy: struct.pack(prefix + "dd", *xy)
    if kind == 1:
        return header + point(coordinates or (math.nan, math.nan))
    if kind == 2:
        return header + count(len(coordinates)) + b"".join(map(point, coordinates))
    if kind == 3:
        return header + count(len(coordinates)) + b"".join(
            count(len(ring)) + b"".join(map(point, ring)) for ring in coordinates)
    return header + count(len(coordinates)) + b"".join(
        encode(child, byte_order) for child in coordinates)


def empty(value):
    kind, coordinates = value
    return not coordinates or kind >= 4 and all(map(empty, coordinates))


def generate():
    point = (1, (1.0, 2.0))
    line = (2, [(0.0, 0.0), (1.0, 1.0), (2.0, 1.0)])
    outer = [(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)]
    hole = [(1.0, 1.0), (1.0, 2.0), (2.0, 2.0), (2.0, 1.0), (1.0, 1.0)]
    polygon = (3, [outer, hole])
    second_polygon = (3, [[(10.0, 10.0), (12.0, 10.0), (12.0, 12.0),
                           (10.0, 12.0), (10.0, 10.0)]])
    shapes = [point, line, polygon,
              (4, [point, (1, (3.0, 4.0))]),
              (5, [line, (2, [(10.0, 10.0), (11.0, 11.0)])]),
              (6, [polygon, second_polygon]),
              (7, [point, line, polygon, (4, [point, (1, (3.0, 4.0))])])]
    records = []

    def payload(value, order):
        return None if value is None else encode(value, order).hex().upper()

    def add(name, value, order="little", array=None, nested="same"):
        encoded = payload(value, order)
        records.append({
            "id": len(records) + 1, "name": name, "byte_order": order,
            "geo_wkb": encoded, "geo_sha256": None if encoded is None else
                hashlib.sha256(bytes.fromhex(encoded)).hexdigest(),
            "expected_shape": None if value is None else NAMES[value[0]],
            "expected_empty": None if value is None else empty(value),
            "geos_wkb": array,
            "nested": {"label": name, "geo_wkb": encoded} if nested == "same" else nested,
            "expected_case_geo_wkb": encoded if (len(records) + 1) % 2 else None,
            "bigquery_required_native_type": "GEOGRAPHY",
            "bigquery_result": "NOT_RUN",
        })

    for order in ("little", "big"):
        for shape in shapes:
            name = NAMES[shape[0]].lower()
            add(name + "_nonempty_" + order, shape, order)
            add(name + "_empty_" + order, (shape[0], []), order)
        for kind, child in ((4, (1, [])), (5, (2, [])), (6, (3, []))):
            add(NAMES[kind].lower() + "_empty_members_" + order,
                (kind, [child, child]), order)
        add("collection_empty_members_" + order,
            (7, [(1, []), (2, []), (3, []), (4, [])]), order)

    add("sql_null", None, nested=None)
    for label, members in (
        ("points", [point, (1, (3.0, 4.0))]),
        ("polygons", [polygon, second_polygon]),
        ("multipolygons", [shapes[5], shapes[5]]),
        ("mixed_shapes", [point, polygon, shapes[5]]),
    ):
        add("array_" + label, shapes[5], array=[
            payload(value, "little" if index % 2 == 0 else "big")
            for index, value in enumerate(members)])
    add("array_empty", point, array=[])
    add("array_null", point, array=None)
    add("array_null_element", point, array=[None])
    add("array_values_and_null", polygon, array=[
        payload(point, "little"), None, payload(polygon, "big"),
        payload(shapes[5], "little")])
    add("nested_row_null", point, nested=None)
    add("nested_leaf_null", point, nested={"label": "null-leaf", "geo_wkb": None})
    return records


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    records = generate()
    args.output.mkdir(parents=True, exist_ok=True)
    document = {
        "schema": "ROW<id BIGINT NOT NULL, name STRING NOT NULL, geo GEOGRAPHY, "
                  "geos ARRAY<GEOGRAPHY>, nested ROW<label STRING, geo GEOGRAPHY>>",
        "row_count": len(records), "records": records,
        "bigquery_note": "Native Iceberg schema recognition must succeed before semantic "
                         "comparison; reconstruction from bytes is not native-read evidence.",
    }
    (args.output / "fixtures.json").write_text(
        json.dumps(document, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"rows": len(records), "output": str(args.output / "fixtures.json")}))


if __name__ == "__main__":
    main()
