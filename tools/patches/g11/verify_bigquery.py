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

"""Verify saved native BigQuery results against the G11 fixture and Flink results."""

import argparse
import csv
import json
from pathlib import Path

LICENSE = """Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License."""


def load(path):
    return json.loads(path.read_text(encoding="utf-8"))


def decode_value(field, value):
    if value is None:
        return None
    if field.get("mode") == "REPEATED":
        element = dict(field, mode="NULLABLE")
        return [decode_value(element, item["v"]) for item in value]
    if field["type"] == "RECORD":
        return {child["name"]: decode_value(child, item["v"])
                for child, item in zip(field["fields"], value["f"])}
    return value


def decode(document):
    assert not document.get("errors"), document.get("errors")
    assert not document.get("pageToken"), "Save all BigQuery pages before verification"
    fields = document["schema"]["fields"]
    rows = [{field["name"]: decode_value(field, item["v"])
             for field, item in zip(fields, row["f"])} for row in document["rows"]]
    assert len(rows) == int(document["totalRows"])
    return rows


def boolean(value):
    return None if value is None else value == "true"


def verify(args):
    fixtures = load(args.fixtures)
    flink = load(args.flink)
    audit_document = load(args.audit)
    native_document = load(args.native)
    audit, native = decode(audit_document), decode(native_document)
    count = fixtures["row_count"]
    assert len(audit) == len(native) == flink["readback_rows"] == count
    native_fields = {field["name"]: field for field in native_document["schema"]["fields"]}
    assert native_fields["geo"]["type"] == "GEOGRAPHY"
    assert native_fields["nested"]["fields"][1]["type"] == "GEOGRAPHY"
    projected_array = native_fields["geos_with_offsets"]
    assert projected_array["mode"] == "REPEATED"
    assert projected_array["fields"][1]["type"] == "GEOGRAPHY"
    matrix = []
    for expected, actual, returned, readback in zip(
            fixtures["records"], audit, native, flink["actual_records"]):
        assert expected["id"] == int(actual["id"]) == int(returned["id"]) == readback["id"]
        assert expected["name"] == actual["name"] == returned["name"] == readback["name"]
        assert readback["status"] == "PASS"
        for field in ("geo_wkb", "geos_wkb", "nested", "expected_case_geo_wkb",
                      "expected_empty", "expected_shape"):
            assert expected[field] == readback[field], (expected["id"], field)
        assert actual["native_geo_type"] == actual["native_nested_geo_type"] == "GEOGRAPHY"
        assert actual["native_array_type"] == "ARRAY<GEOGRAPHY>"
        assert actual["scalar_content_matches"] == actual["nested_content_matches"] == "true"
        assert boolean(actual["scalar_is_null"]) == (expected["geo_wkb"] is None)
        assert boolean(actual["scalar_is_empty"]) == expected["expected_empty"]
        assert returned["geo"] == actual["scalar_wkt"]
        assert boolean(actual["nested_is_null"]) == (expected["nested"] is None)
        assert (returned["nested"] is None) == (expected["nested"] is None)
        null_leaf = expected["nested"] is None or expected["nested"]["geo_wkb"] is None
        assert boolean(actual["nested_leaf_is_null"]) == null_leaf
        if returned["nested"] is not None:
            assert returned["nested"]["label"] == expected["nested"]["label"]
            assert (returned["nested"]["geo"] is None) == null_leaf
        elements = expected["geos_wkb"]
        normalized = elements is None and not boolean(actual["array_is_null"])
        assert not normalized or args.accept_null_list_normalization, expected["id"]
        assert boolean(actual["array_is_null"]) == (elements is None and not normalized)
        assert int(actual["array_length"]) == len(elements or [])
        array_audit = json.loads(actual["array_values_json"])
        assert len(array_audit) == len(returned["geos_with_offsets"]) == len(elements or [])
        for offset, (element, checked, projected) in enumerate(zip(
                elements or [], array_audit, returned["geos_with_offsets"])):
            assert checked["position"] == int(projected["position"]) == offset
            assert checked["content_matches"] is True
            assert checked["is_null"] == (element is None) == (projected["geo"] is None)
            assert projected["geo"] == checked["wkt"]
        shape_changed = (expected["expected_shape"] is not None
                         and actual["scalar_shape"][3:].upper() != expected["expected_shape"])
        matrix.append({
            "id": expected["id"], "name": expected["name"], "byte_order": expected["byte_order"],
            "flink_shape": expected["expected_shape"], "bigquery_shape": actual["scalar_shape"],
            "empty": expected["expected_empty"], "scalar_null": expected["geo_wkb"] is None,
            "list_null_normalized": normalized, "shape_normalized": shape_changed,
            "flink_wkb": "PASS", "bigquery_native_projection": "PASS",
            "bigquery_geographic_content": "PASS"})
    args.output.mkdir(parents=True, exist_ok=True)
    for index, row in enumerate(matrix):
        row["_license"] = LICENSE if index == 0 else ""
    (args.output / "coverage-matrix.json").write_text(json.dumps(matrix, indent=2) + "\n")
    with (args.output / "coverage-matrix.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(matrix[0]))
        writer.writeheader()
        writer.writerows(matrix)
    summary = {
        "_license": LICENSE,
        "status": "PASS_WITH_ACCEPTED_BIGQUERY_BOUNDARIES", "rows": count,
        "snapshot_id": flink["snapshot_id"], "metadata_location": flink["metadata_location"],
        "native_result_job": native_document["jobReference"],
        "semantic_audit_job": audit_document["jobReference"],
        "null_lists_normalized": sum(row["list_null_normalized"] for row in matrix),
        "scalar_shapes_normalized": sum(row["shape_normalized"] for row in matrix),
        "accepted_null_list_normalization": args.accept_null_list_normalization,
        "selected_read_path": "Native GEOGRAPHY with UNNEST/STRUCT array projection",
        "raw_array_result_with_null_elements": "UNSUPPORTED: BigQuery result-format restriction",
        "untested": ["streaming", "state/network/recovery", "compiled plans", "SQL Gateway"],
    }
    (args.output / "verification-summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("fixtures", "flink", "audit", "native", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--accept-null-list-normalization", action="store_true")
    verify(parser.parse_args())


if __name__ == "__main__":
    main()
