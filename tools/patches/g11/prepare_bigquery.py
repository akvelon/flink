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

"""Generate registration and audit SQL for one committed G11 Iceberg snapshot."""

import argparse
import json
import re
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("fixtures", "flink", "cloud", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args()
    fixtures = json.loads(args.fixtures.read_text())
    flink = json.loads(args.flink.read_text())
    cloud = json.loads(args.cloud.read_text())
    table = cloud["bigquery_table"]
    assert re.fullmatch(r"[a-zA-Z0-9_.-]+", table)
    connection = cloud["connection"].split("/")
    connection = ".".join((connection[1], connection[3], connection[5]))
    metadata = flink["metadata_location"]
    assert metadata.startswith("gs://") and "'" not in metadata
    expected = json.dumps(fixtures["records"], separators=(",", ":"))
    assert "'" not in expected
    template = Path(__file__).with_name("audit.sql.template").read_text()
    header = template.split("WITH expected AS", 1)[0]
    queries = {
        "register": f"CREATE EXTERNAL TABLE `{table}` WITH CONNECTION `{connection}`\n"
                    f"OPTIONS (format='ICEBERG', uris=['{metadata}']);\n",
        "fixture-audit": template.replace("FIXTURES", expected).replace("TABLE_NAME", f"`{table}`"),
        "native-results": f"SELECT id, name, geo, nested,\n"
                          "ARRAY(SELECT AS STRUCT position, g AS geo FROM UNNEST(geos) g "
                          "WITH OFFSET position ORDER BY position) AS geos_with_offsets\n"
                          f"FROM `{table}` ORDER BY id;\n",
        "direct-arrays": f"SELECT id, name, geos FROM `{table}` WHERE id BETWEEN 42 AND 45 "
                         "ORDER BY id;\n",
        "counts": "SELECT COUNT(*) AS row_count, COUNTIF(geo IS NULL) AS scalar_nulls, "
                  f"COUNTIF(ST_ISEMPTY(geo)) AS scalar_empties FROM `{table}`;\n",
        "schema": f"SELECT column_name, data_type, is_nullable FROM "
                  f"`{cloud['project']}.{cloud['dataset']}.INFORMATION_SCHEMA.COLUMNS` "
                  f"WHERE table_name='{table.rsplit('.', 1)[1]}' ORDER BY ordinal_position;\n",
    }
    args.output.mkdir(parents=True, exist_ok=True)
    for name, sql in queries.items():
        if not sql.startswith("-- Licensed"):
            sql = header + sql
        (args.output / ("bigquery-" + name + ".sql")).write_text(sql, encoding="utf-8")


if __name__ == "__main__":
    main()
