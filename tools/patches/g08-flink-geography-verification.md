<!--
Licensed to the Apache Software Foundation (ASF) under one
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
limitations under the License.
-->

# G08 Flink GEOGRAPHY verification

Internal Flink 2.5-SNAPSHOT was tested on Windows with JDK 21 and Parquet
1.16.0. The build used a local Calcite 1.41.0 backport of G02–G07 from
[akvelon/calcite#1](https://github.com/akvelon/calcite/pull/1), through
commit `e6fec3d41`. The Flink build resolves the backport as
`1.41.0-g08-SNAPSHOT`.

## Results

| Check | Result |
| --- | --- |
| Flink planner and filesystem connector build | Passed |
| Geography cases in `FlinkTypeFactoryTest` | 6 passed |
| Scalar and nested Parquet geography tests and SQL round trip | 3 passed |
| Parquet module packaging | Passed |

The SQL round trip ran a batch job against a Parquet fixture through the
filesystem connector, Flink SQL planner and runtime, and a Parquet sink.
A second SQL query read the sink back through the same connector.

- The type bridge returned Calcite `GEOGRAPHY` for Flink's non-nullable
  `GeographyType` and preserved nullability in both directions, including
  nullable geography leaves inside an array.
- The SQL projection resolved the `CASE` result and explicit typed NULL as
  nullable `GEOGRAPHY`, and preserved the `ARRAY<GEOGRAPHY>` column.
- The sink readback returned exactly two rows. Row 1 contained the original
  geography payload and an array with a second geography payload followed by
  NULL. Both payloads matched the original WKB byte for byte. Row 2 contained
  NULL for the scalar and array. The typed NULL column was NULL in both rows.

The first run exposed a missing planner size estimate for `GEOGRAPHY` and
open Parquet fixture handles on Windows. The focused run passed after both
fixes. The broader reactor stopped at Avro test compilation because
`InternalTypeInfo` was missing from its test classpath. Avro's main artifact
was built separately with test compilation skipped. A full
`FlinkTypeFactoryTest` run had five failures in RAW and BITMAP cases.

Coverage is limited to the named batch workflow. Compiled plans, SQL Gateway,
state serialization, dedicated network transport, recovery, and WKT or WKB
conversion functions were outside the test scope.

## Reproduce the focused run

From a Calcite checkout at `calcite-1.41.0`, apply
[`calcite-1.41.0-geography.patch`](calcite-1.41.0-geography.patch) and publish
the three modules to the local Maven repository.

```powershell
git apply <path-to-patch>
.\gradlew.bat '-Pcalcite.version=1.41.0-g08' :core:publishToMavenLocal :linq4j:publishToMavenLocal :testkit:publishToMavenLocal
```

From Flink with `JAVA_HOME` set to JDK 21, install the prerequisite modules
and run the focused tests.

```powershell
$buildOptions = @('-Pjava21-target', '-Djdk21', '-Dfast')
.\mvnw.cmd @buildOptions -pl flink-table/flink-table-planner,flink-connectors/flink-connector-files -am -DskipTests install
.\mvnw.cmd @buildOptions -f flink-formats/pom.xml -N '-Dmaven.test.skip=true' install
.\mvnw.cmd @buildOptions -pl flink-formats/flink-avro '-Dmaven.test.skip=true' install
.\mvnw.cmd @buildOptions -pl flink-table/flink-table-planner '-Dtest=FlinkTypeFactoryTest#test*Geography*' install
.\mvnw.cmd @buildOptions -pl flink-formats/flink-parquet '-Dtest=ParquetRowDataWriterTest#testGeography*+testNestedGeographyType' package
```
