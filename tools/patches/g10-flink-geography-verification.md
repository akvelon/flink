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

# G10 Flink geography accessors

This backport adds the Calcite G10 structural accessors from commit
`7bb90db72` in [Calcite PR #1](https://github.com/akvelon/calcite/pull/1).
Flink resolves the resulting Calcite snapshot as `1.41.0-g10-SNAPSHOT`.

Flink registers the same SQL signatures and operates on its existing
`GeographyData` values. The shape name comes from the ISO WKB subtype.
Emptiness uses the existing WKB walker and handles empty members inside
multi-geometries and geometry collections. Calcite Enumerable uses its
JTS-backed geography wrapper.

## Build and verify

Apply both patches, in order, from a clean Calcite `calcite-1.41.0`
checkout. Use the paths to the patches in the Flink checkout.

```powershell
git apply 'C:/path/to/flink/tools/patches/calcite-1.41.0-geography.patch'
git apply 'C:/path/to/flink/tools/patches/calcite-1.41.0-g10-geography-accessors.patch'
.\gradlew.bat '-Pcalcite.version=1.41.0-g10' :core:publishToMavenLocal :linq4j:publishToMavenLocal :testkit:publishToMavenLocal
```

From Flink with `JAVA_HOME` set to JDK 21, install the planner and
connector prerequisites, then run the geography Parquet tests.

```powershell
$buildOptions = @('-Pjava21-target', '-Djdk21', '-Dfast')
.\mvnw.cmd @buildOptions -pl flink-table/flink-table-planner,flink-connectors/flink-connector-files -am -DskipTests install
.\mvnw.cmd @buildOptions -f flink-formats/pom.xml -N '-Dmaven.test.skip=true' install
.\mvnw.cmd @buildOptions -pl flink-formats/flink-avro '-Dmaven.test.skip=true' install
.\mvnw.cmd @buildOptions -pl flink-formats/flink-parquet '-Dtest=ParquetRowDataWriterTest#testGeography*+testNestedGeographyType' package
.\mvnw.cmd @buildOptions -pl flink-table/flink-table-planner '-Dtest=FlinkTypeFactoryTest#test*Geography*' test
.\mvnw.cmd @buildOptions -pl flink-table/flink-table-common '-Dtest=BinaryGeographyDataTest,GeographyTypeTest' test
```

`ParquetRowDataWriterTest.testGeographyAccessors` writes 19 fixtures to
Parquet, evaluates both accessors in a Flink SQL batch insert, and reads
the Parquet sink back. It checks all seven shapes, both WKB byte orders,
empty values and empty collection members, SQL NULLs, result types,
row counts, and byte-for-byte geography payload preservation.

## Results

The Calcite snapshot built and installed to the local Maven repository.
A separate Calcite JDBC smoke test passed on adapter-supplied non-empty,
empty, and NULL values.
The Flink planner built, and the Parquet module packaged successfully.
All four selected Parquet tests passed, along with six planner geography
tests and 27 common geography data and type tests.

The standalone SQL probe also passed both calls on a non-empty point,
an empty point, and SQL NULL. These results cover the local batch Parquet
workflow.
