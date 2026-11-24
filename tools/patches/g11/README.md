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

# G11 native geography cross-engine verification

Final verification on 2026-11-24: **47 fixtures passed Flink SQL → native Iceberg/Parquet
in GCS → a fresh Flink session → native BigQuery Iceberg reads**. This result
uses the two explicitly accepted BigQuery boundaries below. It does not certify
general BigQuery Iceberg geography support, arbitrary versions, or other paths.

## Revisions and integration

| Component | Tested source/configuration |
| --- | --- |
| Flink | 2.5-SNAPSHOT; G11 integration `f8b962dc1cf51a53ba35556258fbf453f0974216` merged with target `d0102d7b1838a0da2f85cf878ce3202ecc3135ac`, internal [PR #25](https://github.com/akvelon/flink/pull/25) |
| Calcite contribution | [apache/calcite#5427](https://github.com/apache/calcite/pull/5427), `da1cb15d5f366d2466b88de84fbf8950495d434b`, OPEN and unmerged when tested |
| Calcite dependency | Released **1.41.0**, with 37 source/resource overrides listed in [the copy inventory](calcite-copy-inventory.json); no patched SNAPSHOT dependency |
| Iceberg | [akvelon/iceberg#1](https://github.com/akvelon/iceberg/pull/1), `94de0342ff408d591bcdf872bdfd430f431629a6`; connector module `v2.1`, compiled against stock Flink 2.1.0, exercised with this Flink 2.5-SNAPSHOT |
| Iceberg runtime artifact | `iceberg-flink-runtime-2.1-94de034.jar`, built locally from that worktree; build version `94de034`, not a released Iceberg version |
| Parquet | Flink filesystem fixture reader/writer: 1.16.0; Iceberg shaded native table reader/writer: 1.17.1 |
| Manual cloud runtime | Hadoop client API/runtime 3.3.6; GCS connector hadoop3-2.2.18, resolved separately from production POMs |
| Host/build | Windows, JDK 21.0.10; Maven wrapper with `jdk21`, `java21-target,intellij`; Iceberg Gradle wrapper 8.14.5 through Git Bash |

The inventory records upstream and destination SHA-256 values, original 1.41
backport hashes, and adaptations. Existing Flink overrides retain their other
changes. Adaptations cover 1.41 APIs, Scala 2.12's Java annotation parser, and
`RexBuilder.DEFAULT`. Flink's existing geography parser extension handles syntax;
`Parser.jj` was not edited. Review fixes include ARRAY_MIN/MAX ordering rejection
and collection equality/hash execution guards. RAW/BITMAP common-type handling
continues to reject mixed opaque types before Calcite assignment-rule lookup.
The final target merge retains CALCITE-7716's UUID literal representation and
the target's time-attribute cache fix alongside native GEOGRAPHY. The five
conflicting files were resolved and the combined code was rebuilt and retested.

The community sequence supplied for this task is Calcite review/merge, followed
by copying relevant files into Flink. This is **internal validation before that
merge**, not evidence that the upstream dependency has landed. The public
[Flink type PR #28740](https://github.com/apache/flink/pull/28740) remains a
separate foundation change; its branch does not contain this integration.
Once Flink upgrades to a released Calcite containing #5427, remove the newly
copied overrides and geography additions to existing overrides, retaining other
Flink fixes, then rerun the focused suite and this cross-engine workflow.

The Iceberg worktree's Gradle Git-properties plugin embedded the main checkout's
`f2e7a65678b4c529bb53b299450faf95ef24385b` in `iceberg-build.properties`, although
the worktree HEAD and compiled connector sources were `94de0342...`. The snapshot
summary consequently contains that older commit label. The artifact digest and
actual worktree revision are recorded in `evidence/revisions.json`; the embedded
label is not used as source provenance. Use a standalone checkout for rebuilding
if matching embedded Git metadata is required.

## Exact cloud snapshot

- Project/dataset: `healthy-skill-501608-t7.akvelon`, location `US`.
- Flink catalog: Hadoop catalog `g11`; table `g11.g11.fixture_matrix`, format v3.
- Warehouse: `gs://healthy-skill-501608-t7-g11-geography-20261123/g11/20261123_130455/warehouse/attempt05`.
- Snapshot: **3137959442311624136**, one Parquet file, **47 records**, 4,911 bytes.
- Immutable registration URI: warehouse above + `/g11/fixture_matrix/metadata/v2.metadata.json`.
- BigQuery table: `healthy-skill-501608-t7.akvelon.g11_geography_20261123_130455_merged`.
- Connection: `projects/347542310562/locations/us/connections/g11_geography_20261123_130316`.

The connection has only `storage.objectViewer` on this bucket's `g11/` prefix,
expiring **2026-11-30T13:04:55Z**. No credentials are included in evidence. The
resources are retained for review; the grant expiry is not data cleanup.

## Fixture and result coverage

[coverage-matrix.csv](evidence/coverage-matrix.csv) maps **each of the 47 named
fixtures** to its byte order, expected Flink shape/empty state, actual BigQuery
shape, normalization, and both engines' results. Full expected WKB/schema is in
`evidence/fixtures.json`; actual Flink values and BigQuery query responses are
stored separately. `verification-summary.json` includes the exact BigQuery jobs.

| Cases | Fixtures/result |
| --- | --- |
| Point, LineString, Polygon, MultiPoint, MultiLineString, MultiPolygon, GeometryCollection | Each non-empty and empty, both WKB byte orders: rows 1–28 pass |
| All-empty members and mixed empty/non-empty collections | Rows 29–36 pass |
| SQL NULL geography | Row 37 remains NULL; both structural accessors return NULL |
| Arrays of points, polygons, multipolygons, mixed shapes | Rows 38–41 pass; array-of-polygons stays distinct from scalar MultiPolygon |
| Empty/NULL arrays, NULL elements, values plus NULL elements | Rows 42–45 pass under the accepted BigQuery policies |
| NULL nested row and NULL nested geography leaf | Rows 46–47 preserve the distinction |
| CASE and typed GEOGRAPHY NULL | Every Flink readback row checked; nullable GEOGRAPHY result metadata checked in a fresh session |
| ST_IsEmpty / ST_GeometryType, scalar and nested | Every Flink readback row checked; nullable BOOLEAN/VARCHAR metadata checked |

Flink preserves **all WKB bytes**, including byte order, concrete empty shape,
array order, NULL array versus empty array, NULL elements, and nested NULLs.
The snapshot schema contains native optional geography scalar/struct fields
and an optional list with optional geography elements. Parquet annotations at
all three leaves are **GEOGRAPHY(OGC:CRS84,SPHERICAL)**. No coordinate conversion
or geodetic calculation is introduced by this test.

BigQuery INFORMATION_SCHEMA reports native `GEOGRAPHY`, `ARRAY<GEOGRAPHY>` and
`STRUCT<label STRING, geo GEOGRAPHY>`. Native results contain GEOGRAPHY fields,
including array elements projected inside STRUCTs. The REST protocol renders
those values as WKT strings, but their SQL/result schema is GEOGRAPHY. The
semantic audit uses `ST_EQUALS(actual_native_value, expected_from_fixture_WKB)`;
constructing the **expected** value is not the native-read evidence. Scalar,
array-element, and nested content comparisons all pass. Totals: 47 rows,
one scalar SQL NULL, 22 empty scalar geographies.

### Accepted BigQuery boundaries

1. **NULL lists normalize to empty arrays in the native external reader.**
   All 40 source NULL lists have `IS NULL = FALSE` and length 0 in BigQuery,
   including `array_null`; `array_empty` is also length 0. The task owner accepted
   this documented loss of list NULL/EMPTY distinction. Flink preserves it.
2. **Selected result path uses native UNNEST/STRUCT projection.** A direct
   result containing an `ARRAY<GEOGRAPHY>` with NULL elements fails HTTP 400:
   `Array cannot have a null element; error in writing field geos`. The task
   owner accepted the verified projection below, which preserves offsets and
   NULL geography fields within non-NULL STRUCT elements. Direct raw-array
   export containing NULL elements is unsupported, not a passing test.

```sql
SELECT id, name, geo, nested,
       ARRAY(SELECT AS STRUCT position, g AS geo
             FROM UNNEST(geos) g WITH OFFSET position
             ORDER BY position) AS geos_with_offsets
FROM `healthy-skill-501608-t7.akvelon.g11_geography_20261123_130455_merged`
ORDER BY id;
```

BigQuery also normalizes geography representations: 18 concrete empty/all-empty
member fixtures become `ST_GeometryCollection`, and two mixed collections become
`ST_Polygon` because other members are covered by the polygon. These **20 shape
label changes are recorded**, while geographic content matches in all cases.
Flink retains the concrete original shape. BigQuery WKB byte equality and
concrete empty shape preservation are not claimed.

Published [Iceberg external-table limitations](https://docs.cloud.google.com/bigquery/docs/iceberg-external-tables#limitations)
list geography as unsupported. The result here is a successful **observed native
read in this project and configuration**, not a claim of generally documented
service support or preview enrollment. The accepted projection is a verified
query path. See also Google's [array NULL rules](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/data-types#array_nulls)
and [geography representation](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/data-types#geography_type).

## Checks and historical attempts

- Clean planner rebuild and focused Maven reactor: **71 tests passed**:
  40 FlinkTypeFactoryTest, 25 BinaryGeographyDataTest, two GeographyTypeTest and
  four Parquet geography tests, zero failures/errors/skips. Spotless
  and Checkstyle checks in that reactor passed. RAT was explicitly skipped.
- Iceberg 2.1 connector compile and shaded runtime build passed: 34 tasks.
- Attempt03 and readback04 on 2026-11-23 passed the initial integration's 47-row
  snapshot `1465060231437242140`. Those historical cloud objects and the original
  BigQuery table without the `_merged` suffix remain retained separately.
- Attempt05 on 2026-11-24 repeats the actual Flink insert, native snapshot
  inspection and fresh-session readback **after merging the current PR target**.
  All 47 rows and derived/static type-nullability assertions pass. The selected
  evidence and BigQuery registration now reference this new snapshot.
- Generated BigQuery audit and native projection were rerun from checked-in
  tooling and verified independently against saved fixtures/readback.
- Attempt01 failed on stale planner output (`BuiltInMethod`/UUID signature).
  A clean planner build removed classes without corresponding source files;
  no unrelated UUID runtime patch was needed.
- Attempt02 committed a separate snapshot, then inspection failed because old
  Hadoop 2 classes preceded Hadoop 3. The launcher now puts both Hadoop 3.3.6
  client artifacts first and excludes other Hadoop artifacts. Attempt02 is not
  used as successful cross-engine evidence.
- A sandboxed readback04 launch could not resolve Google OAuth DNS; the permitted
  cloud rerun passed. No authentication workaround or token was checked in.
- Full `clean verify`, full module test suites, API compatibility and remote CI
  were not run for G11. Streaming, state/network/recovery, compiled plans, SQL
  Gateway and other connector/version combinations remain untested.
- A separate root `apache-rat:check` attempt stalled during file collection and
  was stopped without a result; root license verification remains unverified.
  Saved JSON evidence has an additional `_license` metadata field, and the CSV
  records the license in its first data row.

The older [G08](../g08-flink-geography-verification.md) and
[G10](../g10-flink-geography-verification.md) reports describe earlier runs with
custom Calcite dependencies. Their results are historical and are not combined
with this 47-row Iceberg/BigQuery run.

## Reproduce from source

Commands below use PowerShell 7 at the Flink repository root. Google Cloud ADC
must already be configured and authorized for the chosen project. Use a fresh
checkout or clean the planner output; no local patched Calcite install is needed.

```powershell
$env:JAVA_HOME = 'C:\Program Files\Java\jdk-21.0.10'
.\mvnw.cmd -pl flink-table/flink-table-planner clean
.\mvnw.cmd -pl flink-table/flink-table-planner,flink-formats/flink-parquet -am -Djdk21 -Pjava21-target,intellij '-Drat.skip=true' '-Dtest=FlinkTypeFactoryTest,BinaryGeographyDataTest,GeographyTypeTest,ParquetRowDataWriterTest#testGeography*+testNestedGeographyType' '-Dsurefire.failIfNoSpecifiedTests=false' test
python tools/patches/g11/generate_fixtures.py target/g11/fixtures
.\mvnw.cmd -f tools/patches/g11/runtime-deps/pom.xml dependency:build-classpath '-Dmdep.outputFile=C:/Users/garik/IdeaProjects/flink/target/g11/gcs-classpath.txt'
```

The recorded Maven test run used `--offline` with dependencies already cached;
omit that option on a clean machine. For Iceberg, clone PR #1 into a separate
checkout, pin `94de0342ff408d591bcdf872bdfd430f431629a6`, and run:

```bash
./gradlew --no-daemon -DflinkVersions=2.1 -DsparkVersions= -DkafkaVersions= \
  :iceberg-flink:iceberg-flink-2.1:compileJava \
  :iceberg-flink:iceberg-flink-runtime-2.1:shadowJar
```

Provision a **new dedicated bucket/table** for a new write. Substitute the test
project, dataset and unique bucket you control; do not overwrite the recorded
snapshot. The helper creates a seven-day prefix-scoped connection grant:

```powershell
python tools/patches/g11/cloud.py provision --project healthy-skill-501608-t7 --dataset akvelon --bucket YOUR-UNIQUE-G11-BUCKET --create-bucket --output target/g11/cloud-resources.json
./tools/patches/g11/run-probe.ps1 -IcebergRuntimeJar PATH/TO/iceberg-flink-runtime-2.1-94de034.jar -GcsClasspath target/g11/gcs-classpath.txt -CloudResources target/g11/cloud-resources.json -OutputDirectory target/g11/runs/YOUR-NEW-RUN
```

The source-file Java launcher writes a Parquet fixture, executes the filesystem
source → Iceberg INSERT, inspects native schema/annotations and committed count,
and reads every field through a **new** TableEnvironment. To rerun just the same
snapshot readback, add `-ReadbackManifest PATH/TO/flink-iceberg-result.json` and
choose a new output directory. The manifest snapshot ID must still match.
The selected execution is local batch with the direct planner provider. The
launcher excludes `flink-table-planner-loader` from this manual classpath; an
assembled distribution's isolated planner loading is not verified by this run.

```powershell
python tools/patches/g11/prepare_bigquery.py --fixtures target/g11/fixtures/fixtures.json --flink PATH/TO/flink-iceberg-result.json --cloud target/g11/cloud-resources.json --output target/g11/sql
python tools/patches/g11/cloud.py query --project healthy-skill-501608-t7 --sql target/g11/sql/bigquery-register.sql --output target/g11/register-result.json
python tools/patches/g11/cloud.py query --project healthy-skill-501608-t7 --sql target/g11/sql/bigquery-schema.sql --output target/g11/schema-result.json
python tools/patches/g11/cloud.py query --project healthy-skill-501608-t7 --sql target/g11/sql/bigquery-counts.sql --output target/g11/counts-result.json
python tools/patches/g11/cloud.py query --project healthy-skill-501608-t7 --sql target/g11/sql/bigquery-fixture-audit.sql --output target/g11/audit-result.json
python tools/patches/g11/cloud.py query --project healthy-skill-501608-t7 --sql target/g11/sql/bigquery-native-results.sql --output target/g11/native-result.json
python tools/patches/g11/verify_bigquery.py --fixtures target/g11/fixtures/fixtures.json --flink PATH/TO/flink-iceberg-result.json --audit target/g11/audit-result.json --native target/g11/native-result.json --output target/g11/verified --accept-null-list-normalization
```

`bigquery-direct-arrays.sql` is an optional negative check, expected to fail on
NULL elements. Queries are capped at 100 MB billed. Registration uses the exact
committed metadata URI, not a different reconstructed table. Repeated query runs
may use a result cache; saved job metadata records this accurately.

After review, delete only the named G11 external table, dedicated connection,
prefix-scoped grant and dedicated bucket contents according to the team's
retention policy. Do not delete unrelated datasets, connections or buckets.
