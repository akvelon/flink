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

# Calcite review follow-up verification — 25 November 2026

The reviewed Calcite changes pass the full Calcite build and static checks,
75 focused Flink tests and a new 47-row Flink → native Iceberg/Parquet → fresh
Flink → native BigQuery run. The earlier [G11 report](../README.md) records
different, historical revisions and snapshots; its counts are not added here.

## Revisions and review decisions

- Calcite [PR #5427](https://github.com/apache/calcite/pull/5427), published
  `f5728f797e8abd61ccf197db48c7dc1c5500dd44`.
- Full Calcite build tested `9d439700b`; its complete source tree is identical
  to the published revision. The later commits link the prerequisite histories.
- Checker Framework and ErrorProne tested `c1fc86b5b`. The only subsequent source
  difference is local-variable setup in a server test; production and build inputs
  are unchanged.
- Upstream target `fc889f72fa3f0180452fc0f9dec4e4d251df7709` was merged without
  rewriting the reviewed history. Review fixes remain separate commits.
- General MAP grammar/rendering and its ordinary regression tests are isolated in
  [prerequisite #5452](https://github.com/apache/calcite/pull/5452).
  Legacy constructor/type-to-SQL output stays unchanged; explicit rendering is
  opt-in. The geography conversion uses that capability for its new type.
- General aggregate Javadoc inputs/dependencies and the HTML escape are isolated
  in [prerequisite #5453](https://github.com/apache/calcite/pull/5453).
  Both prerequisites are drafts pending ASF Jira keys and must land independently.
- The generic ROW + NULL algorithm change was removed: existing Calcite fallback
  already passes the geography cases. Checker filename logging was restored to
  its commented-out setting. Equality/hash, concrete cast restrictions, generic
  ANY/UNKNOWN/VARIANT eligibility and the structural accessor binding are retained.
- SQL documentation now states validation versus execution support explicitly;
  adapter ownership requirements and the usage example are in the adapter guide.

## Calcite results

| Check | Result |
| --- | --- |
| Full build regression XML | **22,775 passed; 754 skipped; zero failures/errors**, 23,529 entries |
| Full build, aggregate production/test Javadocs | PASS, 414 tasks in the final successful invocation |
| Checker Framework, build cache disabled | PASS; JDK 11 analysis toolchain |
| ErrorProne, all module classes | PASS; JDK 11 analysis toolchain |
| Formatting, Checkstyle, RAT, bytecode and forbidden API checks | PASS as configured by the full build |
| Separate MAP prerequisite | Three parser/legacy-rendering tests and server Checkstyle pass |
| Separate Javadoc prerequisite | Both aggregate tasks pass on the upstream target, 123 tasks |

Wrapper Gradle 8.14.4, launcher/test JVM JDK 21.0.10; default build compiler JDK 25,
Java 8 release target. Dependencies were resolved offline from the existing cache.
Skipped tests remain skipped; static checks add no test counts.

The initial unconditional MAP rendering experiment failed eight existing
Spark/Presto/default-dialect SQL-output cases. Opt-in rendering fixed them while
retaining the expected SQL. A test line-length failure was corrected separately.
These initial failures are diagnostic history, not final-revision results.

## Flink backport and results

Flink 2.5-SNAPSHOT starts from `c83957c9f8736980c32339be853aeb78a579187b`.
Released Calcite **1.41.0** remains the dependency. The
[copy inventory](../calcite-copy-inventory.json) records **36 active source/resource
overrides** and the retired ROW-only contribution delta. The existing Flink
RelDataTypeFactoryImpl override is restored to its target version. Unrelated
new upstream algorithms are not blanket-copied into the 1.41 backport.

The refreshed overrides contain the adapter Javadoc, compatible MAP rendering
option and original ROW algorithm. Four new parameter cases cover geography and
integer ROW + NULL in both orders. Flink deliberately preserves declared nested
field nullability under a nullable ROW; Rex field access becomes nullable. The
tests verify these two boundaries independently.

| Focused class | Passed |
| --- | ---: |
| FlinkTypeFactoryTest | 44 |
| BinaryGeographyDataTest | 25 |
| GeographyTypeTest | 2 |
| ParquetRowDataWriterTest geography cases | 4 |
| Total | **75**, zero failures/errors/skips |

Planner Spotless and Checkstyle pass. RAT was explicitly skipped in this focused
reactor. Full Flink `clean verify`, API compatibility and remote CI are not claimed.

## New same-snapshot cross-engine run

- Native Iceberg format v3 snapshot **7878252839025090724**; one Parquet data file,
  **47 rows**, real non-NULL and NULL geography values.
- Warehouse:
  `gs://healthy-skill-501608-t7-g11-geography-20261123/g11/20261123_130455/warehouse/review20261125a`.
- Registration URI: that warehouse plus
  `/g11/fixture_matrix/metadata/v2.metadata.json`.
- BigQuery table:
  `healthy-skill-501608-t7.akvelon.g11_geography_20261123_130455_review20261125`.
- Iceberg PR #1 source remains `94de0342ff408d591bcdf872bdfd430f431629a6`;
  its Flink 2.1 runtime artifact was built previously and reused. Flink's fixture
  Parquet uses 1.16.0; the Iceberg table uses shaded Parquet 1.17.1.

All 47 fixtures preserve Flink/Iceberg WKB bytes, byte order, NULL positions,
EMPTY state, shape, arrays, nested rows, CASE/typed NULL and structural accessors.
Native Iceberg types and Parquet geography annotations are checked at scalar,
array-element and struct-field leaves. A new TableEnvironment performs readback.

BigQuery sees native GEOGRAPHY, ARRAY<GEOGRAPHY> and struct geography fields.
All scalar, nested and array-element geographic-content checks pass. Counts are
47 rows, one scalar SQL NULL and 22 scalar empty values. The native result query
returns 47 rows through the accepted UNNEST/STRUCT projection, including NULL
geography elements and offsets.

The accepted boundaries remain: **40 NULL lists normalize to empty arrays**;
direct raw-array export with NULL elements fails the result-format restriction;
**20 scalar shape labels normalize** while geographic content matches. The
projection fields retain native GEOGRAPHY types. Expected fixture reconstruction
is used only for comparison, not to manufacture native-read evidence.

[Per-fixture coverage](evidence/coverage-matrix.csv),
[summary and job IDs](evidence/verification-summary.json),
[Flink values/schema/snapshot](evidence/flink-result.json) and saved SQL/query
responses are included alongside [revision/test metadata](evidence/revisions.json).
Apache license metadata is added to saved JSON and to the CSV's first data row.

This remains observed behavior in the selected GCP project; Google's documented
Iceberg geography limitation is not replaced by a general support claim. The
execution is local batch with the direct planner provider. Streaming,
state/network/recovery, distribution planner isolation, compiled plans and SQL
Gateway remain outside this run. The Iceberg artifact's historical embedded Git
label caveat and the connection grant expiry, 2026-11-30T13:04:55Z, remain as
documented in G11. Cloud objects are retained for review, not automatically deleted.

## Reproduction

Use the [G11 setup and commands](../README.md#reproduce-from-source) with this
Calcite pin and a new output directory/cloud table. The focused Maven test filter
is unchanged; it includes the four new FlinkTypeFactoryTest cases automatically.
For this run the probe output directory is `target/g11/runs/review20261125a`.
Use `prepare_bigquery.py` with that run's manifest and its saved cloud resources,
then run the generated queries and `verify_bigquery.py
--accept-null-list-normalization`. Keep the immutable metadata URI from the new
manifest; do not register an earlier snapshot as new evidence.
