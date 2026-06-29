/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.runtime.batch.sql;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.data.GeographyData;
import org.apache.flink.table.planner.factories.utils.TestCollectionTableFactory;
import org.apache.flink.table.planner.runtime.utils.BatchTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.callSql;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** End-to-end SQL coverage for GEOGRAPHY values. */
class GeographySqlITCase extends BatchTestBase {

    @Test
    void testGeographySqlInsertAndRoundTripAccessors() throws Exception {
        final List<Row> sourceData =
                Arrays.asList(
                        Row.of(1L, "POINT (1 2)"),
                        Row.of(2L, "LINESTRING (0 0, 1 1)"),
                        Row.of(3L, null));

        TestCollectionTableFactory.reset();
        TestCollectionTableFactory.initData(sourceData);

        tEnv().executeSql(
                        "CREATE TABLE GeographySource(id BIGINT, wkt STRING) WITH "
                                + "('connector' = 'COLLECTION')")
                .await();
        tEnv().executeSql(
                        "CREATE TABLE GeographySink("
                                + "id BIGINT, "
                                + "geog GEOGRAPHY, "
                                + "wkt STRING, "
                                + "roundtrip STRING, "
                                + "wkb BYTES"
                                + ") WITH ('connector' = 'COLLECTION')")
                .await();

        tEnv().executeSql(
                        "INSERT INTO GeographySink "
                                + "SELECT id, "
                                + "ST_GEOGFROMTEXT(wkt), "
                                + "ST_ASTEXT(ST_GEOGFROMTEXT(wkt)), "
                                + "ST_ASTEXT(ST_GEOGFROMWKB(ST_ASWKB(ST_GEOGFROMTEXT(wkt)))), "
                                + "ST_ASWKB(ST_GEOGFROMTEXT(wkt)) "
                                + "FROM GeographySource")
                .await();

        final List<Row> sinkRows = TestCollectionTableFactory.getResult();
        assertThat(sinkRows).hasSize(3);

        assertThat(sinkRows.get(0).getField(0)).isEqualTo(1L);
        assertThat(((GeographyData) sinkRows.get(0).getField(1)).toBytes())
                .isEqualTo((byte[]) sinkRows.get(0).getField(4));
        assertThat(sinkRows.get(0).getField(2)).isEqualTo("POINT (1 2)");
        assertThat(sinkRows.get(0).getField(3)).isEqualTo("POINT (1 2)");

        assertThat(sinkRows.get(1).getField(0)).isEqualTo(2L);
        assertThat(((GeographyData) sinkRows.get(1).getField(1)).toBytes())
                .isEqualTo((byte[]) sinkRows.get(1).getField(4));
        assertThat(sinkRows.get(1).getField(2)).isEqualTo("LINESTRING (0 0, 1 1)");
        assertThat(sinkRows.get(1).getField(3)).isEqualTo("LINESTRING (0 0, 1 1)");

        assertThat(sinkRows.get(2).getField(0)).isEqualTo(3L);
        assertThat(sinkRows.get(2).getField(1)).isNull();
        assertThat(sinkRows.get(2).getField(2)).isNull();
        assertThat(sinkRows.get(2).getField(3)).isNull();
        assertThat(sinkRows.get(2).getField(4)).isNull();

        final List<Row> selectedRows =
                CollectionUtil.iteratorToList(
                        tEnv().executeSql(
                                        "SELECT id, "
                                                + "ST_ASTEXT(geog), "
                                                + "OCTET_LENGTH(ST_ASWKB(geog)), "
                                                + "ST_ASTEXT(ST_GEOGFROMWKB(ST_ASWKB(geog))) "
                                                + "FROM ("
                                                + "SELECT id, ST_GEOGFROMTEXT(wkt) AS geog "
                                                + "FROM GeographySource) "
                                                + "WHERE geog IS NOT NULL "
                                                + "ORDER BY id")
                                .collect());

        assertThat(selectedRows)
                .containsExactly(
                        Row.of(1L, "POINT (1 2)", 21, "POINT (1 2)"),
                        Row.of(2L, "LINESTRING (0 0, 1 1)", 41, "LINESTRING (0 0, 1 1)"));
    }

    @Test
    void testSpatialPredicatesAndMeasurementsRemainUnavailableInV1() {
        final String distanceSql =
                "SELECT ST_DISTANCE(ST_GEOGFROMTEXT('POINT (0 0)'), "
                        + "ST_GEOGFROMTEXT('POINT (1 1)'))";
        final String intersectsSql =
                "SELECT ST_INTERSECTS(ST_GEOGFROMTEXT('POINT (0 0)'), "
                        + "ST_GEOGFROMTEXT('POINT (1 1)'))";

        assertThatThrownBy(() -> tEnv().executeSql(distanceSql))
                .hasMessageContaining("ST_DISTANCE");
        assertThatThrownBy(() -> tEnv().executeSql(intersectsSql))
                .hasMessageContaining("ST_INTERSECTS");
    }

    @Test
    void testMixedGeographySchemaAcrossSqlAndTableRuntime() throws Exception {
        TestCollectionTableFactory.reset();

        tEnv().executeSql(
                        "CREATE TEMPORARY VIEW MixedGeographyInput AS "
                                + "SELECT * FROM (VALUES "
                                + "(CAST(1 AS BIGINT), 'alpha', TRUE, "
                                + "TIMESTAMP '2024-01-02 03:04:05.123', "
                                + "CAST(NULL AS STRING), "
                                + "ST_GEOGFROMTEXT('POINT (1 2)')), "
                                + "(CAST(2 AS BIGINT), 'beta', FALSE, "
                                + "TIMESTAMP '2024-02-03 04:05:06.789', "
                                + "'second row', "
                                + "ST_GEOGFROMWKB(ST_ASWKB(ST_GEOGFROMTEXT('LINESTRING (0 0, 1 1)')))), "
                                + "(CAST(3 AS BIGINT), CAST(NULL AS STRING), CAST(NULL AS BOOLEAN), "
                                + "CAST(NULL AS TIMESTAMP(3)), "
                                + "'missing geography', CAST(NULL AS GEOGRAPHY))"
                                + ") AS T(id, name, active, event_ts, note, geog)")
                .await();
        tEnv().executeSql(
                        "CREATE TABLE MixedGeographySink ("
                                + "id BIGINT, "
                                + "name STRING, "
                                + "active BOOLEAN, "
                                + "event_ts TIMESTAMP(3), "
                                + "note STRING, "
                                + "geog GEOGRAPHY, "
                                + "geog_text STRING, "
                                + "geog_wkb BYTES"
                                + ") WITH ('connector' = 'COLLECTION')")
                .await();

        tEnv().executeSql(
                        "INSERT INTO MixedGeographySink "
                                + "SELECT id, name, active, event_ts, note, geog, "
                                + "ST_ASTEXT(geog), ST_ASWKB(geog) "
                                + "FROM MixedGeographyInput")
                .await();

        final List<Row> sinkRows = TestCollectionTableFactory.getResult();
        sinkRows.sort(Comparator.comparing(row -> (Long) row.getField(0)));

        assertThat(sinkRows).hasSize(3);

        assertThat(sinkRows.get(0))
                .extracting(
                        row -> row.getField(0),
                        row -> row.getField(1),
                        row -> row.getField(2),
                        row -> String.valueOf(row.getField(3)),
                        row -> row.getField(4),
                        row -> row.getField(6))
                .containsExactly(1L, "alpha", true, "2024-01-02T03:04:05.123", null, "POINT (1 2)");
        assertThat(((GeographyData) sinkRows.get(0).getField(5)).toBytes())
                .isEqualTo((byte[]) sinkRows.get(0).getField(7));

        assertThat(sinkRows.get(1))
                .extracting(
                        row -> row.getField(0),
                        row -> row.getField(1),
                        row -> row.getField(2),
                        row -> String.valueOf(row.getField(3)),
                        row -> row.getField(4),
                        row -> row.getField(6))
                .containsExactly(
                        2L,
                        "beta",
                        false,
                        "2024-02-03T04:05:06.789",
                        "second row",
                        "LINESTRING (0 0, 1 1)");
        assertThat(((GeographyData) sinkRows.get(1).getField(5)).toBytes())
                .isEqualTo((byte[]) sinkRows.get(1).getField(7));

        assertThat(sinkRows.get(2))
                .extracting(
                        row -> row.getField(0),
                        row -> row.getField(1),
                        row -> row.getField(2),
                        row -> row.getField(3),
                        row -> row.getField(4),
                        row -> row.getField(5),
                        row -> row.getField(6),
                        row -> row.getField(7))
                .containsExactly(3L, null, null, null, "missing geography", null, null, null);

        final Table projectedTable =
                tEnv().from("MixedGeographyInput")
                        .select(
                                $("id"),
                                $("name"),
                                $("active"),
                                callSql("CAST(event_ts AS STRING)"),
                                $("note"),
                                callSql("ST_ASTEXT(geog)"),
                                callSql("OCTET_LENGTH(ST_ASWKB(geog))"));

        final List<Row> projectedRows =
                CollectionUtil.iteratorToList(projectedTable.execute().collect());
        projectedRows.sort(Comparator.comparing(row -> (Long) row.getField(0)));

        assertThat(projectedRows)
                .containsExactly(
                        Row.of(
                                1L,
                                "alpha",
                                true,
                                "2024-01-02 03:04:05.123",
                                null,
                                "POINT (1 2)",
                                21),
                        Row.of(
                                2L,
                                "beta",
                                false,
                                "2024-02-03 04:05:06.789",
                                "second row",
                                "LINESTRING (0 0, 1 1)",
                                41),
                        Row.of(3L, null, null, null, "missing geography", null, null));
    }
}
