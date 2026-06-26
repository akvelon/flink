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

import org.apache.flink.table.data.GeographyData;
import org.apache.flink.table.planner.factories.utils.TestCollectionTableFactory;
import org.apache.flink.table.planner.runtime.utils.BatchTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

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
}
