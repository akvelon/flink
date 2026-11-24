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
package org.apache.flink.geography.validation;

import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.row.ParquetRowDataBuilder;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.GeographyData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.GeographyType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.jackson.JacksonMapperFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.FlinkCatalogFactory;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.shaded.org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.iceberg.shaded.org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.iceberg.shaded.org.apache.parquet.schema.MessageType;
import org.apache.iceberg.shaded.org.apache.parquet.schema.Type;
import org.apache.iceberg.types.Types;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/** Manual G11 batch probe: source fixture, native Iceberg commit, and fresh Flink readback. */
public class G11IcebergProbe {
    private static final ObjectMapper JSON = JacksonMapperFactory.createObjectMapper();
    private static final String COLUMNS =
            "id BIGINT NOT NULL, name STRING NOT NULL, geo GEOGRAPHY, "
                    + "geos ARRAY<GEOGRAPHY>, nested ROW<label STRING, geo GEOGRAPHY>";
    private static final String TABLE = "g11.g11.fixture_matrix";

    public static void main(String[] args) throws Exception {
        if (args.length != 3 && args.length != 4) {
            throw new IllegalArgumentException(
                    "Expected fixtures.json cloud-resources.json output-dir"
                        + " [readback-manifest.json]");
        }
        final JsonNode fixtures = JSON.readTree(Paths.get(args[0]).toFile());
        final JsonNode cloud = JSON.readTree(Paths.get(args[1]).toFile());
        final java.nio.file.Path output = Paths.get(args[2]).toAbsolutePath();
        Files.createDirectories(output);
        final boolean readbackOnly = args.length == 4;
        final JsonNode previous = readbackOnly ? JSON.readTree(Paths.get(args[3]).toFile()) : null;
        final String warehouse =
                readbackOnly
                        ? previous.get("warehouse").asText()
                        : cloud.get("warehouse").asText() + "/" + output.getFileName();
        final Map<String, Object> result = new LinkedHashMap<>();
        result.put("status", "RUNNING");
        result.put("fixture_rows", fixtures.get("row_count").intValue());
        result.put("warehouse", warehouse);
        result.put("calcite_dependency", "1.41.0");
        result.put("calcite_type_class_source", location(SqlTypeName.class));
        result.put("calcite_parser_class_source", location(SqlParser.class));
        result.put("iceberg_class_source", location(HadoopCatalog.class));
        result.put("hadoop_class_source", location(org.apache.hadoop.fs.FileSystem.class));
        try {
            check(
                    SqlTypeName.valueOf("GEOGRAPHY").getName().equals("GEOGRAPHY"),
                    "Native Calcite GEOGRAPHY identity is missing");
            final Configuration conf = hadoopConfiguration(cloud.get("project").asText());
            if (!readbackOnly) {
                final Path input = writeFixture(fixtures, output);
                result.put("source_fixture", input.toString());
                final TableEnvironment writer = environment();
                writer.executeSql(
                        "CREATE TEMPORARY TABLE fixture_source ("
                                + COLUMNS
                                + ") WITH ('connector'='filesystem','format'='parquet','path'='"
                                + input.toUri()
                                + "')");
                validateFlinkSchema(
                        writer.from("fixture_source")
                                .getResolvedSchema()
                                .toPhysicalRowDataType()
                                .getLogicalType());
                registerCatalog(writer, conf, warehouse);
                writer.executeSql("CREATE DATABASE IF NOT EXISTS g11.g11");
                final String createTable =
                        "CREATE TABLE "
                                + TABLE
                                + " ("
                                + COLUMNS
                                + ") WITH ('format-version'='3','write.format.default'='parquet')";
                result.put("iceberg_ddl", createTable);
                writer.executeSql(createTable);
                writer.executeSql(
                                "INSERT INTO "
                                        + TABLE
                                        + " SELECT id, name, geo, geos, nested FROM fixture_source")
                        .await();
                result.put("flink_insert", "COMPLETED");
            } else {
                result.put("flink_insert", "REUSED_PREVIOUS_COMMITTED_SNAPSHOT");
                result.put("source_manifest", Paths.get(args[3]).toAbsolutePath().toString());
            }
            save(output, result);

            inspectSnapshot(conf, warehouse, fixtures, result);
            if (readbackOnly) {
                check(
                        ((Long) result.get("snapshot_id"))
                                == previous.get("snapshot_id").longValue(),
                        "Snapshot changed since the recorded write");
            }
            save(output, result);
            final TableEnvironment reader = environment();
            registerCatalog(reader, conf, warehouse);
            validateFlinkSchema(
                    reader.from(TABLE)
                            .getResolvedSchema()
                            .toPhysicalRowDataType()
                            .getLogicalType());
            final String query =
                    "SELECT id, name, geo, geos, nested, CASE WHEN MOD(id, 2) = 1 THEN geo ELSE"
                            + " CAST(NULL AS GEOGRAPHY) END AS derived, CAST(NULL AS GEOGRAPHY) AS"
                            + " typed_null, ST_IsEmpty(geo), ST_GeometryType(geo),"
                            + " ST_IsEmpty(nested.geo), ST_GeometryType(nested.geo) FROM "
                            + TABLE
                            + " ORDER BY id";
            result.put("fresh_session_query", query);
            final RowType projected =
                    (RowType)
                            reader.sqlQuery(query)
                                    .getResolvedSchema()
                                    .toPhysicalRowDataType()
                                    .getLogicalType();
            for (int field = 5; field <= 10; field++) {
                final LogicalTypeRoot expectedRoot =
                        field <= 6
                                ? LogicalTypeRoot.GEOGRAPHY
                                : field % 2 == 1
                                        ? LogicalTypeRoot.BOOLEAN
                                        : LogicalTypeRoot.VARCHAR;
                check(
                        projected.getTypeAt(field).getTypeRoot() == expectedRoot
                                && projected.getTypeAt(field).isNullable(),
                        "Derived identity/nullability changed at field " + field);
            }
            result.put("projected_flink_schema", projected.asSerializableString());
            final List<Map<String, Object>> actual = new ArrayList<>();
            int index = 0;
            try (CloseableIterator<Row> rows = reader.executeSql(query).collect()) {
                while (rows.hasNext()) {
                    final Row row = rows.next();
                    check(index < fixtures.get("row_count").intValue(), "Unexpected extra row");
                    final JsonNode expected = fixtures.get("records").get(index++);
                    actual.add(verifyRow(row, expected));
                }
            }
            check(index == fixtures.get("row_count").intValue(), "Row count changed on readback");
            result.put("readback_rows", index);
            result.put("actual_records", actual);
            result.put("status", "FLINK_ICEBERG_PASS_BIGQUERY_PENDING");
            save(output, result);
            System.out.println(
                    "PASS G11 native Iceberg snapshot and fresh Flink readback: "
                            + index
                            + " rows");
        } catch (Throwable error) {
            result.put("status", "FAILED");
            result.put("failure_type", error.getClass().getName());
            result.put("failure_message", error.getMessage());
            save(output, result);
            throw error;
        }
    }

    private static TableEnvironment environment() {
        final TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        env.getConfig()
                .getConfiguration()
                .setString("table.exec.resource.default-parallelism", "1");
        return env;
    }

    private static Configuration hadoopConfiguration(String project) {
        final Configuration conf = new Configuration();
        conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
        conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
        conf.set("fs.gs.project.id", project);
        String credentials = System.getenv("GOOGLE_APPLICATION_CREDENTIALS");
        if (credentials == null) {
            final String appData = System.getenv("APPDATA");
            credentials =
                    appData == null
                            ? Paths.get(
                                            System.getProperty("user.home"),
                                            ".config",
                                            "gcloud",
                                            "application_default_credentials.json")
                                    .toString()
                            : Paths.get(appData, "gcloud", "application_default_credentials.json")
                                    .toString();
        }
        check(
                Files.isRegularFile(Paths.get(credentials)),
                "Application-default credentials missing");
        conf.set("fs.gs.auth.service.account.json.keyfile", credentials);
        return conf;
    }

    private static void registerCatalog(
            TableEnvironment env, Configuration conf, String warehouse) {
        env.registerCatalog("g11", new ConfiguredCatalogFactory().configured(conf, warehouse));
    }

    private static final class ConfiguredCatalogFactory extends FlinkCatalogFactory {
        private Catalog configured(Configuration conf, String warehouse) {
            final Map<String, String> properties = new HashMap<>();
            properties.put("catalog-type", "hadoop");
            properties.put("warehouse", warehouse);
            properties.put("default-database", "g11");
            properties.put("cache-enabled", "false");
            return createCatalog("g11", properties, conf);
        }
    }

    private static Path writeFixture(JsonNode fixtures, java.nio.file.Path output)
            throws Exception {
        final RowType nested =
                RowType.of(
                        new LogicalType[] {
                            new VarCharType(VarCharType.MAX_LENGTH), new GeographyType()
                        },
                        new String[] {"label", "geo"});
        final RowType type =
                RowType.of(
                        new LogicalType[] {
                            new BigIntType(false),
                            new VarCharType(false, VarCharType.MAX_LENGTH),
                            new GeographyType(),
                            new ArrayType(new GeographyType()),
                            nested
                        },
                        new String[] {"id", "name", "geo", "geos", "nested"});
        final Path path = new Path(output.resolve("fixture-source.parquet").toUri());
        try (FSDataOutputStream stream =
                path.getFileSystem().create(path, FileSystem.WriteMode.NO_OVERWRITE)) {
            final BulkWriter<RowData> writer =
                    ParquetRowDataBuilder.createWriterFactory(type, new Configuration(), true)
                            .create(stream);
            for (JsonNode row : fixtures.get("records")) {
                GenericArrayData array = null;
                if (!row.get("geos_wkb").isNull()) {
                    final Object[] elements = new Object[row.get("geos_wkb").size()];
                    for (int i = 0; i < elements.length; i++) {
                        elements[i] = geography(row.get("geos_wkb").get(i));
                    }
                    array = new GenericArrayData(elements);
                }
                GenericRowData structure = null;
                if (!row.get("nested").isNull()) {
                    structure =
                            GenericRowData.of(
                                    StringData.fromString(row.get("nested").get("label").asText()),
                                    geography(row.get("nested").get("geo_wkb")));
                }
                writer.addElement(
                        GenericRowData.of(
                                row.get("id").longValue(),
                                StringData.fromString(row.get("name").asText()),
                                geography(row.get("geo_wkb")),
                                array,
                                structure));
            }
            writer.finish();
        }
        return path;
    }

    private static GeographyData geography(JsonNode value) {
        return value.isNull()
                ? null
                : GeographyData.fromBytes(StringUtils.hexStringToByte(value.asText()));
    }

    private static void validateFlinkSchema(LogicalType logicalType) {
        final RowType row = (RowType) logicalType;
        check(row.getTypeAt(2).getTypeRoot() == LogicalTypeRoot.GEOGRAPHY, "Scalar identity lost");
        check(row.getTypeAt(2).isNullable(), "Scalar nullability lost");
        final ArrayType array = (ArrayType) row.getTypeAt(3);
        check(
                array.isNullable()
                        && array.getElementType().isNullable()
                        && array.getElementType().getTypeRoot() == LogicalTypeRoot.GEOGRAPHY,
                "Array geography identity/nullability lost");
        final RowType nested = (RowType) row.getTypeAt(4);
        check(
                nested.isNullable()
                        && nested.getTypeAt(1).isNullable()
                        && nested.getTypeAt(1).getTypeRoot() == LogicalTypeRoot.GEOGRAPHY,
                "Nested geography identity/nullability lost");
    }

    private static void inspectSnapshot(
            Configuration conf, String warehouse, JsonNode fixtures, Map<String, Object> result)
            throws Exception {
        try (HadoopCatalog catalog = new HadoopCatalog(conf, warehouse)) {
            final Table table = catalog.loadTable(TableIdentifier.of("g11", "fixture_matrix"));
            check(table.currentSnapshot() != null, "Iceberg snapshot was not committed");
            result.put("snapshot_id", table.currentSnapshot().snapshotId());
            result.put(
                    "metadata_location",
                    ((BaseTable) table).operations().current().metadataFileLocation());
            result.put("iceberg_schema", SchemaParser.toJson(table.schema()));
            result.put("snapshot_summary", table.currentSnapshot().summary());
            check(
                    table.schema().findType("geo").typeId().name().equals("GEOGRAPHY"),
                    "Iceberg scalar is not native GEOGRAPHY");
            final Types.ListType list = table.schema().findType("geos").asListType();
            check(
                    list.elementType().typeId().name().equals("GEOGRAPHY")
                            && list.isElementOptional(),
                    "Iceberg list leaf mapping changed");
            check(
                    table.schema().findField("geo").isOptional()
                            && table.schema().findField("geos").isOptional()
                            && table.schema().findField("nested.geo").isOptional(),
                    "Iceberg optional fields changed");
            final List<Map<String, Object>> files = new ArrayList<>();
            long count = 0;
            try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
                for (FileScanTask task : tasks) {
                    final String path = task.file().path().toString();
                    check(path.startsWith("gs://"), "Data file is not shared cloud storage");
                    final Map<String, Object> file = new LinkedHashMap<>();
                    file.put("path", path);
                    file.put("record_count", task.file().recordCount());
                    count += task.file().recordCount();
                    try (ParquetFileReader reader =
                            ParquetFileReader.open(
                                    HadoopInputFile.fromPath(
                                            new org.apache.hadoop.fs.Path(path), conf))) {
                        final MessageType schema = reader.getFileMetaData().getSchema();
                        file.put("parquet_schema", schema.toString());
                        checkGeographyAnnotation(schema.getType("geo"));
                        checkGeographyAnnotation(
                                schema.getType("geos")
                                        .asGroupType()
                                        .getType("list")
                                        .asGroupType()
                                        .getType("element"));
                        checkGeographyAnnotation(
                                schema.getType("nested").asGroupType().getType("geo"));
                    }
                    files.add(file);
                }
            }
            check(count == fixtures.get("row_count").longValue(), "Snapshot row count changed");
            result.put("data_files", files);
        }
    }

    private static void checkGeographyAnnotation(Type type) {
        final Object annotation = type.asPrimitiveType().getLogicalTypeAnnotation();
        check(
                annotation != null
                        && annotation
                                .getClass()
                                .getSimpleName()
                                .equals("GeographyLogicalTypeAnnotation"),
                "Native Parquet annotation missing");
    }

    private static Map<String, Object> verifyRow(Row row, JsonNode expected) {
        final Map<String, Object> actual = new LinkedHashMap<>();
        actual.put("id", row.getField(0));
        actual.put("name", row.getField(1));
        actual.put("geo_wkb", hex(row.getField(2)));
        List<String> array = null;
        if (row.getField(3) != null) {
            array = new ArrayList<>();
            for (Object item : (Object[]) row.getField(3)) {
                array.add(hex(item));
            }
        }
        actual.put("geos_wkb", array);
        Map<String, Object> nested = null;
        if (row.getField(4) != null) {
            final Row value = (Row) row.getField(4);
            nested = new LinkedHashMap<>();
            nested.put("label", value.getField(0));
            nested.put("geo_wkb", hex(value.getField(1)));
        }
        actual.put("nested", nested);
        actual.put("expected_case_geo_wkb", hex(row.getField(5)));
        actual.put("expected_empty", row.getField(7));
        actual.put("expected_shape", row.getField(8));
        check(row.getField(6) == null, "Typed GEOGRAPHY NULL changed");
        final JsonNode actualJson = JSON.valueToTree(actual);
        for (String field : actual.keySet()) {
            check(
                    field.equals("id")
                            ? actualJson.get(field).longValue() == expected.get(field).longValue()
                            : Objects.equals(actualJson.get(field), expected.get(field)),
                    "Readback mismatch at row "
                            + expected.get("id")
                            + ", field "
                            + field
                            + ": "
                            + actualJson.get(field)
                            + " versus "
                            + expected.get(field));
        }
        final boolean nullLeaf =
                expected.get("nested").isNull() || expected.get("nested").get("geo_wkb").isNull();
        check(
                Objects.equals(
                        row.getField(9),
                        nullLeaf
                                ? null
                                : JSON.convertValue(expected.get("expected_empty"), Boolean.class)),
                "Nested ST_IsEmpty changed");
        check(
                Objects.equals(
                        row.getField(10),
                        nullLeaf ? null : expected.get("expected_shape").asText()),
                "Nested ST_GeometryType changed");
        actual.put("status", "PASS");
        return actual;
    }

    private static String hex(Object value) {
        if (value == null) {
            return null;
        }
        check(value instanceof GeographyData, "Expected GeographyData runtime representation");
        return StringUtils.byteToHexString(((GeographyData) value).toBytes())
                .toUpperCase(Locale.ROOT);
    }

    private static String location(Class<?> type) {
        return type.getProtectionDomain().getCodeSource().getLocation().toString();
    }

    private static void save(java.nio.file.Path output, Map<String, Object> result)
            throws Exception {
        JSON.writerWithDefaultPrettyPrinter()
                .writeValue(output.resolve("flink-iceberg-result.json").toFile(), result);
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }
}
