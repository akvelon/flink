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

package org.apache.flink.table.data.binary;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.api.TableRuntimeException;
import org.apache.flink.table.data.GeographyData;

import javax.annotation.Nullable;

import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * A binary implementation of {@link GeographyData} backed by raw ISO WKB bytes.
 *
 * <p>GEOGRAPHY uses OGC:CRS84 by contract, but ISO WKB does not encode CRS or SRID metadata. This
 * container stores the raw ISO WKB payload only; CRS validation, CRS transformation, and EWKB/SRID
 * handling belong to constructors, functions, and connector schema mapping.
 *
 * <p>{@link #fromBytes(byte[], int, int)} is the validating, copying construction path. The
 * internal {@link #fromAddress(MemorySegment[], int, int)} path creates a lazy view over trusted
 * binary storage without validating the full WKB payload or taking ownership of the backing
 * segments. Callers using the lazy path must preserve the backing storage for the lifetime of the
 * view.
 */
@Internal
public final class BinaryGeographyData extends BinarySection implements GeographyData {

    private static final int MIN_WKB_HEADER_SIZE = 5;
    private static final int WKB_COUNT_SIZE = 4;
    private static final int WKB_POINT_COORDINATE_SIZE = 16;
    private static final int BIG_ENDIAN = 0;
    private static final int LITTLE_ENDIAN = 1;
    private static final int NATIVE_BYTE_ORDER =
            ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN ? LITTLE_ENDIAN : BIG_ENDIAN;
    private static final int UNKNOWN_SUBTYPE_ID = 0;
    private static final long MAX_INT_ADDRESS_EXCLUSIVE = (long) Integer.MAX_VALUE + 1;

    private int subtypeId = UNKNOWN_SUBTYPE_ID;

    private BinaryGeographyData(MemorySegment[] segments, int offset, int sizeInBytes) {
        super(segments, offset, sizeInBytes);
    }

    /**
     * Creates a {@link BinaryGeographyData} instance from the given address and length without
     * validating the full ISO WKB payload.
     */
    public static BinaryGeographyData fromAddress(
            MemorySegment[] segments, int offset, int numBytes) {
        requireAddressableRange(offset, numBytes, (long) offset + numBytes, "GEOGRAPHY section");
        return new BinaryGeographyData(segments, offset, numBytes);
    }

    /**
     * Creates a {@link BinaryGeographyData} view over a trusted internal ISO WKB byte range without
     * validating the payload.
     *
     * <p>This method is zero-copy with respect to the WKB payload and aliases the provided backing
     * array. It is intended only for trusted internal binary storage. Callers must keep the backing
     * array unchanged for the lifetime of the returned value.
     */
    public static BinaryGeographyData fromTrustedBytes(byte[] bytes, int offset, int numBytes) {
        if (bytes == null) {
            return null;
        }
        checkRange(bytes, offset, numBytes);
        return fromAddress(
                new MemorySegment[] {MemorySegmentFactory.wrap(bytes)}, offset, numBytes);
    }

    /** Creates a {@link BinaryGeographyData} instance from the given ISO WKB bytes. */
    public static BinaryGeographyData fromBytes(byte[] bytes) {
        return bytes == null ? null : fromBytes(bytes, 0, bytes.length);
    }

    /**
     * Creates a {@link BinaryGeographyData} instance from the given ISO WKB byte range after
     * validating the full payload.
     */
    public static BinaryGeographyData fromBytes(byte[] bytes, int offset, int numBytes) {
        if (bytes == null) {
            return null;
        }
        checkRange(bytes, offset, numBytes);
        walkPayload(new MemorySegment[] {MemorySegmentFactory.wrap(bytes)}, offset, numBytes, null);
        byte[] copy = Arrays.copyOfRange(bytes, offset, offset + numBytes);
        return fromAddress(new MemorySegment[] {MemorySegmentFactory.wrap(copy)}, 0, copy.length);
    }

    @Override
    public byte[] toBytes() {
        return BinarySegmentUtils.copyToBytes(segments, offset, sizeInBytes);
    }

    @Override
    public int subtypeId() {
        if (subtypeId == UNKNOWN_SUBTYPE_ID) {
            subtypeId = readSubtypeId(segments, offset, sizeInBytes);
        }
        return subtypeId;
    }

    @Override
    public int sizeInBytes() {
        return sizeInBytes;
    }

    /** Returns whether the geography contains no points, including inside collections. */
    public static boolean isEmpty(GeographyData geography) {
        final Emptiness emptiness = new Emptiness();
        if (geography instanceof BinaryGeographyData) {
            final BinaryGeographyData binary = (BinaryGeographyData) geography;
            walkPayload(binary.segments, binary.offset, binary.sizeInBytes, emptiness);
        } else {
            final byte[] bytes = geography.toBytes();
            walkPayload(
                    new MemorySegment[] {MemorySegmentFactory.wrap(bytes)},
                    0,
                    bytes.length,
                    emptiness);
        }
        return emptiness.empty;
    }

    private static void checkRange(byte[] bytes, int offset, int numBytes) {
        if (offset < 0 || numBytes < 0 || offset > bytes.length - numBytes) {
            throw new TableRuntimeException(
                    String.format(
                            "Invalid ISO WKB byte range: offset %d, length %d, array length %d.",
                            offset, numBytes, bytes.length));
        }
    }

    private static void walkPayload(
            MemorySegment[] segments, int offset, int sizeInBytes, @Nullable Emptiness emptiness) {
        // WKB stores unsigned 32-bit counts, so parser cursors stay long to avoid overflow. Flink's
        // binary memory model is still addressed by signed ints at the actual read boundary.
        final long endOffset = (long) offset + sizeInBytes;
        final GeometryHeader header = readHeader(segments, offset, endOffset);
        final long consumedOffset = walkGeometry(segments, offset, endOffset, header, emptiness);
        if (consumedOffset != endOffset) {
            throw new TableRuntimeException(
                    String.format(
                            "Malformed ISO WKB payload. Found %d trailing byte(s).",
                            endOffset - consumedOffset));
        }
    }

    private static int readSubtypeId(MemorySegment[] segments, int offset, int sizeInBytes) {
        final long endOffset = (long) offset + sizeInBytes;
        return readHeader(segments, offset, endOffset).subtypeId;
    }

    private static long walkGeometry(
            MemorySegment[] segments,
            long geometryOffset,
            long endOffset,
            GeometryHeader header,
            @Nullable Emptiness emptiness) {
        final long cursor = geometryOffset + MIN_WKB_HEADER_SIZE;

        switch (header.subtypeId) {
            case GeographyData.POINT:
                requireBytes(cursor, WKB_POINT_COORDINATE_SIZE, endOffset, "POINT coordinates");
                if (emptiness != null && emptiness.empty) {
                    final int address =
                            checkedAddress(
                                    cursor,
                                    WKB_POINT_COORDINATE_SIZE,
                                    endOffset,
                                    "POINT coordinates");
                    emptiness.empty =
                            isNaNCoordinate(segments, address, header.byteOrder)
                                    || isNaNCoordinate(
                                            segments, address + Long.BYTES, header.byteOrder);
                }
                return cursor + WKB_POINT_COORDINATE_SIZE;
            case GeographyData.LINE_STRING:
                return skipCoordinateSequence(
                        segments, cursor, endOffset, header.byteOrder, emptiness);
            case GeographyData.POLYGON:
                return skipPolygon(segments, cursor, endOffset, header.byteOrder, emptiness);
            case GeographyData.MULTI_POINT:
                return skipGeometryCollection(
                        segments,
                        cursor,
                        endOffset,
                        header.byteOrder,
                        GeographyData.POINT,
                        emptiness);
            case GeographyData.MULTI_LINE_STRING:
                return skipGeometryCollection(
                        segments,
                        cursor,
                        endOffset,
                        header.byteOrder,
                        GeographyData.LINE_STRING,
                        emptiness);
            case GeographyData.MULTI_POLYGON:
                return skipGeometryCollection(
                        segments,
                        cursor,
                        endOffset,
                        header.byteOrder,
                        GeographyData.POLYGON,
                        emptiness);
            case GeographyData.GEOMETRY_COLLECTION:
                return skipGeometryCollection(
                        segments,
                        cursor,
                        endOffset,
                        header.byteOrder,
                        UNKNOWN_SUBTYPE_ID,
                        emptiness);
            default:
                throw new TableRuntimeException(
                        String.format(
                                "Malformed ISO WKB payload. Unsupported geography subtype ID %d.",
                                header.subtypeId));
        }
    }

    private static boolean isNaNCoordinate(MemorySegment[] segments, int offset, int byteOrder) {
        final long nativeBits = BinarySegmentUtils.getLong(segments, offset);
        final long bits =
                byteOrder == NATIVE_BYTE_ORDER ? nativeBits : Long.reverseBytes(nativeBits);
        return Double.isNaN(Double.longBitsToDouble(bits));
    }

    private static long skipCoordinateSequence(
            MemorySegment[] segments,
            long offset,
            long endOffset,
            int byteOrder,
            @Nullable Emptiness emptiness) {
        final long numPoints =
                readUnsignedInt(segments, offset, endOffset, byteOrder, "point count");
        long cursor = offset + WKB_COUNT_SIZE;
        final long coordinateSequenceSize =
                multiplyExact(numPoints, WKB_POINT_COORDINATE_SIZE, "coordinate sequence");
        if (emptiness != null && numPoints > 0) {
            emptiness.empty = false;
        }
        return requireBytes(cursor, coordinateSequenceSize, endOffset, "coordinate sequence")
                + coordinateSequenceSize;
    }

    private static long skipPolygon(
            MemorySegment[] segments,
            long offset,
            long endOffset,
            int byteOrder,
            @Nullable Emptiness emptiness) {
        final long numRings = readUnsignedInt(segments, offset, endOffset, byteOrder, "ring count");
        long cursor = offset + WKB_COUNT_SIZE;
        for (long i = 0; i < numRings; i++) {
            // The first ring is the polygon shell, which determines emptiness.
            cursor =
                    skipCoordinateSequence(
                            segments, cursor, endOffset, byteOrder, i == 0 ? emptiness : null);
        }
        return cursor;
    }

    private static long skipGeometryCollection(
            MemorySegment[] segments,
            long offset,
            long endOffset,
            int byteOrder,
            int expectedSubtypeId,
            @Nullable Emptiness emptiness) {
        final long numGeometries =
                readUnsignedInt(segments, offset, endOffset, byteOrder, "geometry count");
        long cursor = offset + WKB_COUNT_SIZE;
        for (long i = 0; i < numGeometries; i++) {
            final GeometryHeader nestedHeader = readHeader(segments, cursor, endOffset);
            if (expectedSubtypeId != UNKNOWN_SUBTYPE_ID
                    && nestedHeader.subtypeId != expectedSubtypeId) {
                throw new TableRuntimeException(
                        String.format(
                                "Malformed ISO WKB payload. Expected nested subtype ID %d but found %d.",
                                expectedSubtypeId, nestedHeader.subtypeId));
            }
            cursor = walkGeometry(segments, cursor, endOffset, nestedHeader, emptiness);
        }
        return cursor;
    }

    private static GeometryHeader readHeader(
            MemorySegment[] segments, long offset, long endOffset) {
        final int address = checkedAddress(offset, MIN_WKB_HEADER_SIZE, endOffset, "WKB header");

        final int byteOrder = BinarySegmentUtils.getByte(segments, address) & 0xFF;
        if (byteOrder != BIG_ENDIAN && byteOrder != LITTLE_ENDIAN) {
            throw new TableRuntimeException(
                    String.format(
                            "Malformed ISO WKB payload. Unsupported byte order %d.", byteOrder));
        }

        final long subtypeId =
                readUnsignedInt(segments, offset + 1, endOffset, byteOrder, "subtype ID");

        if (subtypeId < GeographyData.POINT || subtypeId > GeographyData.GEOMETRY_COLLECTION) {
            throw new TableRuntimeException(
                    String.format(
                            "Malformed ISO WKB payload. Unsupported geography subtype ID %d.",
                            subtypeId));
        }
        return new GeometryHeader(byteOrder, (int) subtypeId);
    }

    private static long readUnsignedInt(
            MemorySegment[] segments,
            long offset,
            long endOffset,
            int byteOrder,
            String fieldName) {
        final int address = checkedAddress(offset, WKB_COUNT_SIZE, endOffset, fieldName);

        if (byteOrder == LITTLE_ENDIAN) {
            return (BinarySegmentUtils.getByte(segments, address) & 0xFFL)
                    | ((BinarySegmentUtils.getByte(segments, address + 1) & 0xFFL) << 8)
                    | ((BinarySegmentUtils.getByte(segments, address + 2) & 0xFFL) << 16)
                    | ((BinarySegmentUtils.getByte(segments, address + 3) & 0xFFL) << 24);
        }
        return ((BinarySegmentUtils.getByte(segments, address) & 0xFFL) << 24)
                | ((BinarySegmentUtils.getByte(segments, address + 1) & 0xFFL) << 16)
                | ((BinarySegmentUtils.getByte(segments, address + 2) & 0xFFL) << 8)
                | (BinarySegmentUtils.getByte(segments, address + 3) & 0xFFL);
    }

    private static int checkedAddress(
            long offset, long numBytes, long endOffset, String componentName) {
        requireAddressableRange(offset, numBytes, endOffset, componentName);
        requireBytes(offset, numBytes, endOffset, componentName);
        return (int) offset;
    }

    private static void requireAddressableRange(
            long offset, long numBytes, long endOffset, String componentName) {
        if (offset < 0
                || numBytes < 0
                || endOffset < 0
                || offset > endOffset
                || offset > Integer.MAX_VALUE
                || endOffset > MAX_INT_ADDRESS_EXCLUSIVE) {
            throw new TableRuntimeException(
                    String.format(
                            "Unsupported GEOGRAPHY binary address range for %s: offset %d, length %d, end %d exceeds Flink's signed int memory address space.",
                            componentName, offset, numBytes, endOffset));
        }
    }

    private static long requireBytes(
            long offset, long numBytes, long endOffset, String componentName) {
        if (offset > endOffset || endOffset - offset < numBytes) {
            throw new TableRuntimeException(
                    String.format(
                            "Malformed ISO WKB payload. Incomplete %s: expected %d byte(s) but found %d.",
                            componentName, numBytes, Math.max(0, endOffset - offset)));
        }
        return offset;
    }

    private static long multiplyExact(long value, int factor, String componentName) {
        final long result = value * factor;
        if (value != 0 && result / value != factor) {
            throw new TableRuntimeException(
                    String.format("Malformed ISO WKB payload. %s size overflows.", componentName));
        }
        return result;
    }

    private static final class Emptiness {
        private boolean empty = true;
    }

    private static final class GeometryHeader {
        private final int byteOrder;
        private final int subtypeId;

        private GeometryHeader(int byteOrder, int subtypeId) {
            this.byteOrder = byteOrder;
            this.subtypeId = subtypeId;
        }
    }
}
