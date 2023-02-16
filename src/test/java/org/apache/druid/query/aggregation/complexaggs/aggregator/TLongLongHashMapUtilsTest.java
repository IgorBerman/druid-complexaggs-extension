package org.apache.druid.query.aggregation.complexaggs.aggregator;

import gnu.trove.map.hash.TLongLongHashMap;
import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;

public class TLongLongHashMapUtilsTest {

    @Test
    public void testCombineWithLong() {
        TLongLongHashMap map = new TLongLongHashMap();
        TLongLongHashMapUtils.combineWithLong(map, 1L);
        assertEquals(1, map.get(1L));
        TLongLongHashMapUtils.combineWithLong(map, 1L);
        assertEquals(2, map.get(1L));
        TLongLongHashMapUtils.combineWithLong(map, 2L);
        assertEquals(2, map.get(1L));
        assertEquals(1, map.get(2L));
        assertEquals(2, map.size());
    }

    @Test
    public void testCombineEmptyWithEmptyOther() {
        TLongLongHashMap map = new TLongLongHashMap();
        TLongLongHashMap other = new TLongLongHashMap();
        TLongLongHashMapUtils.combineWithOther(map, other);
        assertTrue(map.isEmpty());
    }

    @Test
    public void testCombineSomeWithEmptyOther() {
        TLongLongHashMap map = new TLongLongHashMap(new long[]{1,2}, new long[]{3,4});
        TLongLongHashMap other = new TLongLongHashMap();
        TLongLongHashMapUtils.combineWithOther(map, other);
        assertEquals(new TLongLongHashMap(new long[]{1,2}, new long[]{3,4}), map);
    }

    @Test
    public void testCombineSomeWithOther() {
        TLongLongHashMap map = new TLongLongHashMap(new long[]{1,2}, new long[]{3,4});
        TLongLongHashMap other = new TLongLongHashMap(new long[]{1,3}, new long[]{1,2});
        TLongLongHashMapUtils.combineWithOther(map, other);
        assertEquals(new TLongLongHashMap(new long[]{1,2,3}, new long[]{4,4,2}), map);
    }

    @Test
    public void testToBytesfromBytes() {
        TLongLongHashMap map = new TLongLongHashMap(new long[]{1,2}, new long[]{3,4});
        byte[] bytes = TLongLongHashMapUtils.toBytes(map);
        TLongLongHashMap result = TLongLongHashMapUtils.fromBytes(bytes);
        assertEquals(map, result);
    }

    @Test
    public void testFromBuffer() {
        TLongLongHashMap map = new TLongLongHashMap(new long[]{1,2}, new long[]{3,4});
        byte[] bytes = TLongLongHashMapUtils.toBytes(map);
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        TLongLongHashMap result = TLongLongHashMapUtils.fromBuffer(buffer);
        assertEquals(map, result);
    }

    @Test
    public void testCombineWithNullObject() {
        TLongLongHashMap map = new TLongLongHashMap(new long[]{1,2}, new long[]{3,4});
        TLongLongHashMapUtils.combineWithObject(map, null);
        assertEquals(new TLongLongHashMap(new long[]{1,2}, new long[]{3,4}), map);
    }

    @Test
    public void testCombineWithLongObject() {
        TLongLongHashMap map = new TLongLongHashMap(new long[]{1,2}, new long[]{3,4});
        TLongLongHashMapUtils.combineWithObject(map, 1L);
        assertEquals(new TLongLongHashMap(new long[]{1,2}, new long[]{4,4}), map);
    }

    @Test
    public void testCombineWithNewLongObject() {
        TLongLongHashMap map = new TLongLongHashMap(new long[]{1,2}, new long[]{3,4});
        TLongLongHashMapUtils.combineWithObject(map, 3L);
        assertEquals(new TLongLongHashMap(new long[]{1,2,3}, new long[]{3,4,1}), map);
    }

    @Test
    public void testCmbineWithMap() {
        TLongLongHashMap map = new TLongLongHashMap(new long[]{1,2}, new long[]{3,4});
        TLongLongHashMap other = new TLongLongHashMap(new long[]{1,3}, new long[]{1,2});
        TLongLongHashMapUtils.combineWithObject(map, other);
        assertEquals(new TLongLongHashMap(new long[]{1,2,3}, new long[]{4,4,2}), map);
    }


    @Test
    public void testCombineWithString() {
        TLongLongHashMap map = new TLongLongHashMap(new long[]{1,2}, new long[]{3,4});
        TLongLongHashMap other = new TLongLongHashMap(new long[]{1,3}, new long[]{1,2});
        TLongLongHashMapUtils.combineWithObject(map, TLongLongHashMapUtils.toStringSerializedForm(other));
        assertEquals(new TLongLongHashMap(new long[]{1,2,3}, new long[]{4,4,2}), map);
    }

    @Test
    public void testCombineWithArrayList() {
        TLongLongHashMap map = new TLongLongHashMap(new long[]{1,2}, new long[]{3,4});
        TLongLongHashMap other = new TLongLongHashMap(new long[]{1,3}, new long[]{1,2});
        List<Long> arrayList = Arrays.stream(TLongLongHashMapUtils.toArray(other)).boxed().collect(Collectors.toList());
        TLongLongHashMapUtils.combineWithObject(map, arrayList);
        assertEquals(new TLongLongHashMap(new long[]{1,2,3}, new long[]{4,4,2}), map);
    }

    @Test
    public void testToArrayFromArray() {
        TLongLongHashMap map = new TLongLongHashMap(new long[]{1,2}, new long[]{3,4});
        long[] array = TLongLongHashMapUtils.toArray(map);
        TLongLongHashMap result = TLongLongHashMapUtils.fromArray(array);
        assertEquals(map, result);
    }

    @Test
    public void testToStringSerializedFormFromStringSerializedForm() {
        TLongLongHashMap map = new TLongLongHashMap(new long[]{1,2}, new long[]{3,4});
        String mapAsStr = TLongLongHashMapUtils.toStringSerializedForm(map);
        TLongLongHashMap result = TLongLongHashMapUtils.fromStringSerializedForm(mapAsStr);
        assertEquals(map, result);
    }


    @Test
    public void testToStringSerializedFormFromStringSerializedFormEmpty() {
        TLongLongHashMap map = new TLongLongHashMap(new long[]{}, new long[]{});
        String mapAsStr = TLongLongHashMapUtils.toStringSerializedForm(map);
        TLongLongHashMap result = TLongLongHashMapUtils.fromStringSerializedForm(mapAsStr);
        assertEquals(map, result);
    }


    @Test
    public void testMultiStageCombination() {
        TLongLongHashMap map = new TLongLongHashMap();
        TLongLongHashMapUtils.combineWithLong(map, 1L);
        TLongLongHashMapUtils.combineWithLong(map, 3L);
        byte[] mapBytes = TLongLongHashMapUtils.toBytes(map);

        TLongLongHashMap map2 = new TLongLongHashMap();
        TLongLongHashMapUtils.combineWithLong(map2, 1L);
        TLongLongHashMapUtils.combineWithLong(map2, 2L);
        byte[] map2Bytes = TLongLongHashMapUtils.toBytes(map2);

        TLongLongHashMap mergedSegments = TLongLongHashMapUtils.fromBytes(mapBytes);
        TLongLongHashMapUtils.combineWithObject(mergedSegments, TLongLongHashMapUtils.fromBytes(map2Bytes));


        String mergedSegmentsStringForm = TLongLongHashMapUtils.toStringSerializedForm(mergedSegments);

        TLongLongHashMap map3 = new TLongLongHashMap();
        TLongLongHashMapUtils.combineWithLong(map3, 3L);
        TLongLongHashMapUtils.combineWithLong(map3, 4L);
        String map3StringForm = TLongLongHashMapUtils.toStringSerializedForm(map3);


        TLongLongHashMap combined = new TLongLongHashMap();
        TLongLongHashMapUtils.combineWithObject(combined, mergedSegmentsStringForm);
        TLongLongHashMapUtils.combineWithObject(combined, map3StringForm);

        assertEquals(new TLongLongHashMap(new long[]{1,2,3,4}, new long[]{2,1,2,1}), combined);

    }
}