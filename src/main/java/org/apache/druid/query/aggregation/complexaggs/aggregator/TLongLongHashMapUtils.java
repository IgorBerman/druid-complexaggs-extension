package org.apache.druid.query.aggregation.complexaggs.aggregator;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.druid.io.ByteBufferInputStream;
import org.apache.druid.java.util.common.ISE;

import gnu.trove.map.hash.TLongLongHashMap;
import org.apache.druid.java.util.common.logger.Logger;

public class TLongLongHashMapUtils {

    private static final Logger LOG = new Logger(TLongLongHashMapUtils.class);
    public static void combineWithLong(TLongLongHashMap map, long key) {
        LOG.debug(">combineWithLong [%s] + %s", map, key);
        map.adjustOrPutValue(key, 1, 1);
        LOG.debug("<combineWithLong [%s]", map);
    }

    public static void combineWithOther(TLongLongHashMap combined, TLongLongHashMap other) {
        LOG.debug(">combineWithOther [%s] + %s", combined, other);
        if (other == null) {
            return;
        }
        other.forEachEntry((a, b) -> {
            combined.adjustOrPutValue(a, b, b);
            return true;
        });
        LOG.debug("<combineWithOther [%s]", combined);
    }

    public static TLongLongHashMap fromBuffer(ByteBuffer mutationBuffer) {
        LOG.debug(">fromBuffer [%s]", mutationBuffer);
        TLongLongHashMap map = new TLongLongHashMap();
        try (ByteBufferInputStream bbis = new ByteBufferInputStream(mutationBuffer);
             ObjectInputStream ois = new ObjectInputStream(bbis)) {
            map.readExternal(ois);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        LOG.debug("<fromBuffer [%s]", map);
        return map;
    }

    public static TLongLongHashMap fromBytes(byte[] bytes) {
        LOG.debug(">fromBytes [%s]", Arrays.toString(bytes));
        TLongLongHashMap map = new TLongLongHashMap();
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
             ObjectInputStream ois = new ObjectInputStream(bais)) {
            map.readExternal(ois);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        LOG.debug("<fromBytes [%s]", map);
        return map;
    }

    public static byte[] toBytes(TLongLongHashMap map) {
        LOG.debug(">toBytes [%s]", map);
        byte[] result = null;
        if (map == null) {
            result = new byte[]{};
        } else {
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                 ObjectOutputStream oos = new ObjectOutputStream(baos)) {
                map.writeExternal(oos);
                oos.flush();
                result = baos.toByteArray();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        LOG.debug("<toBytes [%s]", result);
        return result;

    }

    public static void combineWithObject(TLongLongHashMap combined, Object val) {
        LOG.debug(">combineWithObject [%s] + %s", combined, val);
        if (val == null) {
            return;
        } else if (val instanceof String) {
            LOG.debug("?combineWithObject [%s] + %s(string)", combined, val);
            combineWithOther(combined, fromStringSerializedForm((String) val));
        } else if (val instanceof TLongLongHashMap) {
            LOG.debug("?combineWithObject [%s] + %s(map)", combined, val);
            combineWithOther(combined, (TLongLongHashMap) val);
        } else if (val instanceof Number) {
            LOG.debug("?combineWithObject [%s] + %s(number)", combined, val);
            combineWithLong(combined, ((Number) val).longValue());
        } else if (val instanceof ArrayList) {
            LOG.debug("?combineWithObject [%s] + %s(arraylist)", combined, val);
            ArrayList<Number> list = (ArrayList) val;
            long[] longs = list.stream().mapToLong(Number::longValue).toArray();
            combineWithObject(combined, fromArray(longs));
        } else {
            throw new ISE("Unknown class for object: " + val.getClass());
        }
        LOG.debug("<combineWithObject [%s]", combined);
    }

    public static long[] toArray(TLongLongHashMap map) {
        LOG.debug(">toArray [%s]", map);
        long[] result = new long[map.size()*2];
        long[] keys = map.keys();
        Arrays.sort(keys);
        int i = 0;
        for (long key : keys) {
            result[i++] = key;
            result[i++] = map.get(key);
        }
        LOG.debug("<toArray [%s]", Arrays.toString(result));
        return result;
    }

    public static TLongLongHashMap fromArray(long[] array) {
        LOG.debug(">fromArray [%s]", Arrays.toString(array));
        int size = (array.length)/2;
        long[] keys = new long[size];
        long[] values = new long[size];
        for (int i = 0, j=0; i < array.length; j++) {
            long key = array[i++];
            long value = array[i++];
            keys[j] = key;
            values[j] = value;
        }
        TLongLongHashMap result = new TLongLongHashMap(keys, values);
        LOG.debug("<fromArray [%s]", result);
        return result;
    }

    public static String toStringSerializedForm(TLongLongHashMap map) {
        LOG.debug(">toStringSerializedForm [%s]", map);
        String result = Arrays.stream(toArray(map)).boxed().map(String::valueOf).collect(Collectors.joining(",", "[", "]"));
        LOG.debug("<toStringSerializedForm [%s]", result);
        return result;
    }

    public static TLongLongHashMap fromStringSerializedForm(String serializedAsString) {
        LOG.debug(">fromStringSerializedForm [%s]", serializedAsString);
        if (serializedAsString == null) {
            return null;
        }
        long[] serializedAsArray = Arrays.stream(serializedAsString.substring(1, serializedAsString.length() - 1).split(",")).mapToLong(Long::parseLong).toArray();
        TLongLongHashMap result = fromArray(serializedAsArray);
        LOG.debug("<fromStringSerializedForm [%s]", result);
        return result;
    }
}
