package org.apache.druid.query.aggregation.complexaggs.aggregator;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;

import org.apache.druid.io.ByteBufferInputStream;
import org.apache.druid.java.util.common.ISE;

import gnu.trove.map.hash.TLongLongHashMap;

public class TLongLongHashMapUtils {

    private static final Base64.Decoder base64Decoder = Base64.getDecoder();

    public static void combineWithLong(TLongLongHashMap map, long key) {
        map.adjustOrPutValue(key, 1, 1);
    }

    public static void combineWithOther(TLongLongHashMap combined, TLongLongHashMap other) {
        if (other == null) {
            return;
        }
        other.forEachEntry((a, b) -> {
            combined.adjustOrPutValue(a, b, b);
            return true;
        });
    }

    public static TLongLongHashMap fromBuffer(ByteBuffer mutationBuffer) {
        TLongLongHashMap map = new TLongLongHashMap();
        try (ByteBufferInputStream bbis = new ByteBufferInputStream(mutationBuffer);
             ObjectInputStream ois = new ObjectInputStream(bbis)) {
            map.readExternal(ois);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return map;
    }

    public static TLongLongHashMap fromBytes(byte[] bytes) {
        TLongLongHashMap map = new TLongLongHashMap();
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
             ObjectInputStream ois = new ObjectInputStream(bais)) {
            map.readExternal(ois);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return map;
    }

    public static byte[] toBytes(TLongLongHashMap map) {
        if (map == null) {
            return new byte[]{};
        }
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            map.writeExternal(oos);
            oos.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void combineWithObject(TLongLongHashMap combined, Object val) {
        if (val == null) {
            return;
        } else if (val instanceof String) {
            combineWithOther(combined, fromBase64((String) val));
        } else if (val instanceof TLongLongHashMap) {
            combineWithOther(combined, (TLongLongHashMap) val);
        } else if (val instanceof Number) {
            combineWithLong(combined, ((Number) val).longValue());
        } else if (val instanceof ArrayList) {
            ArrayList list = (ArrayList) val; //TODO how to test...is it array with serialized map or it's just vector of to be aggreagated column values??
            for (Object obj : list) {
                combineWithObject(combined, ((Number) obj).longValue());
            }
        } else {
            throw new ISE("Unknown class for object: " + val.getClass());
        }
    }

    public static TLongLongHashMap fromBase64(String rawValue) {
        byte[] bytes = base64Decoder.decode(rawValue.getBytes(StandardCharsets.UTF_8));
        return fromBytes(bytes);
    }

    public static void combineWithArray(TLongLongHashMap combined, ArrayList lhs) {

    }
}
