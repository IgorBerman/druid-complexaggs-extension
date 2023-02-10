package org.apache.druid.query.aggregation.complexaggs;

import java.io.IOException;
import java.util.Arrays;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import gnu.trove.map.hash.TLongLongHashMap;

/**
 * Serializes key and values of map to consistent form by sorted keys first and then getting value
 * Probably might be done faster with working with key,value pair and implementing Comparator
 */
public class TLongLongHashMapOrderedSerializer extends StdSerializer<TLongLongHashMap> {
    public TLongLongHashMapOrderedSerializer() {
        super(TLongLongHashMap.class);
    }

    @Override
    public void serialize(TLongLongHashMap map, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
            throws IOException {

        long[] keys = map.keys();
        Arrays.sort(keys);

        jsonGenerator.writeArray(keys, 0, keys.length);
        long[] values = new long[map.size()];
        int i = 0;
        for (long key : keys) {
            values[i++] = map.get(key);
        }
        jsonGenerator.writeArray(values, 0, values.length);
    }
}
