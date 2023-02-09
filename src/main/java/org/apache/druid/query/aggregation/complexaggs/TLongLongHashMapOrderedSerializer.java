package org.apache.druid.query.aggregation.complexaggs;

import java.io.IOException;
import java.util.Arrays;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import gnu.trove.map.hash.TLongLongHashMap;

public class TLongLongHashMapOrderedSerializer extends StdSerializer<TLongLongHashMap> {
    public TLongLongHashMapOrderedSerializer() {
        super(TLongLongHashMap.class);
    }

    @Override
    public void serialize(TLongLongHashMap map, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
            throws IOException {

        long[][] kvs = new long[map.size()][2];

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
