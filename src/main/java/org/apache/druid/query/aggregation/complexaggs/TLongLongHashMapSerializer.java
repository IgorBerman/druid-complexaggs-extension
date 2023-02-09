package org.apache.druid.query.aggregation.complexaggs;

import gnu.trove.map.hash.TLongLongHashMap;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

public class TLongLongHashMapSerializer extends StdSerializer<TLongLongHashMap> {
    public TLongLongHashMapSerializer() {
        super(TLongLongHashMap.class);
    }

    @Override
    public void serialize(TLongLongHashMap map, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
            throws IOException {

        long[] keys = map.keys();
        jsonGenerator.writeArray(keys, 0, keys.length);
        long[] values = map.values();
        jsonGenerator.writeArray(values, 0, values.length);
    }
}
