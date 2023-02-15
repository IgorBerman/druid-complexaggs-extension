package org.apache.druid.query.aggregation.complexaggs;

import gnu.trove.map.hash.TLongLongHashMap;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.PrimitiveArrayDeserializers;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

public class TLongLongHashMapDeserializer extends StdDeserializer<TLongLongHashMap> {
    public TLongLongHashMapDeserializer() {
        super(TLongLongHashMap.class);
    }

    @Override
    public TLongLongHashMap deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
            throws IOException {
        int size = jsonParser.getIntValue();

        long[] keys = new long[size];
        long[] values = new long[size];

        for (int i = 0; i < size; i++) {
            keys[i] = jsonParser.getLongValue();
            values[i] = jsonParser.getLongValue();
        }
        return new TLongLongHashMap(keys, values);
    }
}
