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
        PrimitiveArrayDeserializers<long[]> jsonDeserializer = (PrimitiveArrayDeserializers<long[]>) PrimitiveArrayDeserializers.forType(Long.class);
        long[] keys = jsonDeserializer.deserialize(jsonParser, deserializationContext);
        long[] values = jsonDeserializer.deserialize(jsonParser, deserializationContext);
        return new TLongLongHashMap(keys, values);
    }
}
