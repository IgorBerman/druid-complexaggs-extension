package org.apache.druid.query.aggregation.complexaggs;

import gnu.trove.map.hash.TLongLongHashMap;

import java.io.IOException;

import org.apache.druid.query.aggregation.complexaggs.aggregator.TLongLongHashMapUtils;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.PrimitiveArrayDeserializers;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

public class TLongLongHashMapDeserializer extends StdDeserializer<TLongLongHashMap> {
    private final PrimitiveArrayDeserializers<long[]> jsonDeserializer;
    public TLongLongHashMapDeserializer() {
        super(TLongLongHashMap.class);
        this.jsonDeserializer = (PrimitiveArrayDeserializers<long[]>) PrimitiveArrayDeserializers.forType(Long.TYPE);
    }

    @Override
    public TLongLongHashMap deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
            throws IOException {
        long[] array = jsonDeserializer.deserialize(jsonParser, deserializationContext);
        return TLongLongHashMapUtils.fromArray(array);
    }
}
