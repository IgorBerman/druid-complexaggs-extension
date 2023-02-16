package org.apache.druid.query.aggregation.complexaggs;

import java.io.IOException;

import org.apache.druid.query.aggregation.complexaggs.aggregator.TLongLongHashMapUtils;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdArraySerializers;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import gnu.trove.map.hash.TLongLongHashMap;

/**
 * Serializes key and values of map to consistent form by sorted keys first and then getting value
 * Probably might be done faster with working with key,value pair and implementing Comparator
 */
public class TLongLongHashMapOrderedSerializer extends StdSerializer<TLongLongHashMap> {
    StdArraySerializers.LongArraySerializer delegate = new StdArraySerializers.LongArraySerializer();
    public TLongLongHashMapOrderedSerializer() {
        super(TLongLongHashMap.class);
    }

    @Override
    public void serialize(TLongLongHashMap map, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
            throws IOException {
        delegate.serialize(TLongLongHashMapUtils.toArray(map), jsonGenerator, serializerProvider);
    }
}
