package org.apache.druid.query.aggregation.complexaggs;

import java.util.List;

import org.apache.druid.initialization.DruidModule;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.apache.druid.sql.guice.SqlBindings;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;

import gnu.trove.map.hash.TLongLongHashMap;
import org.apache.druid.query.aggregation.complexaggs.aggregator.LongFrequencyAggregatorFactory;
import org.apache.druid.query.aggregation.complexaggs.sql.LongFrequencySqlAggregator;

public class ComplexAggregatorsExtensionModule implements DruidModule {
    @Override
    public List<? extends Module> getJacksonModules() {
        return ImmutableList.of(createSubTypesModule(), createProductionSerdeModule());
    }

    public static Module createSubTypesModule() {
        SimpleModule simpleModule = new SimpleModule(ComplexAggregatorsExtensionModule.class.getSimpleName());
        simpleModule.registerSubtypes(new NamedType(LongFrequencyAggregatorFactory.class, LongFrequencyAggregatorFactory.TYPE_NAME));
        return simpleModule;
    }

    public static final ObjectMapper OBJECT_MAPPER =  new DefaultObjectMapper().registerModule(createProductionSerdeModule());
    static SimpleModule createProductionSerdeModule() {
        SimpleModule module = new SimpleModule();
        module.addSerializer(TLongLongHashMap.class, new TLongLongHashMapOrderedSerializer()); //haven't found a way to switch between them...:(
        module.addDeserializer(TLongLongHashMap.class, new TLongLongHashMapDeserializer());
        return module;
    }

    @VisibleForTesting
    public static SimpleModule createTestSerdeModule() {
        SimpleModule module = new SimpleModule();
        module.addSerializer(TLongLongHashMap.class, new TLongLongHashMapOrderedSerializer()); //this is the difference from production
        module.addDeserializer(TLongLongHashMap.class, new TLongLongHashMapDeserializer());
        return module;
    }

    @Override
    public void configure(Binder binder) {
        registerSerde();
        SqlBindings.addAggregator(binder, LongFrequencySqlAggregator.class);
    }

    @VisibleForTesting
    public static void registerSerde() {
        ComplexMetrics.registerSerde(LongFrequencyAggregatorFactory.TYPE_NAME, new ComplexFrequencySerde());
    }

}
