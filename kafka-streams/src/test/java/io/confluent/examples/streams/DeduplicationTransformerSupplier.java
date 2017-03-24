package io.confluent.examples.streams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.streams.state.internals.RocksDBWindowStore;


/**
 * Created by cvaliente on 24.03.17.
 * <p>
 * de-duplicates a stream by ids returned by a uidExtractor over a window of maintainDurationMs
 */


public class DeduplicationTransformerSupplier<K, V, T> implements TransformerSupplier<K, V, KeyValue<K, V>> {

    private final String storeName;
    private final long maintainDurationMs;
    private final KeyValueMapper<K, V, T> uidExtractor;

    public DeduplicationTransformerSupplier(KStreamBuilder builder,
                                            String storeName,
                                            KeyValueMapper<K, V, T> uidExtractor,
                                            Serde<T> uidSerde,
                                            long maintainDurationMs,
                                            int segments) {
        this.storeName = storeName;
        this.uidExtractor = uidExtractor;
        this.maintainDurationMs = maintainDurationMs;
        StateStoreSupplier deduplicationStoreSupplier = Stores.create(this.storeName)
                .withKeys(uidSerde)
                .withValues(Serdes.Long())
                .persistent()
                .windowed(maintainDurationMs, segments, false)
                .build();
        builder.addStateStore(deduplicationStoreSupplier, storeName);
    }


    @Override
    public Transformer<K, V, KeyValue<K, V>> get() {
        return new DeduplicationTransformer();
    }

    private class DeduplicationTransformer implements Transformer<K, V, KeyValue<K, V>> {

        private RocksDBWindowStore<T, Long> store;
        private ProcessorContext context;


        @SuppressWarnings("unchecked")
        @Override
        public void init(ProcessorContext context) {
            this.context = context;

            store = (RocksDBWindowStore<T, Long>) context.getStateStore(storeName);
        }

        @Override
        public KeyValue<K, V> transform(final K key, final V value) {

            T uid = uidExtractor.apply(key, value);

            // if the uidExtractor returns null, always forward -- allows skipping of this check for certain records
            if (uid == null)
                return KeyValue.pair(key, value);

            WindowStoreIterator<Long> timeIterator = store.fetch(uid, context.timestamp() - maintainDurationMs,
                    context.timestamp() + maintainDurationMs);
            store.put(uid, context.timestamp());

            if (timeIterator.hasNext()) {
                timeIterator.close();
                return null;
            } else {
                timeIterator.close();
                return KeyValue.pair(key, value);
            }
        }

        @Override
        public KeyValue<K, V> punctuate(final long timestamp) {
            // our windowStore segments are closed automatically
            return null;
        }

        @Override
        public void close() {
            // The Kafka Streams API will automatically close stores when necessary.
        }
    }
}