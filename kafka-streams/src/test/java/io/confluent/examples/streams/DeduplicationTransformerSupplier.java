
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
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

import java.util.concurrent.TimeUnit;

/**
 * Created by cvaliente on 24.03.17.
 *
 * de-duplicates a stream by ids returned by a uidExtractor over a window of maintainDurationMs
 */


public class DeduplicationTransformerSupplier<K, V, T> implements TransformerSupplier<K, V, KeyValue<K,V>> {

    private final String storeName;
    private final long maintainDurationMs;
    private long flushInterval;
    private final KeyValueMapper<K,V,T> uidExtractor;

    public DeduplicationTransformerSupplier(KStreamBuilder builder,
                                            String storeName,
                                            KeyValueMapper<K,V,T> uidExtractor,
                                            Serde<T> uidSerde,
                                            long maintainDurationMs) {
        this.storeName = storeName;
        this.uidExtractor = uidExtractor;
        this.maintainDurationMs = maintainDurationMs;
        StateStoreSupplier deduplicationStoreSupplier = Stores.create(this.storeName)
                .withKeys(uidSerde)
                .withValues(Serdes.Long())
                .persistent()
                .build();
        builder.addStateStore(deduplicationStoreSupplier, storeName);
        this.flushInterval = TimeUnit.MINUTES.toMillis(1);
    }

    public void setFlushInterval(long flushInterval) {
        this.flushInterval = flushInterval;
    }

    @Override
    public Transformer<K, V, KeyValue<K,V>> get() {
        return new DeduplicationTransformer();
    }

    private class DeduplicationTransformer implements Transformer<K, V, KeyValue<K,V>> {

        private KeyValueStore<T, Long> store;
        private ProcessorContext context;

        @SuppressWarnings("unchecked")
        @Override
        public void init(ProcessorContext context) {
            this.context = context;

            store = (KeyValueStore<T, Long>) context.getStateStore(storeName);

            this.context.schedule(flushInterval);
        }

        @Override
        public KeyValue<K,V> transform(final K key,final  V value) {

            T uid = uidExtractor.apply(key, value);

            // if the uidExtractor returns null, always forward -- allows skipping of this check for certain records
            if (uid == null)
                return KeyValue.pair(key, value);

            Long uidTime = store.get(uid);

            // update entry to timestamp of latest entry
            store.put(uid, uidTime == null || uidTime < context.timestamp() ? context.timestamp() : uidTime);

            if (uidTime == null || hasExpired(uidTime)) {
                return KeyValue.pair(key, value);
            }
            else {
                return null;
            }
        }


        @Override
        public KeyValue<K,V> punctuate(final long timestamp) {

            // delete uIDs that haven't been seen for maintainDurationMs ms
            purgeExpiredEventIds();
            return null;
        }

        private void purgeExpiredEventIds() {
            try (final KeyValueIterator<T, Long> iterator = store.all()) {
                while (iterator.hasNext()) {
                    final KeyValue<T, Long> entry = iterator.next();
                    final long eventTimestamp = entry.value;
                    if (hasExpired(eventTimestamp)) {
                        store.delete(entry.key);
                    }
                }
            }
        }


        private boolean hasExpired(final long eventTimestamp) {
            final long nowMs = context.timestamp();
            return (nowMs - eventTimestamp) > maintainDurationMs;
        }

        @Override
        public void close() {
            // The Kafka Streams API will automatically close stores when necessary.
        }
    }
}