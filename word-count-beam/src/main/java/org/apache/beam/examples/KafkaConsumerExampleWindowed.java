package org.apache.beam.examples;

import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.beam.examples.common.WriteOneFilePerWindow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;

public class KafkaConsumerWindowedExample {

    static final int DEFAULT_WINDOW_SIZE = 1;
    static final int DEFAULT_NUM_SHARDS = 1;

    public interface Options extends PipelineOptions {
        @Description("Fixed window duration, in minutes")
        @Default.Integer(DEFAULT_WINDOW_SIZE)
        Integer getWindowSize();

        void setWindowSize(Integer value);

        @Description("Fixed number of shards to produce per window")
        @Default.Integer(DEFAULT_NUM_SHARDS)
        Integer getNumShards();

        void setNumShards(Integer numShards);

        @Description("Path of the file to write to")
        @Validation.Required
        String getOutput();

        void setOutput(String value);
    }

    public static void main(String[] args) {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        Pipeline p = Pipeline.create(options);

        PTransform<PBegin, PCollection<KV<String, String>>> kafka = KafkaIO.<String, String>read()
                .withBootstrapServers("localhost:9092")
                .withTopic("orders")
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                // .updateConsumerProperties(ImmutableMap.of("auto.offset.reset", (Object)"earliest"))
                .withoutMetadata();

        p.apply(kafka)
                .apply(Values.<String>create())
                .apply(Window.into(FixedWindows.of(Duration.standardMinutes(options.getWindowSize()))))
                .apply(new WriteOneFilePerWindow("./output/orders", options.getNumShards()));

        p.run().waitUntilFinish();
    }
}
