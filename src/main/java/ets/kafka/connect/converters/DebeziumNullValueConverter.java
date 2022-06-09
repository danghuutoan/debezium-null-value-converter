package ets.kafka.connect.converters;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.TimestampConverter;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DebeziumNullValueConverter
        implements CustomConverter<SchemaBuilder, RelationalColumn> {
    public static final String UNIX_START_TIME = "1970-01-01 00:00:00";
    public static final String MYSQL_ZERO_DATETIME = "0000-00-00 00:00:00";
    private List<TimestampConverter<SourceRecord>> converters = new ArrayList<>();
    private String alternativeDefaultValue;
    private Boolean debug;
    private List<String> columnTypes = new ArrayList<>();
    private List<String> nullEquivalentValues = new ArrayList<>();
    private static final Logger LOGGER = LoggerFactory
            .getLogger(DebeziumNullValueConverter.class);

    @Override
    public void configure(Properties props) {

        Iterable<String> inputFormats = null;
        try {
            inputFormats = Arrays.asList(props.getProperty("input.formats").split(";"));
        } catch (NullPointerException e) {
            throw new ConfigException(
                    "No input datetime format provided");
        }
        String [] nullEquivalentValuesArray;
        columnTypes = Arrays.asList(props.getProperty("column.types", "TIMESTAMP").split(";"));
        alternativeDefaultValue = props.getProperty("alternative.default.value", UNIX_START_TIME);
        debug = props.getProperty("debug", "false").equals("true");
        nullEquivalentValuesArray = props.getProperty("null.equivalent.values", "0000-00-00 00:00:00").split(";");
        for (int i = 0; i < nullEquivalentValuesArray.length; i++) {
            nullEquivalentValues.add(nullEquivalentValuesArray[i]);
        }
        if (alternativeDefaultValue.equals("null"))
            alternativeDefaultValue = null;

        for (String format : inputFormats) {
            LOGGER.info("configure DebeziumAllTimestampFieldsToAvroTimestampConverter using format {}", format);
            Map<String, String> config = new HashMap<>();
            config.put(TimestampConverter.TARGET_TYPE_CONFIG, "Timestamp");
            config.put(TimestampConverter.FORMAT_CONFIG, format);
            TimestampConverter<SourceRecord> timestampConverter = new TimestampConverter.Value<>();
            timestampConverter.configure(config);
            converters.add(timestampConverter);
        }

    }

    @Override
    public void converterFor(RelationalColumn column,
            ConverterRegistration<SchemaBuilder> registration) {
        
        if (columnTypes.contains(column.typeName())) {
            SchemaBuilder schema = Timestamp.builder();
            if (alternativeDefaultValue == null)
                schema = schema.optional().defaultValue(alternativeDefaultValue);
            registration.register(schema, value -> {
                
                if (debug) LOGGER.info("Received value{}", value);
                value = (value == null || nullEquivalentValues.contains(value.toString())) ? alternativeDefaultValue
                        : value.toString();
                if (value == null) return null;
                
                SourceRecord record = new SourceRecord(null, null, null, 0, SchemaBuilder.string().schema(),
                        value);

                SourceRecord convertedRecord = null;
                Exception exception = null;
                for (TimestampConverter<SourceRecord> converter : converters) {
                    try {
                        convertedRecord = converter.apply(record);
                        break;
                    } catch (DataException e) {
                        exception = e;
                    }
                }

                if (convertedRecord == null) {
                    if (exception == null) {
                        throw new RuntimeException(
                                "Bug Alert TimestampConverter: if record is null, exception should be provided");
                    }
                    LOGGER.error("Provided input format are not compatible with data.");
                    throw new DataException(exception);
                }
                return convertedRecord.value();
            });
        }
    }
}