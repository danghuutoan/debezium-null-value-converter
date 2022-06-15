package ets.kafka.connect.converters;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import io.debezium.util.BoundedConcurrentHashMap;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.TimestampConverter;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.connect.data.Values;

public class DebeziumNullValueConverter
        implements CustomConverter<SchemaBuilder, RelationalColumn> {
    public static final String UNIX_START_TIME = "1970-01-01 00:00:00";
    public static final String MYSQL_ZERO_DATETIME = "0000-00-00 00:00:00";
    private List<TimestampConverter<SourceRecord>> converters = new ArrayList<>();
    private Boolean debug;
    private List<String> columnTypes = new ArrayList<>();
    private Iterable<String> inputFormats = null;
    private static final Logger LOGGER = LoggerFactory
            .getLogger(DebeziumNullValueConverter.class);
    private Cache<Object, Object> nullValueCache;

    @Override
    public void configure(Properties props) {

        debug = props.getProperty("debug", "false").equals("true");
        try {
            inputFormats = Arrays.asList(props.getProperty("input.formats").split(";"));
        } catch (NullPointerException e) {
            throw new ConfigException(
                    "No input datetime format provided");
        }

        for (String format : inputFormats) {
            LOGGER.info("configure DebeziumAllTimestampFieldsToAvroTimestampConverter using format {}", format);
            Map<String, String> config = new HashMap<>();
            config.put(TimestampConverter.TARGET_TYPE_CONFIG, "Timestamp");
            config.put(TimestampConverter.FORMAT_CONFIG, format);
            TimestampConverter<SourceRecord> timestampConverter = new TimestampConverter.Value<>();
            timestampConverter.configure(config);
            converters.add(timestampConverter);
        }
        nullValueCache = new SynchronizedCache<>(new LRUCache<>(64));
    }

    private Object doConvert(Object value, SchemaBuilder schema) {
        if (debug)
            LOGGER.info("Received value{}", value);

        if (value == null)
            return null;

        java.util.Date convertedValue = (java.util.Date) nullValueCache.get(value);

        if (convertedValue == null) {
            Exception exception = null;

            if (value instanceof String) {
                for (String format : inputFormats) {
                    try {
                        convertedValue = new SimpleDateFormat(format).parse(value.toString());
                        break;
                    } catch (Exception e) {
                        exception = e;
                    }
                }
            } else if (value instanceof java.util.Date) {
                convertedValue = Values.convertToDate(schema, value);
            } else if (value instanceof LocalDate) {
                convertedValue = java.sql.Date.valueOf((LocalDate) value);
                java.util.Date.from(((LocalDate) value).atStartOfDay().atZone(ZoneId.systemDefault()).toInstant());
                value = convertedValue;
            } else if (value instanceof ZonedDateTime) {
                convertedValue = java.util.Date.from(((ZonedDateTime) value).toInstant());
            } else {
                return value;
            }

            if (convertedValue == null) {
                if (exception == null) {
                    throw new RuntimeException(
                            "Bug Alert TimestampConverter: if record is null, exception should be provided");
                }
                LOGGER.error("Provided input format are not compatible with data.");
                throw new DataException(exception);
            }
        }

        if (debug)
            LOGGER.info("Received value{}", convertedValue.getTime());
        if (convertedValue.getTime() <= 0) {
            nullValueCache.put(value, convertedValue);
            return null;
        }

        return value;
    }

    @Override
    public void converterFor(RelationalColumn column,
            ConverterRegistration<SchemaBuilder> registration) {

        if (column.typeName().equals("DATE")) {
            SchemaBuilder schema = Date.builder().optional().defaultValue(null);
            registration.register(schema, value -> doConvert(value, schema));
        } else if (column.typeName().equals("DATETIME")) {
            SchemaBuilder schema = Timestamp.builder().optional().defaultValue(null);
            registration.register(schema, value -> doConvert(value, schema));
        } else if ((column.typeName().equals("TIMESTAMP")) || (columnTypes.contains(column.typeName()))) {
            SchemaBuilder schema = Timestamp.builder().optional().defaultValue(null);
            registration.register(schema, value -> doConvert(value, schema));
        } else {
            if (debug)
                LOGGER.info("##############{}", column.typeName());
        }
    }
}