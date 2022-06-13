package ets.kafka.connect.converters;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
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
    private Iterable<String> inputFormats = null;
    private static final Logger LOGGER = LoggerFactory
            .getLogger(DebeziumNullValueConverter.class);

    @Override
    public void configure(Properties props) {

        try {
            inputFormats = Arrays.asList(props.getProperty("input.formats").split(";"));
        } catch (NullPointerException e) {
            throw new ConfigException(
                    "No input datetime format provided");
        }
        String[] nullEquivalentValuesArray;
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

        SchemaBuilder schema = null;
        switch (column.typeName()) {
            case "TIMESTAMP":
                schema = Timestamp.builder().optional();
            case "DATE":
                schema = Date.builder().optional();
        }

        if (schema != null) {
            registration.register(schema, value -> {
                if (value == null)
                    return null;
            
                java.util.Date convertedValue = null;
                Exception exception = null;
                for (String format : inputFormats) {
                    try {
                        DateFormat dateFormat = new SimpleDateFormat(format);
                        convertedValue = dateFormat.parse(value.toString());

                    } catch (Exception e) {
                        exception = e;
                    }
                }

                if (convertedValue == null) {
                    if (exception == null) {
                        throw new RuntimeException(
                                "Bug Alert TimestampConverter: if record is null, exception should be provided");
                    }
                    LOGGER.error("Provided input format are not compatible with data.");
                    throw new DataException(exception);
                }
                if (convertedValue.getTime() <= 0)
                    return null;

                return value;
            });
        }
    }
}