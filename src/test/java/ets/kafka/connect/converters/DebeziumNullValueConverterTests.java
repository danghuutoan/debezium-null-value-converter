package ets.kafka.connect.converters;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.DataException;
import org.junit.Test;
import org.junit.Before;
import org.fest.assertions.Assertions;
import io.debezium.spi.converter.RelationalColumn;
import io.debezium.spi.converter.CustomConverter.Converter;
import io.debezium.spi.converter.CustomConverter.ConverterRegistration;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.TimeZone;

public class DebeziumNullValueConverterTests {

    public static class BasicColumn implements RelationalColumn {

        private final String name;
        private final String dataCollection;
        private final String typeName;

        public BasicColumn(String name, String dataCollection, String typeName) {
            super();
            this.name = name;
            this.dataCollection = dataCollection;
            this.typeName = typeName;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public String dataCollection() {
            return dataCollection;
        }

        @Override
        public String typeName() {
            return typeName;
        }

        @Override
        public int jdbcType() {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public int nativeType() {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public String typeExpression() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public OptionalInt length() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public OptionalInt scale() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public boolean isOptional() {
            // TODO Auto-generated method stub
            return false;
        }

        @Override
        public Object defaultValue() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public boolean hasDefaultValue() {
            // TODO Auto-generated method stub
            return false;
        }
    }

    private static class TestRegistration implements ConverterRegistration<SchemaBuilder> {
        public SchemaBuilder fieldSchema;
        public Converter converter;

        @Override
        public void register(SchemaBuilder fieldSchema, Converter converter) {
            this.fieldSchema = fieldSchema;
            this.converter = converter;
        }
    }

    private TestRegistration testRegistration;

    @Before
    public void before() {
        testRegistration = new TestRegistration();
    }

    @Test
    public void testShouldHandleTimestampType() throws ParseException {

        ZonedDateTime zdtWithZoneOffset = ZonedDateTime.parse("2022-05-20T00:35:29Z");
        final DebeziumNullValueConverter tsConverter = new DebeziumNullValueConverter();
        Properties props = new Properties();
        props.put("debug", "true");
        props.put("input.formats", "yyyy-MM-dd HH:mm:ss.S;yyyy-MM-dd'T'HH:mm:ss'Z'");
        tsConverter.configure(props);
        tsConverter.converterFor(new BasicColumn("myfield", "db1.table1", "TIMESTAMP"), testRegistration);
        Object actualResult = (Object) testRegistration.converter.convert(zdtWithZoneOffset);
        Assertions.assertThat(actualResult.equals(zdtWithZoneOffset)).isEqualTo(true);
    }

    @Test
    public void testShouldHandleLocalDateType() throws ParseException {

        final String format = "yyyy-MM-dd'T'HH:mm:ss'Z'";
        DateTimeFormatter formater = DateTimeFormatter.ofPattern(format);
        LocalDate lDate = LocalDate.parse("2022-05-20T00:35:29Z", formater);
        final DebeziumNullValueConverter tsConverter = new DebeziumNullValueConverter();
        Properties props = new Properties();
        props.put("debug", "true");
        props.put("input.formats", "yyyy-MM-dd HH:mm:ss.S;yyyy-MM-dd'T'HH:mm:ss'Z'");
        tsConverter.configure(props);
        tsConverter.converterFor(new BasicColumn("myfield", "db1.table1", "TIMESTAMP"), testRegistration);
        Object actualResult = (Object) testRegistration.converter.convert(lDate);
        Assertions.assertThat(actualResult.equals(lDate)).isEqualTo(true);
    }
    
    @Test
    public void testShouldReturnNullLocalDateType() throws ParseException {

        final String format = "yyyy-MM-dd'T'HH:mm:ss'Z'";
        DateTimeFormatter formater = DateTimeFormatter.ofPattern(format);
        LocalDate lDate = LocalDate.parse("1970-01-01T00:00:00Z",formater);
        final DebeziumNullValueConverter tsConverter = new DebeziumNullValueConverter();
        Properties props = new Properties();
        props.put("debug", "true");
        props.put("input.formats", "yyyy-MM-dd HH:mm:ss.S;yyyy-MM-dd'T'HH:mm:ss'Z'");
        tsConverter.configure(props);
        tsConverter.converterFor(new BasicColumn("myfield", "db1.table1", "TIMESTAMP"), testRegistration);
        Object actualResult = (Object) testRegistration.converter.convert(lDate);
        Assertions.assertThat(actualResult).isEqualTo(null);
    }


    @Test
    public void testShouldReturnNullTimestampType() throws ParseException {

        ZonedDateTime zdtWithZoneOffset = ZonedDateTime.parse("1970-01-01T00:00:00Z");
        final DebeziumNullValueConverter tsConverter = new DebeziumNullValueConverter();
        Properties props = new Properties();
        props.put("debug", "true");
        props.put("input.formats", "yyyy-MM-dd HH:mm:ss.S;yyyy-MM-dd'T'HH:mm:ss'Z'");
        tsConverter.configure(props);
        tsConverter.converterFor(new BasicColumn("myfield", "db1.table1", "TIMESTAMP"), testRegistration);
        Object actualResult = (Object) testRegistration.converter.convert(zdtWithZoneOffset);
        Assertions.assertThat(actualResult).isEqualTo(null);
    }

    @Test
    public void testShouldHandleStringTimestampType() throws ParseException {
        final DebeziumNullValueConverter tsConverter = new DebeziumNullValueConverter();
        Properties props = new Properties();
        props.put("debug", "true");
        props.put("input.formats", "yyyy-MM-dd HH:mm:ss.S;yyyy-MM-dd'T'HH:mm:ss'Z'");
        tsConverter.configure(props);
        tsConverter.converterFor(new BasicColumn("myfield", "db1.table1", "TIMESTAMP"), testRegistration);
        Object actualResult = (Object) testRegistration.converter.convert("2022-05-20T00:35:29Z");
        Assertions.assertThat(actualResult.equals("2022-05-20T00:35:29Z")).isEqualTo(true);
    }

    @Test
    public void testShouldReturnNullStringTimestampType() throws ParseException {
        // final String input = "2022-05-20T00:35:29Z";
        final DebeziumNullValueConverter tsConverter = new DebeziumNullValueConverter();
        Properties props = new Properties();
        props.put("debug", "true");
        props.put("input.formats", "yyyy-MM-dd HH:mm:ss.S;yyyy-MM-dd'T'HH:mm:ss'Z'");
        tsConverter.configure(props);
        tsConverter.converterFor(new BasicColumn("myfield", "db1.table1", "TIMESTAMP"), testRegistration);
        Object actualResult = (Object) testRegistration.converter.convert("1970-01-01T00:00:00Z");
        Assertions.assertThat(actualResult).isEqualTo(null);
    }

    @Test
    public void testShouldHandleDateType() throws ParseException {
        // final String input = "2022-05-20T00:35:29Z";
        final DebeziumNullValueConverter tsConverter = new DebeziumNullValueConverter();
        Properties props = new Properties();
        props.put("debug", "true");
        props.put("input.formats", "yyyy-MM-dd HH:mm:ss.S;yyyy-MM-dd'T'HH:mm:ss'Z'");
        tsConverter.configure(props);
        tsConverter.converterFor(new BasicColumn("myfield", "db1.table1", "TIMESTAMP"), testRegistration);
        Object actualResult = (Object) testRegistration.converter.convert("2022-05-20T00:35:29Z");
        Assertions.assertThat(actualResult.equals("2022-05-20T00:35:29Z")).isEqualTo(true);
    }

    @Test
    public void testShouldReturnNullDateType() throws ParseException {
        // final String input = "2022-05-20T00:35:29Z";
        final DebeziumNullValueConverter tsConverter = new DebeziumNullValueConverter();
        Properties props = new Properties();

        props.put("debug", "true");
        props.put("input.formats", "yyyy-MM-dd HH:mm:ss.S;yyyy-MM-dd'T'HH:mm:ss'Z'");
        tsConverter.configure(props);
        tsConverter.converterFor(new BasicColumn("myfield", "db1.table1", "TIMESTAMP"), testRegistration);
        Object actualResult = (Object) testRegistration.converter.convert("1970-01-01T00:00:00Z");
        Assertions.assertThat(actualResult).isEqualTo(null);
    }

    @Test
    public void testShouldIgoreStringType() throws ParseException {
        final DebeziumNullValueConverter tsConverter = new DebeziumNullValueConverter();
        final String format = "yyyy-MM-dd'T'HH:mm:ss'Z'";
        Properties props = new Properties();

        props.put("input.formats", "yyyy-MM-dd HH:mm:ss.S;yyyy-MM-dd'T'HH:mm:ss'Z'");
        props.put("debug", "true");
        props.put("format", format);
        tsConverter.configure(props);
        tsConverter.converterFor(new BasicColumn("myfield", "db1.table1", "String"), testRegistration);
        Assertions.assertThat(testRegistration.converter == null).isEqualTo(true);
    }

    @Test(expected = DataException.class)
    public void testInvalidDatetimeFormat() {
        final String input = "2022-05-20T00:35:29Z";
        final DebeziumNullValueConverter tsConverter = new DebeziumNullValueConverter();
        final String format = "yyyy-MM-dd'T'HH:mm:xys'Z'";
        Properties props = new Properties();
        props.put("format", format);
        props.put("debug", "true");
        props.put("input.formats", "yyyy-MM-dd HH:mm:ss.S;yyyy-MM-dd'T'HH:mm:ss.SS'Z'");
        tsConverter.configure(props);
        tsConverter.converterFor(new BasicColumn("myfield", "db1.table1", "TIMESTAMP"), testRegistration);
        testRegistration.converter.convert(input);
    }

    @Test
    public void shouldHandleMultiDatetimeFormat() {
        final String input = "2022-05-20T00:35:29Z";
        final DebeziumNullValueConverter tsConverter = new DebeziumNullValueConverter();
        final String format = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
        Properties props = new Properties();

        props.put("input.formats", "yyyy-MM-dd HH:mm:ss.S;yyyy-MM-dd'T'HH:mm:ss'Z'");
        props.put("format", format);
        props.put("debug", "true");
        tsConverter.configure(props);
        tsConverter.converterFor(new BasicColumn("myfield", "db1.table1", "TIMESTAMP"), testRegistration);
        testRegistration.converter.convert(input);
    }

    @Test(expected = ConfigException.class)
    public void shouldHaveCompatibleSimpleDateFormat() {
        final String input = "2022-05-20T00:35:29Z";
        final DebeziumNullValueConverter tsConverter = new DebeziumNullValueConverter();
        final String uncompatibleSimpleDateformat = "anUncompatibleFormat";
        Properties props = new Properties();

        props.put("input.formats", uncompatibleSimpleDateformat);
        tsConverter.configure(props);
        tsConverter.converterFor(new BasicColumn("myfield", "db1.table1", "TIMESTAMP"), testRegistration);
        testRegistration.converter.convert(input);
    }

    @Test(expected = ConfigException.class)
    public void shouldHaveFormatConfig() {
        final DebeziumNullValueConverter tsConverter = new DebeziumNullValueConverter();
        final String input = "2022-05-20T00:35:29Z";
        Properties props = new Properties();
        props.put("debug", "true");
        tsConverter.configure(props);
        tsConverter.converterFor(new BasicColumn("myfield", "db1.table1", "TIMESTAMP"), testRegistration);
        testRegistration.converter.convert(input);
    }

    SimpleDateFormat getFormater(String parttern) {
        SimpleDateFormat simpleDatetimeFormatter = new SimpleDateFormat(parttern);
        simpleDatetimeFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));
        return simpleDatetimeFormatter;
    }
}