package org.apache.avro.elasticsearch;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.avro.Schema.Type.BOOLEAN;
import static org.apache.avro.Schema.Type.RECORD;

public class App {
    public static final String KEY_TYPE = "es_type";
    public static final String KEY_FIELDS = "es_fields";

    private static CommandLine parseArguments(String[] args) throws ParseException {
        Options options = new Options();

        Option input = new Option("s", "schema", true, "Avro schema file");
        input.setRequired(true);
        options.addOption(input);

        Option output = new Option("o", "output", true, "Output file to store the ES schema.");
        output.setRequired(true);
        options.addOption(output);

        Option mapping = new Option("m", "mapping", true, "The name of the ES mapping.");
        mapping.setRequired(true);
        options.addOption(mapping);

        CommandLineParser parser = new DefaultParser();

        return parser.parse(options, args);
    }

    public static void convert(Map<String, Object> map, List<Schema.Field> fields) {
        for (Schema.Field field : fields) {
            Map<String, Object> converted = convertField(field);
            map.put(field.name(), converted);

            if (field.schema().getType() == RECORD) {
                convert((Map<String, Object>) converted.get("properties"), field.schema().getFields());
            }
        }
    }

    private static String convertAvroTypeToElasticType(Schema.Type avroType, LogicalType lt) {
        switch (avroType) {
            case BOOLEAN:
                return "boolean";
            case DOUBLE:
                return "double";
            case FLOAT:
                return "double";
            case INT:
                return lt != null && lt.getName().equals("date") ? "date": "int";
            case LONG:
                return lt != null && lt.getName().contains("timestamp") ? "date" : "long";
            case STRING:
                return "text";
            case RECORD:
                return "nested";
            default:
                return "text";
        }
    }

    private static Map<String, Object> convertField(Schema.Field field) {
        Map<String, Object> converted = new HashMap<>();

        Schema.Type schemaName = field.schema().getType();
        LogicalType lt = field.schema().getLogicalType();

        if (field.getObjectProps().containsKey(KEY_TYPE)) {
            converted.put("type", field.getObjectProp(KEY_TYPE));
        } else {
            converted.put("type", convertAvroTypeToElasticType(schemaName, lt));
        }

        if (schemaName == RECORD) {
            converted.put("properties", new HashMap<>());
        }

        if (field.getObjectProps().containsKey(KEY_FIELDS)) {
            Map<String, Object> fields = new HashMap<>();
            Object temp = field.getObjectProps().get(KEY_FIELDS);
            if (!(temp instanceof Map)) {
                System.out.println(String.format("The property %s in field %s was not a JSON map.", KEY_FIELDS, field.name()));
                System.exit(1);
            }

            Map<String, Object> prop = (Map<String, Object>)temp;
            prop.entrySet().forEach(set -> {
                Map<String, Object> inner = new HashMap<>();
                inner.put("type", set.getValue());
                fields.put(set.getKey(), inner);
            });
            converted.put("fields", fields);
        }

        return converted;
    }

    public static void writeJson(Map<String, Object> map, String output) throws IOException  {
        String json = new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(map);
        FileWriter writer = new FileWriter(output);
        writer.write(json);
        writer.close();
        System.out.println(String.format("Wrote schema to %s", output));
    }

    public static void main(String[] args) throws Exception {
        CommandLine arguments = parseArguments(args);

        String schema = arguments.getOptionValue("schema");
        String output = arguments.getOptionValue("output");
        String mapping = arguments.getOptionValue("mapping");

        Schema parsedSchema = new Schema.Parser().parse(new FileInputStream(schema));
        Map<String, Object> map = new HashMap<>();
        Map<String, Object> mappings = new HashMap<>();
        Map<String, Object> thisMap = new HashMap<>();
        Map<String, Object> properties = new HashMap<>();
        map.put("mappings", mappings);
        mappings.put(mapping, thisMap);
        thisMap.put("properties", properties);

        convert(properties, parsedSchema.getFields());

        writeJson(map, output);
    }
}
