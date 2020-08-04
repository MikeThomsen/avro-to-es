package org.apache.avro.elasticsearch;

import com.fasterxml.jackson.databind.ObjectMapper;
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
