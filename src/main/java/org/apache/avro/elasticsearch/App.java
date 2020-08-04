package org.apache.avro.elasticsearch;

import org.apache.avro.Schema;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.FileInputStream;

public class App {
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

    public static void main(String[] args) throws Exception {
        CommandLine arguments = parseArguments(args);

        String schema = arguments.getOptionValue("schema");
        String output = arguments.getOptionValue("output");
        String mapping = arguments.getOptionValue("mapping");

        Schema parsedSchema = new Schema.Parser().parse(new FileInputStream(schema));
        parsedSchema.getFields().forEach(field -> System.out.println(field.getObjectProp("es_type")));
        parsedSchema.getFields().forEach(field -> System.out.println(field.getObjectProp("es_fields")));
    }
}
