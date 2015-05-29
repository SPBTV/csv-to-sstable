package com.spbtv.cassandra.bulkload;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;

import com.google.common.base.Joiner;

/**
 * Usage: java bulkload.BulkLoad <path/to/schema.cql> <path/to/input.csv> <path/to/output/dir>"
 */
public class Bulkload {
	
	private static final CsvPreference QUOTE_DELIMITED_CSV_PREF = new CsvPreference.Builder(
			'\'', ',', "\n").build();

	private static String readFile(String path, Charset encoding)
			throws IOException {
		byte[] encoded = Files.readAllBytes(Paths.get(path));
		return new String(encoded, encoding);
	}
	
	private static Map<String, String> extractColumns(String schema) 
	{
		Map<String, String> cols = new HashMap<>();
		Pattern columnsPattern = Pattern.compile(".*\\((.*)PRIMARY KEY.*");
		Matcher m = columnsPattern.matcher(schema);
		if (m.matches()) {
			for(String col : m.group(1).split(",")) {
				String [] name_type = col.trim().split("\\s+");
				if(name_type.length == 2)
					cols.put(name_type[0], name_type[1]);
			}

		} else throw new RuntimeException("Could not extract columns from provided schema.");
		return cols;
	}
	
	private static String extractTable(String schema, String keyspace) {
		Pattern columnsPattern = Pattern.compile(".*\\s+"+keyspace+"\\.(\\w+)\\s*\\(.*PRIMARY KEY.*");
		Matcher m = columnsPattern.matcher(schema);
		if (m.matches()) {
			return m.group(1).trim();
		}
		throw new RuntimeException("Could not extract table name from provided schema.");
	}
	
	private static Object parse(String value, String type) {
		// We use Java types here based on
		// http://www.datastax.com/drivers/java/2.0/com/datastax/driver/core/DataType.Name.html#asJavaClass%28%29
		switch(type) {
			case "text":
				return value;
			case "float":
				return Float.parseFloat(value);
			default:
				throw new RuntimeException("Cannot parse type '" + type + "'.");
		}
	}

	public static void main(String[] args) {
		if (args.length < 4) {
			System.out.println("usage: java bulkload.BulkLoad <path/to/schema.cql> <path/to/input.csv> <path/to/output/dir>");
			return;
		}

		String keyspace = args[0];
		String schema_path = args[1];
		String csv_path = args[2];
		String output_path = args[3];

		String schema = null;
		try {
			schema = String.format(readFile(schema_path, StandardCharsets.UTF_8), keyspace)
						   .replace("\n", " ").replace("\r", " ");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		Map<String, String> columns = extractColumns(schema);
		String table = extractTable(schema, keyspace);
		
		
		System.out.println(String.format("Converting CSV to SSTables for table '%s'...", table));

		// magic!
		Config.setClientMode(true);

		// Create output directory that has keyspace and table name in the path
		File outputDir = new File(output_path + File.separator + keyspace
				+ File.separator + table);
		if (!outputDir.exists() && !outputDir.mkdirs()) {
			throw new RuntimeException("Cannot create output directory: "
					+ outputDir);
		}

		try (
			BufferedReader reader = new BufferedReader(new FileReader(csv_path));
			CsvListReader csvReader = new CsvListReader(reader,QUOTE_DELIMITED_CSV_PREF)) {
			
			String [] header = csvReader.getHeader(true);

			String insert_stmt = String.format("INSERT INTO %s.%s ("
					+ Joiner.on(", ").join(header)
					+ ") VALUES (" + new String(new char[header.length - 1]).replace("\0", "?, ")
					+ "?)", keyspace,
					table);
			
			// Prepare SSTable writer
			CQLSSTableWriter.Builder builder = CQLSSTableWriter.builder();
			// set output directory
			builder.inDirectory(outputDir)
			// set target schema
					.forTable(schema)
					// set CQL statement to put data
					.using(insert_stmt)
					// set partitioner if needed
					// default is Murmur3Partitioner so set if you use different
					// one.
					.withPartitioner(new Murmur3Partitioner());
			CQLSSTableWriter writer = builder.build();

			// Write to SSTable while reading data
			List<String> line;
			while ((line = csvReader.read()) != null) {
				Map<String, Object> row = new HashMap<>();
				for(int i = 0; i < header.length; i++) {
					row.put(header[i], parse(line.get(i), columns.get(header[i])));
				}
				writer.addRow(row);
			}

			writer.close();

		} catch (InvalidRequestException | IOException e) {
			e.printStackTrace();
		}

		System.out.println("Done.");
	}
}