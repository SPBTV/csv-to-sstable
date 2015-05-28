
package com.spbtv.cassandra.bulkload;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;

/**
 * Usage: java bulkload.BulkLoad
 */
public class Bulkload
{
//	public static final String csv_path = "/Users/ovdbigge/Documents/common_recommender/tmp/amedia/mahout_item_binary_knn_low_threshold_result_cassandra.csv";
//	public static final String output_path = "/Users/ovdbigge/Documents/common_recommender/tmp/amedia/sstables";

    /** Keyspace name */
    public static final String KEYSPACE = "recommendations";
    /** Table name */
    public static final String TABLE = "user_recommendations";

    /**
     * Schema for bulk loading table.
     * It is important not to forget adding keyspace name before table name,
     * otherwise CQLSSTableWriter throws exception.
     */
    public static final String SCHEMA = String.format("CREATE TABLE %s.%s (" +
													  "user_id text, " +
													  "application text, " +
													  "algorithm text, " +
													  "recommended_item_id text, " +
													  "language text, " +
													  "rating float, " +
													  "type text, " +
													  "PRIMARY KEY ((user_id, application), algorithm, recommended_item_id) " +
													") WITH CLUSTERING ORDER BY (algorithm ASC, recommended_item_id ASC)", KEYSPACE, TABLE);

    /**
     * INSERT statement to bulk load.
     * It is like prepared statement. You fill in place holder for each data.
     */
    public static final String INSERT_STMT = String.format("INSERT INTO %s.%s (" +
            "user_id, application, algorithm, recommended_item_id, language, rating, type" +
        ") VALUES (" +
            "?, ?, ?, ?, ?, ?, ?" +
        ")", KEYSPACE, TABLE);
    
    private static final CsvPreference QUOTE_DELIMITED_CSV_PREF = new CsvPreference.Builder('\'', ',', "\n").build();

    public static void main(String[] args)
    {    	
        if (args.length < 2)
        {
            System.out.println("usage: java bulkload.BulkLoad <path/to/input.csv> <path/to/output/dir>");
            return;
        }
        
        String csv_path = args[0];
        String output_path = args[1];
        
        System.out.println("Generating SSTables...");

        // magic!
        Config.setClientMode(true);

        // Create output directory that has keyspace and table name in the path
        File outputDir = new File(output_path + File.separator + KEYSPACE + File.separator + TABLE);
        if (!outputDir.exists() && !outputDir.mkdirs())
        {
            throw new RuntimeException("Cannot create output directory: " + outputDir);
        }

        // Prepare SSTable writer
        CQLSSTableWriter.Builder builder = CQLSSTableWriter.builder();
        // set output directory
        builder.inDirectory(outputDir)
               // set target schema
               .forTable(SCHEMA)
               // set CQL statement to put data
               .using(INSERT_STMT)
               // set partitioner if needed
               // default is Murmur3Partitioner so set if you use different one.
               .withPartitioner(new Murmur3Partitioner());
        CQLSSTableWriter writer = builder.build();

        try (
        	BufferedReader reader = new BufferedReader(new FileReader(csv_path));
            CsvListReader csvReader = new CsvListReader(reader, QUOTE_DELIMITED_CSV_PREF)
        )
        {
            csvReader.getHeader(true);

            // Write to SSTable while reading data
            List<String> line;
            while ((line = csvReader.read()) != null)
            {
                // We use Java types here based on
                // http://www.datastax.com/drivers/java/2.0/com/datastax/driver/core/DataType.Name.html#asJavaClass%28%29

                writer.addRow(line.get(0),
                		line.get(1),
                        line.get(2),
                        line.get(3),
                        line.get(4),
                        Float.parseFloat(line.get(5)),
                        line.get(6));
            }
        }
        catch (InvalidRequestException | IOException e)
        {
            e.printStackTrace();
        }

        try
        {
            writer.close();
        }
        catch (IOException ignore) {}
        
        System.out.println("Done.");
    }
}