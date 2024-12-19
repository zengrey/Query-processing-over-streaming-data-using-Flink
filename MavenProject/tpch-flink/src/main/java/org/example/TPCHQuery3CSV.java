package org.example;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.io.File;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.core.fs.Path;

import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineFormat;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class TPCHQuery3CSV{

     // Generate random date between 1995-03-01 and 1995-03-31
     public static String generateRandomDate() {
        LocalDate startDate = LocalDate.of(1995, 3, 1);
        LocalDate endDate = LocalDate.of(1995, 3, 31);
        long daysBetween = java.time.temporal.ChronoUnit.DAYS.between(startDate, endDate);
        Random random = new Random();
        long randomDays = random.nextInt((int) daysBetween + 1);
        LocalDate randomDate = startDate.plusDays(randomDays);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        return randomDate.format(formatter);
    }


    public static void execute(StreamExecutionEnvironment env) {
        String csvFilePath = "F:/IP/MavenProject/tpch-flink/input_data_all.csv";
        String kafkaTopic = "tpch-query3-results";
        String bootstrapServers = "localhost:9092";
        String result_path="F:/IP/MavenProject/tpch-flink/output/result.txt";

        // Source: Read the file as a DataStream of Strings
        File file = new File(csvFilePath);
        //build the reference result
        Set<String> referenceData;
        try{
            referenceData=ReferenceLoader.loadReferenceData(result_path);
        }catch (Exception e) {
            e.printStackTrace(); // Print the exception (or handle it appropriately)
            referenceData = new HashSet<>(); // Use an empty set as a fallback
        }

        //load the csv data
        FileSource<String> fileSource = FileSource
        .forRecordStreamFormat(new TextLineFormat(), Path.fromLocalFile(file))
        .build();
       
       // Read the file as DataStream of Strings
       System.out.println("Starting to read from the CSV file...");
       DataStream<String> input = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "CSV Source");
       System.out.println("Successfully read the file source.");
        // Step 1: Filter live tuples and keep state
        DataStream<Tuple2<String, Object>> liveTuples = input
        .keyBy(line -> {
        // Key the stream based on table and appropriate key
        String tableName = line.substring(1, 3);
        String[] fields = line.substring(3).split("\\|");
        switch (tableName) {
            case "CU": return Integer.parseInt(fields[0]); // Customer key
            case "OR": return Integer.parseInt(fields[0]); // Order key
            case "LI": return Integer.parseInt(fields[0]); // LineItem order key
            default: return -1; // Default for safety
        }
    })
    .flatMap(new LiveTupleFilter()); // Stateful filter to select the live tuple

        System.out.println("Filtered live tuples successfully.");

        // Set up Flink Table Environment
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // Register temporary views for the tables
        tableEnv.createTemporaryView("lineitem", liveTuples
                .filter(record -> record.f0.equals("LI"))
                .map(record -> (LineItem) record.f1));
        System.out.println("Temporary view 'lineitem' created.");

        tableEnv.createTemporaryView("orders", liveTuples
                .filter(record -> record.f0.equals("OR"))
                .map(record -> (Orders) record.f1));
        System.out.println("Temporary view 'orders' created.");

        tableEnv.createTemporaryView("customer", liveTuples
                .filter(record -> record.f0.equals("CU"))
                .map(record -> (Customer) record.f1));
        System.out.println("Temporary view 'customer' created.");

        //Execute Query
        //String randomDate = generateRandomDate();
        String randomDate =  "1995-03-13";
        String query = String.format(
                "SELECT l_orderkey, " +
                "       SUM(l_extendedprice * (1 - l_discount)) AS revenue, " +
                "       o_orderdate, " +
                "       o_shippriority " +
                "FROM lineitem " +
                "JOIN orders ON l_orderkey = o_orderkey " +
                "JOIN customer ON c_custkey = o_custkey " +
                "WHERE c_mktsegment = 'AUTOMOBILE' " +
                "  AND o_orderdate < DATE '%s' " +
                "  AND l_shipdate > DATE '%s' " +
                "GROUP BY l_orderkey, o_orderdate, o_shippriority " +
                "ORDER BY revenue DESC, o_orderdate " +
                "LIMIT 10",randomDate,randomDate);

        Table result = tableEnv.sqlQuery(query);
        System.out.println("Query executed successfully.");

        //verify the result 
        DataStream<String> queryResults = tableEnv.toDataStream(result)
                .map(row -> row.toString()); 

        //  Add Accuracy Checking
        DataStream<Tuple2<String, Double>> resultsWithAccuracy = queryResults
                .keyBy(row -> row.hashCode()) 
                .process(new AccuracyChecker(referenceData))
                .name("Accuracy Checker");

        resultsWithAccuracy
        .map(resultTuple -> {
            System.out.println("Result: " + resultTuple.f0 + ", Accuracy: " + resultTuple.f1 + "%");
            return resultTuple;
        })
        .returns(TypeInformation.of(new org.apache.flink.api.common.typeinfo.TypeHint<Tuple2<String, Double>>() {})) 
        .print();

        // Write the result to Upsert Kafka Sink
        tableEnv.executeSql(String.format(
                "CREATE TABLE kafka_sink (" +
                "    l_orderkey INT, " +
                "    revenue DOUBLE, " +
                "    o_orderdate STRING, " +
                "    o_shippriority STRING, " +
                "    PRIMARY KEY (l_orderkey) NOT ENFORCED " +
                ") WITH (" +
                "    'connector' = 'upsert-kafka', " +
                "    'topic' = '%s', " +
                "    'properties.bootstrap.servers' = '%s', " +
                "    'key.format' = 'json', " +
                "    'value.format' = 'json'" +
                ")", kafkaTopic, bootstrapServers));

        result.executeInsert("kafka_sink");
        System.out.println("Results written to Kafka.");
    }

    // LiveTupleFilter: Filters tuples based on query conditions and parent-child relationships
    public static class LiveTupleFilter extends RichFlatMapFunction<String, Tuple2<String, Object>> {
        private transient MapState<Integer, Boolean> customerState;
        private transient MapState<Integer, Boolean> orderState;
        private transient MapState<Integer, Boolean> itemState;
        
        @Override
        public void open(Configuration parameters) throws Exception {
            // Initialize state to track live customers and orders
            customerState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("customerState", Integer.class, Boolean.class));
            orderState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("orderState", Integer.class, Boolean.class));
            itemState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("itemState", Integer.class, Boolean.class));
        }

        @Override
        public void flatMap(String line, Collector<Tuple2<String, Object>> out) throws Exception {
            String tableName = line.substring(1, 3);
            String[] fields = line.substring(3).split("\\|");

            switch (tableName) {
                case "CU": // Customer
                    int custKey = Integer.parseInt(fields[0]);
                    String segment = fields[6];
                    if ("AUTOMOBILE".equals(segment)) {
                        customerState.put(custKey, true); // Mark as live
                        Customer customer = new Customer();
                        try{
                            customer.c_custkey = Integer.parseInt(fields[0]);
                            customer.c_name = fields[1];
                            customer.c_address = fields[2];
                            customer.c_nationkey = Integer.parseInt(fields[3]);
                            customer.c_phone = fields[4];
                            customer.c_acctbal = Double.parseDouble(fields[5]);
                            customer.c_mktsegment = fields[6];
                            customer.c_comment = fields[7];
                            System.out.println("Parsed Customer: " + customer);
                            out.collect(new Tuple2<>("CU", customer));
                            }catch (NumberFormatException e) {
                                System.out.println("Skipping invalid record: " + line);
                            }
                    }
                    break;

                case "OR": // Orders
                    int orderKey = Integer.parseInt(fields[0]);
                    int o_custkey = Integer.parseInt(fields[1]);
                    String orderdate = fields[4];
                    if (customerState.get(o_custkey) != null && orderdate.compareTo("1995-03-13") < 0) { // Verify parent is live and if it withine the time range
                        orderState.put(orderKey, true);//Mark as live 
                        Orders order = new Orders();
                        try{
                            order.o_orderkey = Integer.parseInt(fields[0]);
                            order.o_custkey = Integer.parseInt(fields[1]);
                            order.o_orderstatus = fields[2];
                            order.o_totalprice = Double.parseDouble(fields[3]);
                            order.o_orderdate = fields[4];
                            order.o_orderpriority = fields[5];
                            order.o_clerk = fields[6];
                            order.o_shippriority = fields[7];
                            order.o_comment = fields[8];
                            out.collect(new Tuple2<>("OR", order));
                        }catch (NumberFormatException e) {
                            System.out.println("Skipping invalid record: " + line);
                        }
                    }
                    break;

                case "LI": // LineItem
                    int l_orderkey = Integer.parseInt(fields[0]);
                    String shipDate = fields[10];
                    if (orderState.get(l_orderkey) != null && shipDate.compareTo("1995-03-13") > 0) {
                        itemState.put(l_orderkey, true);
                        LineItem lineItem = new LineItem();
                     try{
                         lineItem.l_orderkey = Integer.parseInt(fields[0]);
                         lineItem.l_partkey = Integer.parseInt(fields[1]);
                         lineItem.l_suppkey = Integer.parseInt(fields[2]);
                         lineItem.l_linenumber = Integer.parseInt(fields[3]);
                         lineItem.l_quantity = Double.parseDouble(fields[4]);
                         lineItem.l_extendedprice = Double.parseDouble(fields[5]);
                         lineItem.l_discount = Double.parseDouble(fields[6]);
                         lineItem.l_tax = Double.parseDouble(fields[7]);
                         lineItem.l_returnflag = fields[8];
                         lineItem.l_linestatus = fields[9];
                         lineItem.l_shipdate = fields[10];
                         lineItem.l_commitdate = fields[11];
                         lineItem.l_receiptdate = fields[12];
                         lineItem.l_shipinstruct = fields[13];
                         lineItem.l_shipmode = fields[14];
                         lineItem.l_comment = fields[15];
                         out.collect(new Tuple2<>("LI", lineItem));
                         }catch (NumberFormatException e) {
                                System.out.println("Skipping invalid record: " + line);
                                }
                    }
                    break;
            }
        }
    }

   
}

