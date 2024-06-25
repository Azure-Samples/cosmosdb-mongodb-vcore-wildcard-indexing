package com.cosmosdb.mongodb.vcore.dataindexer;

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.bson.Document;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.cosmosdb.mongodb.vcore.filereader.SampleJSONFileReader;
import com.cosmosdb.mongodb.vcore.json.parser.JsonParser;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;

public class ReIndex {

	/**
     * Connection String for the Cosmos DB for MongoDB vCore cluster
     */
    private static String MongovCoreConnectionString;
    
    /**
     * The database within which the documents will be inserted for the benchmarking run.
     */
    private static String MongovCoreDatabase;
    
    /**
     * The collection into which the documents will be inserted for the benchmarking run.
     */
    private static String MongovCoreCollection;
    
    /**
     * The path to the local json file to be parsed and each field and nested field to be indexed.
     */
    private static String SampleJsonFilePath;
    
    public static void main(String[] args) throws InterruptedException {
    	
    	((LoggerContext) LoggerFactory.getILoggerFactory()).getLogger("org.mongodb.driver").setLevel(Level.ERROR);
    	
    	if (args.length > 0) {
    		MongovCoreConnectionString = args[0];
    	} if (args.length > 1) {
    		MongovCoreDatabase = args[1];
    	} if (args.length > 2) {
    		MongovCoreCollection = args[2];
    	} if (args.length > 3) {
    		SampleJsonFilePath = args[3];
    	}
    	
    	SampleJSONFileReader sampleFileReader = new SampleJSONFileReader();
    	String jsonDocument = sampleFileReader.getJSONStringFromSampleFile(SampleJsonFilePath);
    	
    	JsonParser jsonParser = new JsonParser(jsonDocument);
    	Set<String> eachPathToIndex = jsonParser.parseJson();
    	
    	try (MongoClient mongoClient = MongoClients.create(MongovCoreConnectionString)) {
        	MongoDatabase database = mongoClient.getDatabase(MongovCoreDatabase);
        	MongoCollection<Document> collection = database.getCollection(MongovCoreCollection);
        	
        	ExecutorService es = Executors.newCachedThreadPool();
        	
        	for (String eachNestedPath : eachPathToIndex) {
    			System.out.println(eachNestedPath);
    			
    			Indexer fireAndForgetIndexer = new Indexer(eachNestedPath, collection);
    			es.execute(new Thread(fireAndForgetIndexer));
    		}
    		
    		es.shutdown();
    		
    		// Wait for 2 minutes before exiting all threads
            while (!es.awaitTermination(2*60*1000, TimeUnit.MILLISECONDS)) {
            }
        }
    }
}
