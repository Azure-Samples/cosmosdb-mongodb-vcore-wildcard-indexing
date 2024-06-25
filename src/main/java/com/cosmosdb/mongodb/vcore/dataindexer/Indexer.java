package com.cosmosdb.mongodb.vcore.dataindexer;

import org.bson.Document;

import com.mongodb.MongoSocketReadException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Indexes;

public class Indexer implements Runnable {

	/**
	 * The field (including as many levels of nesting as needed) to be indexed for the first time
	 */
    private String fieldToBeIndexed;
    
    /**
     * The MongoCollection instance through which the indexing operation needs to be executed
     */
    private MongoCollection<Document> collection;
    
    public Indexer(String fieldToBeIndexed, MongoCollection<Document> collection) {
        this.fieldToBeIndexed = fieldToBeIndexed;
        this.collection = collection;
    }
    
    /**
     * Indexes the specific field as an independent thread.
     * 
     * The purpose behind this is to circumvent the scenario where a field when indexed for the first time,
     * does not get a response back from the vCore server until the indexing has been completed across
     * all existing documents in the collection.
     * 
     * This can be a time consuming effort when a large amount of data has already been ingested.
     * 
     * Thus, this avoids having to index each field one after the other until completion.
     * 
     * This is the equivalent of a fire and forget operation for all the fields that need to be indexed.
     */
    @Override
    public void run() {
        System.out.println(
            "About to create an index on field " + 
            this.fieldToBeIndexed + 
            "on thread: " + 
            Thread.currentThread().getName());
        
        try {
        	collection.createIndex(Indexes.ascending(this.fieldToBeIndexed));
        } catch (MongoSocketReadException ex) {
        	System.out.println(
        		"Done creating the index on field: " + 
        	    this.fieldToBeIndexed + 
        	    " ignoring MongoSocketReadException");
        }
        
        System.out.println("Done creating an index on field" + this.fieldToBeIndexed); 
    }
}
