package com.cosmosdb.mongodb.vcore.filereader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class SampleJSONFileReader {
	
	public SampleJSONFileReader() {
		
	}
	
	/**
	 * Simply reads the contents of the specified JSON file and returns the contents of the file as a string.
	 * 
	 * This method does NOT valid that the specified field contains a valid JSON.
	 * 
	 * @param jsonFilePath - The local path of the sample JSON file
	 * 
	 * @return The contents of the sample file as a string
	 */
	public String getJSONStringFromSampleFile(String jsonFilePath) {
		StringBuilder jsonStringBuilder = new StringBuilder();
		try (BufferedReader br = new BufferedReader(new FileReader(jsonFilePath))) {
			String line;
		    while ((line = br.readLine()) != null) {
		        jsonStringBuilder.append(line);
		    }
		} catch (IOException e) {
		    e.printStackTrace();
		}
		
		return jsonStringBuilder.toString();
	}

}
