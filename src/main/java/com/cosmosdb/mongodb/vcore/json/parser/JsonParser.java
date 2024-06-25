package com.cosmosdb.mongodb.vcore.json.parser;

import java.io.StringReader;
import java.util.LinkedHashSet;
import java.util.Map.Entry;
import java.util.Set;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonReader;
import javax.json.JsonValue;

public class JsonParser {

	private String jsonDocument;
	
	public JsonParser(String jsonDocument) {
		this.jsonDocument = jsonDocument;
	}
	
	public Set<String> parseJson() {
		JsonReader jsonReader = Json.createReader(new StringReader(this.jsonDocument));
		JsonValue jValue = jsonReader.readValue();
		
		Set<String> nestedPaths = new LinkedHashSet<>();
		getRecursivePaths(jValue, "", nestedPaths);
		
		return nestedPaths;
	}
	
	private void getRecursivePaths(JsonValue jsonObjectToTraverse, String rootPath, Set<String> nestedPaths) {
		
		switch(jsonObjectToTraverse.getValueType()) {
			case OBJECT:
				String rootPathForObject = "";
				
				for (Entry<String, JsonValue> eachEntryInObject : jsonObjectToTraverse.asJsonObject().entrySet()) {
					
					if(rootPath.length() == 0) {
						rootPathForObject = eachEntryInObject.getKey();
					} else {
						rootPathForObject = rootPath + "." + eachEntryInObject.getKey();
					}
					
					getRecursivePaths(eachEntryInObject.getValue(), rootPathForObject, nestedPaths);
				}
				break;
				
			case ARRAY:
				JsonArray jArray = jsonObjectToTraverse.asJsonArray();
				
				for (JsonValue eachValueInArray : jArray) {
					getRecursivePaths(eachValueInArray, rootPath, nestedPaths);
				}
				break;
				
			case STRING:				
			case NUMBER:				
			case TRUE:				
			case FALSE:				
			case NULL:
				nestedPaths.add(rootPath);
				break;
				
			default:
				break;
		}
	}
}
