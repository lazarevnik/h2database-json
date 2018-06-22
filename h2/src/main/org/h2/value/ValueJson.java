package org.h2.value;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.TextNode;


public class ValueJson extends Value {

	JsonNode jsonObj;
	String jsonString;

	
	ValueJson (String str) throws IOException {
		char c = str.charAt(0);
		if ('{' == c) {
			ObjectMapper mapper = new ObjectMapper();
			ObjectNode node = (ObjectNode) mapper.readTree(str);
			this.jsonObj = node;
		} else if ('[' == c){
			ObjectMapper mapper = new ObjectMapper();
			ArrayNode node = (ArrayNode) mapper.readTree(str);
			this.jsonObj = node;
		} else {
			TextNode stringVal = new TextNode(str);
			this.jsonObj = (JsonNode) stringVal; 
		}
		this.jsonString = str.replaceAll(" ", "").replaceAll("\n","");
	}
	
	ValueJson(JsonNode json) {
		this.jsonObj = json;
		this.jsonString = json.toString().replaceAll(" ", "").replaceAll("\n","");
	}
	
	public static ValueJson get(String str) throws IOException {
		return new ValueJson(str);
	}
	
	@Override
	public Value convertTo(int targetType){
		switch(targetType){
		case Value.JAVA_OBJECT:
			return ValueJavaObject.getNoCopy(jsonObj, null, null);
		case Value.STRING:
			return new ValueString(jsonString);
		default:
			return super.convertTo(targetType); 
		}
	}
	
	@Override
	public String getSQL() {
		return "'" + getString() + "'::JSON";
	}

	@Override
	public int getType() {
		return Value.JSON;
	}

	@Override
	public long getPrecision() {
		return 0;
	}

	@Override
	public int getDisplaySize() {
		return 0;
	}

	@Override
	public String getString() {
		return jsonString;
	}

	@Override
	public Object getObject() {
		return jsonObj;
	}

	@Override
	public void set(PreparedStatement prep, int parameterIndex)
			throws SQLException {
		// TODO Auto-generated method stub

	}

	@Override
	protected int compareSecure(Value v, CompareMode mode) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int hashCode() {
		return jsonObj.hashCode();
	}

	@Override
	public boolean equals(Object other) {
		if (other instanceof ValueJson) {
			return this.jsonObj.equals(((ValueJson) other).getObject());
		} else if (other instanceof JsonNode) {
			return this.jsonObj.equals(other);
		}
		return false;
	}	
	
	public static Value getTextField(Value v0, Value v1) throws IOException {
		if(v0 instanceof ValueJson) {
			JsonNode json = ((ValueJson) v0).jsonObj;
			String tag = v1.getString();
			return json.has(v1.getString()) ? ValueString.get(json.get(tag).toString()) : ValueNull.INSTANCE;
		} else if (v0.getType() == Value.STRING) {
			JsonNode json = get(v0.getString()).jsonObj;
			String tag = v1.getString();
			return json.has(v1.getString()) ? ValueString.get(json.get(tag).toString()) : ValueNull.INSTANCE;
		}
		return ValueNull.INSTANCE;
	}

}
