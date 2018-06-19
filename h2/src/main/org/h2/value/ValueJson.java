package org.h2.value;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.TextNode;
import com.fasterxml.jackson.databind.node.ValueNode;


public class ValueJson extends Value {

//	JSONObject jsonObj;
	JsonNode jsonObj;
	String jsonString;
//	byte[] bytes;
	
	
	public ValueJson(String str) {// throws IOException {

		System.out.println("[MY ERROR] H2 ValueJson Constructor: getted  " + str);
		System.out.println("[MY ERROR] H2 ValueJson Constructor: getted (bytes) " + str.getBytes().clone());
//		JsonFactory fact = new JsonFactory();
		ObjectMapper mapper = new ObjectMapper();
		JsonNode json = null;
		if(!str.contains("{")) {
			try {
				TextNode val = new TextNode(str);
				json = (JsonNode) val;
				System.out.println("[MY ERROR] H2 ValueJson Constructor: TextNode " + val + " DONE");
			} catch (Exception e) {
				System.out.println("[MY ERROR] H2 ValueJson Constructor: cannot do TextNode " + str);
				e.printStackTrace();
//				throw e;
			}
		} else {
			try {
				json = mapper.readTree(str.getBytes());
			} catch (Exception e) {
				System.out.println("[MY ERROR] H2 ValueJson Constructor: cannot parse " + str);
				e.printStackTrace();
//				throw e;
			}
		}
		this.jsonObj = json;
		this.jsonString = str;
	}
//	public ValueJson(JsonNode json){
//		this.jsonObj = json;
//		this.jsonString = json.toString();
//	}
	
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
		}
		if( other instanceof JsonNode) {
			return this.jsonObj.equals(other);
		}
		return false;
	}
	public static Value getTextField(ValueJson v1, ValueString v2) {
		JsonNode json = v1.jsonObj;
		String tag =  v2.getString();
		return json.has(tag) ? ValueString.get(json.findValue(tag).asText()) : ValueString.get("");
	}
	
	
	public static Value getTextField(Value v0, Value v1) throws IOException {
		System.out.println("[MY COMMENT] H2 ValueJson.getTextField(value, value); v0.toString() = " + v0.toString() + " v1 = " + v1.toString());
		System.out.println("[MY COMMENT] H2 ValueJson.getTextField(value, value); v0.getString() = " + v0.getString() + " v1 = " + v1.getString());
		if(v0 instanceof ValueJson) {
			JsonNode json = ((ValueJson) v0).jsonObj;
			return json.has(v1.getString()) ? ValueString.get(json.get(v1.toString()).asText()) : ValueString.get("");
		} else if (v0 instanceof ValueString) {
			ObjectMapper mapper = new ObjectMapper();
			JsonNode json = mapper.readTree(v0.getString());
			return json.has(v1.getString()) ? ValueString.get(json.get(v1.getString()).asText()) : ValueString.get("");
		}
		return ValueString.get("");
	}

}
