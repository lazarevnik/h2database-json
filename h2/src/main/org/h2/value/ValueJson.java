package org.h2.value;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Iterator;

import org.h2.message.DbException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.TextNode;

/**
 * 
 * @author laznik
 * Value implementation of json 
 */
public class ValueJson extends Value {

	private JsonNode jsonObj;
	private String jsonString;
	
	/**
	 * static mapper to optimize serialization
	 */
	private static final ObjectMapper mapper = new ObjectMapper();

		
	ValueJson(String str) throws IOException {
		char c = str.charAt(0);
		if ('{' == c) {
			ObjectNode node = (ObjectNode) mapper.readTree(str);
			this.jsonObj = node;
		} else if ('[' == c){
			ArrayNode node = (ArrayNode) mapper.readTree(str);
			this.jsonObj = node;
		} else {
			TextNode stringVal = new TextNode(str);
			this.jsonObj = (JsonNode) stringVal; 
		}
		this.jsonString = str;
	}
	
/*
	ValueJson(PGobject pg) throws IOException {
		assert pg.getType().equals("json");
		ValueJson json = get(pg.getValue());
		this.jsonObj = json.jsonObj;
		this.jsonString = json.jsonString;
	}
*/
	
	public ValueJson(JsonNode json) {
		this.jsonObj = json;
		this.jsonString = json.toString();
	}
	
	public static ValueJson get(String str) {
		try {
			JsonNode json = mapper.readTree(str);
			return new ValueJson(json);
		} catch (IOException e) {
			return new ValueJson(new TextNode("Serialization Error"));
		}
	}
	
	public static ValueJson get(JsonNode json) {
		return new ValueJson(json);
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
	public JsonNode getObject() {
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
	
	/*
	 * Unused
	 */
	public static Value getFieldAsText(Value v0, Value arg) throws IOException {
		JsonNode json;
		if(v0.getType() == Value.JSON) {
			json = ((ValueJson) v0).jsonObj;
		} else {
			json = get(v0.getString()).jsonObj;
		}
		if (json.isArray()) {
			int idx = -1;
			try {
				idx = arg.getInt();
			} catch (DbException e) {
				return ValueNull.INSTANCE;
			}
			if (json.has(idx)) {
				JsonNode res = json.get(idx);
				if (res.isTextual()) {
					return ValueString.get(res.asText());
				} else if (res.isInt()) {
					return ValueInt.get(res.asInt());
				} else {
					return ValueString.get(res.toString());
				}
			}
			return json.has(idx) ? new ValueString(json.get(idx).toString()) : ValueNull.INSTANCE;
		} else if (json.isObject()) {
			String key = arg.getString();
			if (json.has(key)) {
				JsonNode res = json.get(key);
				if (res.isTextual()) {
					return ValueString.get(res.asText());
				} else if (res.isInt()) {
					return ValueInt.get(res.asInt());
				} else {
					return ValueString.get(res.toString());
				}
			}
			return json.has(key) ? new ValueString(json.get(key).toString()) : ValueNull.INSTANCE;
		} else {
			return ValueNull.INSTANCE;
		}
	}

	/*
	 * Unused
	 */
	public static Value getFieldAsObject(Value v0, Value arg) throws DbException, IOException {
		JsonNode json = get(v0.getString()).jsonObj;
		if (json.isArray()) {
			int idx = -1;
			try {
				idx = arg.getInt();
			} catch (DbException e) {
				return ValueNull.INSTANCE;
			}
			return json.has(idx) ? new ValueJson(json.get(idx)) : ValueNull.INSTANCE;
		} else if (json.isObject()) {
			String key = arg.getString();
			return json.has(key) ? new ValueJson(json.get(key)) : ValueNull.INSTANCE;
		} else {
			return ValueNull.INSTANCE;
		}
	}

	/*
	 * Unused
	 */
	public static Value getFieldsAsText(Value v0, ValueArray args) throws IOException {
		JsonNode json = get(v0.getString()).jsonObj;
		JsonNode res = json;
		for (Value v : args.getList()) {
			if (res.isArray()) {
				int idx = -1;
				try {
					idx = v.getInt();
				} catch (DbException e) {
					return ValueNull.INSTANCE;
				}
				if (res.has(idx)) {
					res = res.get(idx);
				} else {
					return ValueNull.INSTANCE;
				}
			} else if (res.isObject()) {
				String key = "";
				try {
					key = v.getString();
				} catch (DbException e) {
					return ValueNull.INSTANCE;
				}
				if (res.has(key)) {
					res = res.get(key);
				} else {
					return ValueNull.INSTANCE;
				}
			} else {
				return ValueNull.INSTANCE;
			}
		}
		return ValueString.get(res.toString());
	}

	/*
	 * Unused
	 */
	public static Value getFieldsAsObject(Value v0, ValueArray args) throws IOException {
		JsonNode json = get(v0.getString()).jsonObj;
		JsonNode res = json;
		for (Value v : args.getList()) {
			if (res.isArray()) {
				int idx = -1;
				try {
					idx = v.getInt();
				} catch (DbException e) {
					return ValueNull.INSTANCE;
				}
				if (res.has(idx)) {
					res = res.get(idx);
				} else {
					return ValueNull.INSTANCE;
				}
			} else if (res.isObject()) {
				String key = "";
				try {
					key = v.getString();
				} catch (DbException e) {
					return ValueNull.INSTANCE;
				}
				if (res.has(key)) {
					res = res.get(key);
				} else {
					return ValueNull.INSTANCE;
				}
			} else {
				return ValueNull.INSTANCE;
			}
		}
		return new ValueJson(res);
	}

	/*
	 * Unused
	 */
	public static Value doesContains(Value v0, Value v1) throws IOException, DbException {
		JsonNode json1 = get(v0.getString()).jsonObj;
		JsonNode json2 = get(v1.getString()).jsonObj;
		if (json1.isArray() && json2.isArray()) {
			ArrayNode arr1 = (ArrayNode) json1;
			ArrayNode arr2 = (ArrayNode) json2;
			for( int i = 0; i < arr2.size(); i++) {
				boolean was = false;
				for(int j = 0; j < arr1.size(); j++) {
					if (arr1.get(j).equals(arr2.get(i))) {
						was = true;
						break;
					}
				}
				if (was) {
					continue;
				} else {
					return ValueBoolean.get(false);
				}
			}
			return ValueBoolean.get(true);
		} else if (json1.isObject() && json2.isObject()) {
			ObjectNode obj1 = (ObjectNode) json1;
			ObjectNode obj2 = (ObjectNode) json2;
			Iterator<String> names = json2.fieldNames();
			while (names.hasNext()) {
				String key = names.next();
				if (!obj1.has(key) || !obj2.get(key).equals(obj1.get(key))) {
					return ValueBoolean.get(false);
				}
			}
			return ValueBoolean.get(true);
		} else {
			return ValueBoolean.get(false);
		}
	}

	/*
	 * Unused
	 */
	public static Value hasKey(Value v0, Value v1) throws IOException, DbException {
		JsonNode json = get(v0.getString()).jsonObj;
		String key = v1.getString();
		if (json.isObject()) {
			return ValueBoolean.get(json.has(key));
		} else {
			return ValueBoolean.get(false);
		}
	}

	/*
	 * Unused
	 */
	public static Value hasAnyKey(Value v0, Value v1) throws DbException, IOException {
		JsonNode json = get(v0.getString()).jsonObj;
		if (v1.getType() == Value.ARRAY) {
			Value[] keys = ((ValueArray) v1).getList();
			for (Value key: keys) {
				if (json.has(key.getString())) {
					return ValueBoolean.get(true);
				}
			}
		} else if (v1.getType() == Value.JSON && ((ValueJson) v1).jsonObj.isArray()) {
			Iterator<JsonNode> arr = ((ValueJson) v1).jsonObj.elements();
			while (arr.hasNext()) {
				JsonNode n = arr.next();
				if (json.has(n.toString())) {
					return ValueBoolean.get(true);
				}
			}
			return ValueBoolean.get(false);
		}
		return ValueBoolean.get(false);
	}

	/*
	 * Unused
	 */
	public static Value hasKeys(Value v0, Value v1) throws DbException, IOException {
		JsonNode json = get(v0.getString()).jsonObj;
		if (v1.getType() == Value.ARRAY) {
			Value[] keys = ((ValueArray) v1).getList();
			for (Value key: keys) {
				if (!json.has(key.getString())) {
					return ValueBoolean.get(false);
				}
			}
			return ValueBoolean.get(true);
		} else if (v1.getType() == Value.JSON && ((ValueJson) v1).jsonObj.isArray()) {
			Iterator<JsonNode> arr = ((ValueJson) v1).jsonObj.elements();
			while (arr.hasNext()) {
				JsonNode n = arr.next();
				if (json.has(n.toString())) {
					return ValueBoolean.get(false);
				}
			}
			return ValueBoolean.get(true);
		}
		return ValueBoolean.get(false);
	}

	/*
	 * Unused
	 */
	public static Value concatenate(Value v0, Value v1) throws DbException, IOException {
		JsonNode json1 = get(v0.getString()).jsonObj;
		JsonNode json2 = get(v1.getString()).jsonObj;
		if (json1.isObject()) {
			ObjectNode obj1 = (ObjectNode) json1;
			if (json2.isObject()) {
				Iterator<String> names = json2.fieldNames();
				while(names.hasNext()) {
					String key = names.next();
					JsonNode val = json2.get(key);
					obj1.put(key, val);
				}
				return new ValueJson(obj1);
			} else if (json2.isArray()) {
				ObjectMapper mapper = new ObjectMapper();
				ArrayNode res = mapper.createArrayNode();
				res.add(obj1);
				res.addAll((ArrayNode) json2);
				return new ValueJson(res);
			} else {
				throw new IOException();
			}
		} else if (json1.isArray()) {
			ArrayNode arr1 = (ArrayNode) json1;
			if (json2.isArray()) {
				arr1.addAll((ArrayNode) json2);
			} else {
				arr1.add(json2);
			}
			return new ValueJson(arr1);
		} else if (json2.isArray()) {
			ArrayNode arr2 = (ArrayNode) json2;
			arr2.add(json1);
			return new ValueJson(arr2);
		} else {
			ObjectMapper mapper = new ObjectMapper();
			ArrayNode arr = mapper.createArrayNode();
			arr.add(json1);
			arr.add(json2);
			return new ValueJson(arr);
		}
	}

	/*
	 * Unused
	 */
	public static Value deleteKey(Value v0, Value v1) throws DbException, IOException {
		JsonNode json = get(v0.getString()).jsonObj;
		if (json.isObject()) {
			String key = v1.getString();
			if (json.has(key)) {
				((ObjectNode) json).remove(key);
				return new ValueJson(json);
			}
		} else if (json.isArray()) {
			int idx = -1;
			try {
				idx = v1.getInt();
			} catch (DbException e) {
				return v0;
			}
			if ( json.has(idx)) {
				((ArrayNode) json).remove(idx);
				return new ValueJson(json);
			}
		}
		return v0;
	}
	

	/*
	 * Unused
	 */
	public static Value deleteKeys(Value v0, ValueArray v1) throws DbException, IOException {
		for (Value v: v1.getList()) {
			v0 = deleteKey(v0, v);
		}
		return v0;
	}
}
