package com.github.marvin_c_offiah.data_utils;

import java.util.ArrayList;
import java.util.TreeMap;

public class SQLStringUtils {

    public static String[] toWildcardListStrings(TreeMap<String, Object> values) {
	if (values == null) {
	    return new String[] { "", "" };
	}
	ArrayList<String> keys = new ArrayList<String>();
	ArrayList<Object> vals = new ArrayList<Object>();
	for (String key : values.keySet()) {
	    if (key != null) {
		keys.add(key);
		vals.add(values.get(key));
	    }
	}
	return new String[] { toListString(keys.toArray(new String[keys.size()])),
		toWildcardListString(vals.toArray()) };
    }

    public static String toWildcardListString(Object[] values) {
	String str = "";
	if (values != null) {
	    for (int i = 0; i < values.length - 1; i++) {
		if (values[i] != null) {
		    str += "?,";
		}
	    }
	    if (values[values.length - 1] != null) {
		str += "?";
	    } else {
		str = str.length() == 0 ? "" : str.substring(0, str.length() - 1);
	    }
	}
	return str;
    }

    public static String toListString(Object[] keys) {
	String str = "";
	if (keys != null) {
	    for (int i = 0; i < keys.length - 1; i++) {
		if (keys[i] != null) {
		    str += keys[i] + ",";
		}
	    }
	    if (keys[keys.length - 1] != null) {
		str += keys[keys.length - 1];
	    } else {
		str = str.length() == 0 ? "" : str.substring(0, str.length() - 1);
	    }
	}
	return str;
    }

    public static enum AssignmentDelimiter {
	COMMA, AND, OR
    };

    public static String toWildcardAssignmentsString(TreeMap<String, Object> values, AssignmentDelimiter delimiter) {
	String result = "";
	if (values == null) {
	    return result;
	}
	String delim = "";
	switch (delimiter) {
	case COMMA:
	    delim = ",";
	case AND:
	    delim = "AND";
	case OR:
	    delim = "OR";
	default:
	    delim = ",";
	}
	String[] keysArray = values.keySet().toArray(new String[values.size()]);
	for (int i = 0; i < keysArray.length - 1; i++) {
	    if (keysArray[i] != null) {
		result += keysArray[i] + " = ? " + delim;
	    }
	}
	if (keysArray[keysArray.length - 1] != null) {
	    result += keysArray[keysArray.length - 1] + " = ? ";
	} else {
	    result = result.substring(0, result.lastIndexOf(delim));
	}
	return result;
    }

}
