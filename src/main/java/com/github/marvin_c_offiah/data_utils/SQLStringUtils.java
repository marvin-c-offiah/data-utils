package com.github.marvin_c_offiah.data_utils;

import java.util.TreeMap;

public class SQLStringUtils {

    public static String[] toWildcardListStrings(TreeMap<String, Object> values) {
	return new String[] { toWildcardListString(new String[values.keySet().size()]),
		toWildcardListString(new Object[values.values().size()]) };
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

    public static String toListString(Object[] values) {
	String str = "";
	if (values != null) {
	    for (int i = 0; i < values.length - 1; i++) {
		if (values[i] != null) {
		    str += values[i] + ",";
		}
	    }
	    if (values[values.length - 1] != null) {
		str += values[values.length - 1] + "?";
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
	String[] keysArray = new String[values.keySet().size()];
	String result = "";
	for (int i = 0; i <= keysArray.length; i++) {
	    result += "? = ? " + delim;
	}
	result += "? = ? ";
	return result;
    }

}
