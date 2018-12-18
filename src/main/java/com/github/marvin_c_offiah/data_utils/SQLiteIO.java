package com.github.marvin_c_offiah.data_utils;

import static com.github.marvin_c_offiah.data_utils.SQLStringUtils.toWildcardAssignmentsString;
import static com.github.marvin_c_offiah.data_utils.SQLStringUtils.toWildcardListStrings;

import java.io.File;
import java.sql.Blob;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Observable;
import java.util.TreeMap;

import org.sqlite.SQLiteConnection;
import org.sqlite.core.Codes;
import org.sqlite.jdbc4.JDBC4PreparedStatement;

public class SQLiteIO extends Observable {

    protected SQLiteConnection connection;

    public SQLiteIO() throws Exception {
	connection = (SQLiteConnection) DriverManager.getConnection("jdbc:sqlite:");
    }

    public SQLiteIO(String filePath) throws Exception {
	if (filePath == null)
	    throw new Exception("The file path for the database must be provided.");
	File f = new File(filePath);
	if (f.isDirectory() || !f.exists())
	    throw new Exception("Invalid file path for database: \"" + filePath + "\". File must exist.");
	this.connection = (SQLiteConnection) DriverManager.getConnection("jdbc:sqlite:" + filePath);
    }

    public SQLiteConnection getConnection() {
	return connection;
    }

    public String[] getPrimaryKey(String tableName) throws Exception {
	ResultSet rs = connection.getMetaData().getPrimaryKeys(null, null, tableName);
	ArrayList<String> key = new ArrayList<String>();
	while (rs.next()) {
	    key.add(rs.getString("COLUMN_NAME"));
	}
	return key.toArray(new String[0]);
    }

    public TreeMap<String, TreeMap<String, String>> getImportedKeys(String table) throws Exception {
	ResultSet rs = connection.getMetaData().getImportedKeys(null, null, table);
	TreeMap<String, TreeMap<String, String>> keys = new TreeMap<String, TreeMap<String, String>>();
	while (rs.next()) {
	    String pkTblName = rs.getString("PKTABLE_NAME");
	    if (keys.get(pkTblName) == null) {
		keys.put(pkTblName, new TreeMap<String, String>());
	    }
	    keys.get(pkTblName).put(rs.getString("PKCOLUMN_NAME"), rs.getString("FKCOLUMN_NAME"));
	}
	return keys;
    }

    public TreeMap<String, TreeMap<String, Object[]>> getTables() throws Exception {
	ResultSet rs = connection.getMetaData().getTables(null, null, null, new String[] { "TABLE" });
	TreeMap<String, TreeMap<String, Object[]>> tables = new TreeMap<String, TreeMap<String, Object[]>>();
	while (rs.next()) {
	    String name = rs.getString("TABLE_NAME");
	    tables.put(name, selectColumns(name, null, true));
	}
	return tables;
    }

    public TreeMap<String, Object[]> selectColumns(String tableName,
			String[] names, boolean includeRowId) throws Exception {
		String colNames = (includeRowId ? "_rowid_ as _rowid_, " : "")
				+ (names == null || names.length == 0 ? "*"
						: toWildcardListString(values)(names));
		
		
		JDBC4PreparedStatement statement = new JDBC4PreparedStatement(
			connection,
			"SELECT " + toWildcardListString(names) + " FROM ?");
		
		
		
		ResultSet rs = connection.createStatement()
				.executeQuery("SELECT " + colNames + " FROM " + tableName);
		TreeMap<String, Object[]> selection = new TreeMap<String, Object[]>();
		if (names == null) {
			int colCount = rs.getMetaData().getColumnCount();
			names = new String[colCount];
			for (int i = 0; i < colCount; i++) {
				names[i] = rs.getMetaData().getColumnName(i + 1);
			}
		}
		TreeMap<String, ArrayList<Object>> colVals = new TreeMap<String, ArrayList<Object>>();
		while (rs.next()) {
			for (String name : names) {
				if (colVals.get(name) == null)
					colVals.put(name, new ArrayList<Object>());
				colVals.get(name).add(rs.getObject(name));
			}
		}
		for (String name : names)
			selection.put(name, colVals.get(name).toArray());
		return selection;
	}

    public void insertIntoTable(String name, TreeMap<String, Object> values) throws Exception {

	Object[] valsStrings = null;
	TreeMap<String, Object> wildcards = null;
	Object[] wildcardStrgs = null;
	if (!(values == null || values.isEmpty())) {
	    valsStrings = toWildcardListStrings(values);
	    wildcards = new TreeMap<String, Object>();
	    for (int i = 0; i < values.size(); i++) {
		wildcards.put("" + i, "?");
	    }
	    wildcardStrgs = toListStrings(wildcards);
	}

	JDBC4PreparedStatement statement = new JDBC4PreparedStatement(connection, "INSERT INTO ?" + (valsStrings == null
		? " DEFAULT VALUES" : ("(" + wildcardStrgs[0] + ") VALUES(" + wildcardStrgs[1] + ")")));

	if (valsStrings != null) {

	    JDBC4PreparedStatement metaStmnt = new JDBC4PreparedStatement(connection, "SELECT * FROM ? WHERE 1 = 0");
	    metaStmnt.setString(0, name);
	    ResultSetMetaData metadata = metaStmnt.executeQuery().getMetaData();
	    TreeMap<String, Integer> columnTypes = new TreeMap<String, Integer>();
	    for (int i = 0; i < metadata.getColumnCount(); i++) {
		columnTypes.put(metadata.getColumnName(i + 1), metadata.getColumnType(i + 1));
	    }

	    statement.setString(0, name);
	    String[] columnNames = values.keySet().toArray(new String[0]);
	    for (int i = 1; i <= columnNames.length; i++) {
		statement.setString(i, columnNames[i]);
	    }
	    for (int i = columnNames.length + 1; i <= columnNames.length * 2; i++) {
		Object value = values.get(columnNames[i]);
		switch (columnTypes.get(columnNames[i])) {
		case Codes.SQLITE_INTEGER:
		    statement.setInt(i, (Integer) value);
		    break;
		case Codes.SQLITE_FLOAT:
		    statement.setFloat(i, (Float) value);
		    break;
		case Codes.SQLITE_TEXT:
		    statement.setString(i, (String) value);
		    break;
		case Codes.SQLITE_BLOB:
		    statement.setBlob(i, (Blob) value);
		}
	    }
	}

	statement.execute();
	statement.close();

	setChanged();

    }

    public void updateInTable(String name, TreeMap<String, Object> primaryKey, TreeMap<String, Object> line)
	    throws Exception {
	connection.createStatement().executeQuery("UPDATE ? SET " + toWildcardAssignmentsString(line, COMMA) + " WHERE "
		+ toWildcardAssignmentsString(primaryKey));
	Object[] valsStrings = null;
	TreeMap<String, Object> wildcards = null;
	Object[] wildcardStrgs = null;
	if (!(values == null || values.isEmpty())) {
	    valsStrings = toListStrings(values);
	    wildcards = new TreeMap<String, Object>();
	    for (int i = 0; i < values.size(); i++) {
		wildcards.put("" + i, "?");
	    }
	    wildcardStrgs = toListStrings(wildcards);
	}
	JDBC4PreparedStatement statement = new JDBC4PreparedStatement(connection,
		"UPDATE " + name + " SET " + wildcardStrgs + ")");
	setChanged();
    }

    public void deleteFromTable(String name, TreeMap<String, Object> primaryKey) throws Exception {
	connection.createStatement().executeQuery("DELETE FROM " + name + " WHERE " + toAssignmentsString(primaryKey));
	setChanged();
    }

}
