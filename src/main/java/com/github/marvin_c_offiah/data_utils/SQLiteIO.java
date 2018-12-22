package com.github.marvin_c_offiah.data_utils;

import static com.github.marvin_c_offiah.data_utils.SQLStringUtils.toListString;
import static com.github.marvin_c_offiah.data_utils.SQLStringUtils.toWildcardAssignmentsString;
import static com.github.marvin_c_offiah.data_utils.SQLStringUtils.toWildcardListStrings;

import java.io.File;
import java.sql.Blob;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Observable;
import java.util.TreeMap;

import org.sqlite.SQLiteConnection;
import org.sqlite.jdbc4.JDBC4PreparedStatement;

import com.github.marvin_c_offiah.data_utils.SQLStringUtils.AssignmentDelimiter;

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

    public TreeMap<String, TreeMap<String, String>> getImportedKeys(String tableName) throws Exception {
	ResultSet rs = connection.getMetaData().getImportedKeys(null, null, tableName);
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

    public TreeMap<String, ArrayList<TreeMap<String, Object>>> getTables() throws Exception {
	ResultSet rs = connection.getMetaData().getTables(null, null, null, new String[] { "TABLE" });
	TreeMap<String, ArrayList<TreeMap<String, Object>>> tables = new TreeMap<String, ArrayList<TreeMap<String, Object>>>();
	while (rs.next()) {
	    String name = rs.getString("TABLE_NAME");
	    tables.put(name, selectColumns(name, null, null, true));
	}
	return tables;
    }

    public ArrayList<TreeMap<String, Object>> selectColumns(String tableName, TreeMap<String, Object> primaryKey,
	    String[] names, boolean includeRowId) throws Exception {
	checkTableName(tableName);
	if (primaryKey != null) {
	    checkColumnNames(tableName, primaryKey.keySet().toArray(new String[primaryKey.size()]));
	}
	checkColumnNames(tableName, names);
	String pkString = toWildcardAssignmentsString(primaryKey, AssignmentDelimiter.AND);
	String namesList = toListString(names);
	String colsList = (includeRowId ? "_rowid_ as _rowid_, " : "") + (namesList.equals("") ? "*" : namesList);
	JDBC4PreparedStatement statement = new JDBC4PreparedStatement(connection,
		"SELECT " + colsList + " FROM " + tableName + (pkString.equals("") ? "" : (" WHERE " + pkString)));
	ArrayList<SimpleEntry<String, Object>> pairs = new ArrayList<SimpleEntry<String, Object>>();
	if (!pkString.equals("")) {
	    for (String key : primaryKey.keySet()) {
		pairs.add(new SimpleEntry<String, Object>(key, primaryKey.get(key)));
	    }
	}
	executePreparedStatement(statement, tableName, pairs, false);
	ResultSet rs = statement.getResultSet();
	int colCount = rs.getMetaData().getColumnCount();
	names = new String[colCount];
	for (int i = 0; i < colCount; i++) {
	    names[i] = rs.getMetaData().getColumnName(i + 1);
	}
	ArrayList<TreeMap<String, Object>> selection = new ArrayList<TreeMap<String, Object>>();
	while (rs.next()) {
	    TreeMap<String, Object> row = new TreeMap<String, Object>();
	    for (String name : names) {
		row.put(name, rs.getObject(name));
	    }
	    selection.add(row);
	}
	return selection;
    }

    public void insertIntoTable(String name, TreeMap<String, Object> values) throws Exception {
	checkTableName(name);
	if (values != null) {
	    checkColumnNames(name, values.keySet().toArray(new String[values.size()]));
	}
	String[] valsLists = toWildcardListStrings(values);
	String colsList = valsLists[0];
	String valsList = valsLists[1];
	JDBC4PreparedStatement statement = new JDBC4PreparedStatement(connection, "INSERT INTO " + name
		+ (colsList.equals("") ? " DEFAULT VALUES" : ("(" + colsList + ") VALUES(" + valsList + ")")));
	ArrayList<SimpleEntry<String, Object>> pairs = new ArrayList<SimpleEntry<String, Object>>();
	for (String key : values.keySet()) {
	    pairs.add(new SimpleEntry<String, Object>(key, values.get(key)));
	}
	executePreparedStatement(statement, name, pairs);
	setChanged();
    }

    public void updateInTable(String name, TreeMap<String, Object> primaryKey, TreeMap<String, Object> line)
	    throws Exception {
	checkTableName(name);
	if (primaryKey != null) {
	    checkColumnNames(name, primaryKey.keySet().toArray(new String[primaryKey.size()]));
	}
	if (line == null || toListString(line.keySet().toArray(new String[line.size()])) == "") {
	    throw new IllegalArgumentException("The values to set must be provided for table \"" + name + "\".");
	}
	checkColumnNames(name, line.keySet().toArray(new String[line.size()]));
	String pkString = toWildcardAssignmentsString(primaryKey, AssignmentDelimiter.AND);
	JDBC4PreparedStatement statement = new JDBC4PreparedStatement(connection,
		"UPDATE " + name + " SET " + toWildcardAssignmentsString(line, AssignmentDelimiter.COMMA)
			+ (pkString.equals("") ? "" : (" WHERE " + pkString)));

	ArrayList<SimpleEntry<String, Object>> pairs = new ArrayList<SimpleEntry<String, Object>>();
	for (String key : line.keySet()) {
	    pairs.add(new SimpleEntry<String, Object>(key, line.get(key)));
	}
	if (!pkString.equals("")) {
	    for (String key : primaryKey.keySet()) {
		pairs.add(new SimpleEntry<String, Object>(key, primaryKey.get(key)));
	    }
	}
	executePreparedStatement(statement, name, pairs);
	setChanged();
    }

    public void deleteFromTable(String name, TreeMap<String, Object> primaryKey) throws Exception {
	checkTableName(name);
	if (primaryKey != null) {
	    checkColumnNames(name, primaryKey.keySet().toArray(new String[primaryKey.size()]));
	}
	String pkString = toWildcardAssignmentsString(primaryKey, AssignmentDelimiter.AND);
	JDBC4PreparedStatement statement = new JDBC4PreparedStatement(connection,
		"DELETE FROM" + name + (pkString.equals("") ? "" : (" WHERE " + pkString)));
	ArrayList<SimpleEntry<String, Object>> pairs = new ArrayList<SimpleEntry<String, Object>>();
	if (!pkString.equals("")) {
	    for (String key : primaryKey.keySet()) {
		pairs.add(new SimpleEntry<String, Object>(key, primaryKey.get(key)));
	    }
	}
	executePreparedStatement(statement, name, pairs);
	setChanged();
    }

    protected void checkTableName(String name) throws Exception {
	ResultSet allTables = connection.getMetaData().getTables(null, null, null, new String[] { "TABLE" });
	boolean tblFound = false;
	while (allTables.next()) {
	    if (allTables.getString("TABLE_NAME").equals(name)) {
		tblFound = true;
		break;
	    }
	}
	if (!tblFound) {
	    throw new IllegalArgumentException("Table \"" + name + "\" does not exist.");
	}
    }

    protected void checkColumnNames(String tableName, String[] names) throws Exception {
	String namesList = toListString(names);
	ArrayList<String> allColumns = new ArrayList<String>();
	if (!namesList.equals("")) {
	    ResultSet allCols = connection.getMetaData().getColumns(null, null, tableName, null);
	    while (allCols.next()) {
		allColumns.add(allCols.getString("COLUMN_NAME"));
	    }
	    allColumns.add("_rowid_");
	    for (String name : names) {
		if (name != null && !allColumns.contains(name)) {
		    throw new IllegalArgumentException(
			    "Column \"" + name + "\" does not exist in table \"" + tableName + "\".");
		}
	    }
	}
    }

    protected void executePreparedStatement(JDBC4PreparedStatement statement, String tableName,
	    ArrayList<SimpleEntry<String, Object>> values) throws Exception {
	executePreparedStatement(statement, tableName, values, true);
    }

    protected void executePreparedStatement(JDBC4PreparedStatement statement, String tableName,
	    ArrayList<SimpleEntry<String, Object>> values, boolean closeAfterExecution) throws Exception {
	String[] columnNames = new String[values.size()];
	for (int i = 0; i < values.size(); i++) {
	    columnNames[i] = values.get(i).getKey();
	}
	String columnsList = toListString(columnNames);
	if (!columnsList.equals("")) {
	    ResultSetMetaData metadata = connection.createStatement()
		    .executeQuery("SELECT " + columnsList + " FROM " + tableName + " WHERE 1 = 0").getMetaData();
	    TreeMap<String, String> columnTypes = new TreeMap<String, String>();
	    for (int i = 1; i <= metadata.getColumnCount(); i++) {
		columnTypes.put(metadata.getColumnName(i), metadata.getColumnTypeName(i));
	    }
	    for (int i = 1; i <= metadata.getColumnCount(); i++) {
		Object value = values.get(i - 1).getValue();
		String colType = columnTypes.get(metadata.getColumnName(i));
		if (colType.contains("INT")) {
		    statement.setInt(i, Integer.parseInt("" + value));
		    continue;
		}
		if (colType.contains("BOOLEAN ")) {
		    statement.setBoolean(i, Boolean.parseBoolean("" + value));
		    continue;
		}
		if (colType.contains("REAL") || colType.contains("DOUBLE") || colType.contains("FLOAT")) {
		    statement.setDouble(i, Double.parseDouble("" + value));
		    continue;
		}
		if (colType.contains("TEXT") || colType.contains("CHAR") || colType.contains("CLOB")) {
		    statement.setString(i, "" + value);
		    continue;
		}
		if (colType.contains("BLOB") || colType.equals("")) {
		    statement.setBlob(i, (Blob) value);
		    continue;
		}
		statement.setString(i, "" + value);
	    }
	}
	statement.execute();
	if (closeAfterExecution) {
	    statement.close();
	}
    }

}
