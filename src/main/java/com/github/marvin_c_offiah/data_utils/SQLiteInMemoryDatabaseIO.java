package com.github.marvin_c_offiah.data_utils;

import java.io.File;
import java.sql.DriverManager;

import org.sqlite.SQLiteConnection;

public class SQLiteInMemoryDatabaseIO {

    protected String filePath;

    public SQLiteInMemoryDatabaseIO(String filePath) throws Exception {
	if (filePath == null)
	    throw new Exception("The file path for the database must be provided.");
	File f = new File(filePath);
	if ((f.isDirectory() || !f.exists()))
	    throw new Exception("Invalid file path for database: \"" + filePath + "\". File must exist.");
	this.filePath = filePath;
    }

    public SQLiteInMemoryDatabase read() throws Exception {
	SQLiteConnection connection = (SQLiteConnection) DriverManager.getConnection("jdbc:sqlite::memory:");
	connection.createStatement().executeUpdate("restore from " + filePath);
	return new SQLiteInMemoryDatabase(connection);
    }

    public void write(SQLiteIO database) throws Exception {
	database.getConnection().createStatement().executeUpdate("backup to " + filePath);
    }

}
