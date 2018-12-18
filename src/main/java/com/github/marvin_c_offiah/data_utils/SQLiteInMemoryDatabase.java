package com.github.marvin_c_offiah.data_utils;

import org.sqlite.SQLiteConnection;

public class SQLiteInMemoryDatabase extends SQLiteIO {

    public SQLiteInMemoryDatabase(SQLiteConnection connection) throws Exception {
	super();
	if (connection == null)
	    throw new Exception("The database connection must be provided.");
	String url = connection.getMetaData().getURL();
	if (!(url.equals("jdbc:sqlite:") || url.equals("jdbc:sqlite::memory:"))) {
	    throw new Exception("Invalid url in database connection: \"" + url
		    + "\". The connection must be an in-memory SQLite connection.");
	}
	this.connection = connection;
    }

}
