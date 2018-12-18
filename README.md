# Data Utils
Data Utils provides a set of data handling utilities.

## SQLite data handling

* Wrappers around `org.sqlite.SQLiteConnection` for simplifying instantiation and use of SQLite database connections by file path only, including in-memory copies of such databases.
  * To obtain an in-memory copy of your DB-file, simply type:
    * `SQLiteInMemoryDatabaseIO dbIO = new SQLiteInMemoryDatabaseIO(<path-to-your-db-file>);`
    * `SQLiteInMemoryDatabase database = dbIO.read();`
  * Make your changes using the interfaces listed below.
  * To save the database from memory back to the DB-file:
    * `dbIO.write();`

* Wrappers provide convenience interfaces that allow simple IO-access to SQlite database tables. DML/DQL commands are masked by programmer-friendly interfaces that use known Java datatypes as parameters or return values only (Strings and TreeMaps).
  * Get and use entries from all tables of the database in a TreeMap that contains one table per map entry, again as a TreeMap:
    * `TreeMap<String, TreeMap<String, Object[]>> allTables = database.getTables();`
    * `TreeMap<String, Object[]> artistsTable = allTables.get("artists");`
    * `System.out.println("Artist number 5 is " + artistsTable.get("name")[5] + " and was born in " + artistsTable.get("year_of_birth")[5] + ".";`
  * Insert a new row into a table:
    * `TreeMap<String, Object> michaelJackson = new TreeMap<String, Object>();`
    * `michaelJackson.put("name", "Michael Jackson");`
    * `michaelJackson.put("year_of_birth", 1958);`
    * `database.insertIntoTable("artists", michaelJackson);`
  * Similar interfaces for update, delete etc.

* Wrappers provide safety from SQL-injection by checking validity of table/column names and using SQL Prepared Statements.
