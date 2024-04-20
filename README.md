## TinyDB 
### A Simplified Database Engine in Java with B+ Tree Indexing

TinyDB is a lightweight, in-memory database engine designed for educational and demonstration purposes. It offers basic functionalities for managing data in tables and leverages B+ trees for efficient searching.

---

### Features

* **Table Creation:** Define table schemas with column names, data types, and a primary key (clustering column).
* **Data Manipulation:** Insert, update, and delete rows (tuples) within tables.
* **B+ Tree Indexing:** Create B+ tree indexes on specific table columns to enhance search performance.
* **Index Utilization:** When applicable, TinyDB utilizes B+ tree indexes for efficient search queries.
* **Basic Selection:** Perform selections on tables using a simplified subset of SQL terms (e.g., equality comparisons).

---

### Note

TinyDB is a simplified engine and does not currently support:

* **Foreign Keys and Referential Integrity:** Enforcing relationships between tables.
* **Joins:** Combining data from multiple tables based on conditions.
* **Transactions:** Guaranteeing data consistency across multiple operations.
* **Complex SQL Queries:** Supports only a limited subset of SQL terms (e.g., no JOINs, aggregations).

---

### Usage

**1. Initialization:**

Call the `init()` method to perform any necessary initialization tasks.

**2. Create Table:**

Use the `createTable` method to define a new table. Specify the table name `strTableName`, primary key `strClusteringKeyColumn` name, and a hashtable `htblColNameType` mapping column names to their corresponding data types.

```java
Hashtable<String, String> colNameType = new Hashtable<>();
colNameType.put("id", "int");
colNameType.put("name", "string");
db.createTable("students", "id", colNameType);
```

**3. Create Index (Optional):**

This method allows you to create B+ tree indexes on specific table columns for faster searching.

```java
db.createIndex(String strTableName, String strColName, String strIndexName);
```

**Parameters:**

`strTableName`: The name of the table you want to create the index on.
`strColName`: The name of the column you want to create the index on.
`strIndexName`: A desired name for the index. 

**4. Data Operations:**

Insert Tuples: Insert a new row into a table using `insertIntoTable`. The provided `htblColNameValue` hashtable must include a value for the primary key.

```java
Hashtable<String, Object> rowData = new Hashtable<>();
rowData.put("id", 1);
rowData.put("name", "Alice");
db.insertIntoTable("students", rowData);
```

Update Tuples: Modify an existing row using `updateTable`. Specify the value of the primary key `strClusteringKeyValue` to identify the row and provide a hashtable `htblColNameValue` containing the new values for other columns (excluding the primary key).

```java
rowData = new Hashtable<>();
rowData.put("name", "Charlie");
db.updateTable("students", "1", rowData); // Update student with ID 1
```

Delete Tuples: Delete rows from a table using `deleteFromTable`. The `htblColNameValue` hashtable defines the search criteria (AND operation) for identifying rows to delete.

```java
rowData = new Hashtable<>();
rowData.put("name", "Alice");
db.deleteFromTable("students", rowData);
```

**5. Selection (Simplified)**:

Perform basic selections on tables using the `selectFromTable` method. It accepts an array of `SQLTerm` objects representing selection conditions and an array of operators `strarrOperators` specifying the logical relationship between terms (currently supports AND, OR and XOR operations). The method returns an iterator to iterate through the selected rows.

Please note that the current implementation supports a simplified subset of SQL terms.


It is important to note that the standard precedence of operators is applied, that is, AND is the highest in precedence followed by XOR and then OR.

For example this java code
```java
SQLTerm[] arrSQLTerms = new SQLTerm[3];
String[] strarrOperators = new String[2];
arrSQLTerms[0] = new SQLTerm();
arrSQLTerms[1] = new SQLTerm();
arrSQLTerms[2] = new SQLTerm();
arrSQLTerms[0]._strTableName = "Student";
arrSQLTerms[0]._strColumnName = "gpa";
arrSQLTerms[0]._strOperator = "<=";
arrSQLTerms[0]._objValue = new Double(4);
arrSQLTerms[1]._strTableName = "Student";
arrSQLTerms[1]._strColumnName = "gpa";
arrSQLTerms[1]._strOperator = "<=";
arrSQLTerms[1]._objValue = new Double(7);
arrSQLTerms[2]._strTableName = "Student";
arrSQLTerms[2]._strColumnName = "gpa";
arrSQLTerms[2]._strOperator = ">=";
arrSQLTerms[2]._objValue = new Double(5);
strarrOperators[0] = "OR";
strarrOperators[1] = "AND";
System.out.println(dbApp.selectFromTable(arrSQLTerms,strarrOperators));
```

is equivalent to the following SQL statement
```SQL
SELECT * FROM Student WHERE gpa <= 4 OR (gpa <= 7 AND gpa >= 5);
```