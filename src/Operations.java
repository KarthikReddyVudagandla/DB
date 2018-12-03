import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;

public class Operations {
	static long FILESIZE = 512;
	
	public static void parseDelete(String userCommand) {
		// DELETE FROM table_name [WHERE condition];
		String[] splitOnWhere = userCommand.split("where");
		if (splitOnWhere.length < 2) {
			System.out.println("Missing Where clause.");
			return;
		}
		String[] condition = splitOnWhere[1].trim().split(" ");
		String[] queryString = splitOnWhere[0].trim().split(" ");
		String tableName = queryString[queryString.length - 1];
		try {
			Stub.delete("data/user_data", tableName, condition);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void parseShow(String userCommand) {
		// SHOW TABLES;
		if (userCommand.split(" ")[1].trim().equalsIgnoreCase("tables")) {
			String[] columnNames = new String[] { "*" };
			String[] condition = new String[0];
			try {
				Stub.select("data/catalog", "davisbase_tables", columnNames, condition);
			} catch (Exception e) {
				System.out.println("Error: Couldn't show tables");
				e.printStackTrace();
			}
		} else
			System.out.println("Unknown command: " + userCommand);
	}

	public static void dropTable(String dropTableString) {
		// DROP TABLE table_name;
		String[] tokens = dropTableString.split(" ");
		try {
			Stub.dropTable("data/user_data", tokens[tokens.length - 1].trim());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Stub method for executing queries
	 *
	 * @param queryString
	 *            is a String of the user input
	 */
	public static void parseQuery(String queryString) {
		// SELECT [col_names] FROM table_name [WHERE condition];
		String[] splitOnWhere = queryString.split("where");
		String querySplit[] = splitOnWhere[0].trim().split(" ");
		String tableName = querySplit[querySplit.length - 1];

		String[] cols = splitOnWhere[0].trim().split("from")[0].trim().replace("select", "").split(",");
		String[] columnNames = new String[cols.length];
		for (int i = 0; i < cols.length; i++)
			columnNames[i] = cols[i].trim();

		String[] condition = new String[0];
		if (splitOnWhere.length > 1) {
			condition = splitOnWhere[1].trim().split(" ");
		}
		try {
			if (tableName.contains("davisbase"))
				Stub.select("data/catalog", tableName, columnNames, condition);
			else
				Stub.select("data/user_data", tableName, columnNames, condition);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Stub method for updating records
	 *
	 * @param updateString
	 *            is a String of the user input
	 */
	public static void parseUpdate(String updateString) {
		// UPDATE table_name SET column_name = value [WHERE condition]
		String[] splitOnWhere = updateString.split("where");
		if (splitOnWhere.length < 2) {
			System.out.println("Missing where clause. Can not update without where claues.");
			return;
		}
		String[] condition = splitOnWhere[1].trim().split(" ");
		String[] splitOnSet = splitOnWhere[0].trim().split("set");
		String tableName = splitOnSet[0].trim().split(" ")[1];
		String[] data = splitOnSet[1].trim().split(" ");
		
//		for(int i=0;i< data.length ; i++)
//		{
//			System.out.println("@@@"+data[i]);
//		}

		if (tableName.equalsIgnoreCase("davisbase_tables") || tableName.equalsIgnoreCase("davisbase_columns")) {
			System.out.println("can not update Meta tables.");
			return;
		}
		try {
			Stub.update("data/user_data", tableName, data, condition);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Stub method for updating records
	 *
	 * @param insertString
	 *            is a String of the user input
	 */
	public static void parseInsert(String insertString) {
		// INSERT INTO table_name [column_list] VALUES value_list
		ArrayList<String> createTableTokens = new ArrayList<String>(Arrays.asList(insertString.split(" ")));

		/* Define table file name */
		String tableName = createTableTokens.get(2);
		String[] temp = insertString.replaceAll("\\(", " ").replaceAll("\\)", " ").split("values");
		String[] values = temp[1].trim().split(",");
		String[] temp2 = temp[0].trim().split(" "+tableName+" ");
		String[] columnNames = temp2[1].trim().split(",");

		try {
			for (int i = 0; i < values.length; i++)
			{	
				values[i] = values[i].trim();
				System.out.println("values---"+values[i]);
			}

			for (int i = 0; i < columnNames.length; i++)
			{
				columnNames[i] = columnNames[i].trim();
				System.out.println("col.names---"+columnNames[i]);
			}	

			Stub.insert("data/user_data", tableName, columnNames, values);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Stub method for creating new tables
	 *
	 * @param createTableString
	 *            is a String of the user input
	 */
	public static void parseCreateTable(String createTableString) {
		
		//TODO create index files....parse separately fr it....
		// CREATE TABLE table_name ( row_id INT, column_name2 data_type2 [NOT NULL],
		// column_name3 data_type3 [NOT NULL], ...)
		ArrayList<String> createTableTokens = new ArrayList<String>(Arrays.asList(createTableString.split(" ")));
		
		/* Define table file name */
		String tableName = createTableTokens.get(2);
		System.out.println("create table string---"+ createTableString);
		/* YOUR CODE GOES HERE */
		String[] temp = createTableString.replaceAll("\\(", " ").replaceAll("\\)", " ").split(" "+tableName+" ");
		for(int i=0;i<temp.length;i++)
			System.out.println("temp---"+temp[i]);
		String[] columnNames = temp[1].trim().split(",");
		
		try {
			if (columnNames.length < 1)
				throw new Exception("Can not create a table with no columns.");
			String[] col1_Details = columnNames[0].split(" ");
			if(!col1_Details[0].trim().equalsIgnoreCase("row_id") || col1_Details[col1_Details.length-1].trim().equalsIgnoreCase("NULL"))
				throw new Exception("Format Error: Primary column should be 'row_id INT' and cannot be 'NULL' ");

			/* Code to create a .tbl file to contain table data */
			
			System.out.println("creating davisbase_tables table");
			RandomAccessFile binaryFile = new RandomAccessFile("data/user_data/"+ tableName +".tbl", "rw");
			binaryFile.setLength(0);
			binaryFile.setLength(FILESIZE);
			
			binaryFile.seek(0);
			binaryFile.writeByte(0x0D);
			
			binaryFile.seek(1);
			binaryFile.writeByte(0x00);

			binaryFile.seek(2);
			binaryFile.writeShort(512);

			binaryFile.seek(4);
			binaryFile.writeInt(0xFFFFFFFF);
			
			binaryFile.close();
			//insert table info into meta tables
			for(int i=0;i<columnNames.length;i++)
				System.out.println("col names---"+columnNames[i]);
			
			Stub.InsertToMeta("data/user_data", tableName, columnNames);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void parseCreateIndex(String userCommand) {
		// TODO Auto-generated method stub
		System.out.println(userCommand);
	}
}
