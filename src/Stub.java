import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

public class Stub {
	private static final int FILESIZE = 512;
	
	static void InitTable() throws IOException{
		//create directories 
		File catalog = new File("data/catalog");
		catalog.mkdirs();
		
		File user_data = new File("data/user_data");
		user_data.mkdirs();
		
		if(!tableExists("data/catalog","davisbase_tables")) {
			System.out.println("creating davisbase_tables table");
			RandomAccessFile binaryFile = new RandomAccessFile("data/catalog/davisbase_tables.tbl", "rw");
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
			
		}else {
			System.out.println("davisbase_tables already exists");
		}
		
		if(!tableExists("data/catalog","davisbase_columns")) {
			System.out.println("creating davisbase_columns table");
			RandomAccessFile binaryFile= new RandomAccessFile("data/catalog/davisbase_columns.tbl", "rw");
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
			
			
			try {
				InsertToMeta("data/catalog","davisbase_tables", new String[] { "row_id int", "table_name text" });
			} catch (Exception e) {
				// TODO Auto-generated catch block
				System.out.println("Error: Couldn't Insert To Meta");
				e.printStackTrace();
			}
			
			try {
				InsertToMeta("data/catalog","davisbase_columns", new String[] { "row_id int", "table_name text",
						"column_name text", "data_type text", "ordinal_position int", "is_nullable text" });
			} catch (Exception e) {
				// TODO Auto-generated catch block
				System.out.println("Error: Couldn't Insert To Meta");
				e.printStackTrace();
			}
					
		}else {
			System.out.println("davisbase_columns already exists");
		}
		
				
	}
	
	static Boolean tableExists(String path,String tableName) {
		
		File tablefile = new File (path + "/" + tableName + ".tbl");
		//System.out.println("table exists-->"+tablefile.exists());
		return tablefile.exists();
		
	}
	
	public static void InsertToMeta(String path, String tableName, String[] columns){
		try {
			System.out.println("Insert to meta--try "+ tableName);
			InsertToMetaTables(tableName);
			for (int i = 0; i < columns.length; i++) {
				String[] columnDetails = columns[i].trim().split(" ");
				String[] columnMeta = new String[6];
				columnMeta[0] = "0"; //for row_id...calculated from table...not from input
				columnMeta[1] = tableName;
				columnMeta[2] = columnDetails[0];
				columnMeta[3] = columnDetails[1];
				columnMeta[4] = String.valueOf(i + 1);
				
				if (columnDetails.length == 3 && columnDetails[2].equalsIgnoreCase("null"))
					columnMeta[5] = "YES";
				else
					columnMeta[5] = "NO";
				InsertToMetaColumns(columnMeta);
			}
			
		} catch (Exception e) {
			System.out.println("Insert to meta--catch "+ tableName);
			System.out.println("Failed to create Table.");
		}
	}	
	private static void InsertToMetaTables(String tableName){
		String path = "data/catalog";
		Table metaTable = new Table();
		try {
			metaTable.initTable(path, "davisbase_tables");
		    metaTable.InsertDataToMeta(new String[] { "row_id", "tableName" },
				new String[] { String.valueOf(metaTable.fetchNextRowID()), tableName });
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println("Error occured while Insert to meta tables");
			e.printStackTrace();
		}
	}

	private static void InsertToMetaColumns(String[] columnData){
		String path = "data/catalog";
		Table metaColumn = new Table();
		try {
			metaColumn.initTable(path, "davisbase_columns");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println("InsertToMetaColumns init failed");
			e.printStackTrace();
		}
		
		//insert both meta tables data into both meta tables
		try {
			metaColumn.InsertDataToMeta(new String[] { "row_id", "table_name",
					"column_name", "data_type", "ordinal_position", "is_nullable" }, columnData );
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println("InsertDataToMetaColumns failed");
			e.printStackTrace();
		}
	}
	
	public static void select(String path, String tableName, String[] columnNames, String[] condition){
		File file = new File(path + "/" + tableName + ".tbl");
		if (!file.exists()) {
			System.out.println(tableName + " Table does not exist.");
			return;
		}
		Table table = new Table();
		try {
		table.initTable(path, tableName);
		Map<Integer, Cell> data = table.selectRecords(columnNames, condition);
		
		Set<Entry<Integer, Cell>> dataSet = data.entrySet();

		List<String> colmns = new ArrayList<>();
		colmns.addAll(table.getColumnNames().values());

		StringBuffer colNames = new StringBuffer();
		if (columnNames.length == 1 && columnNames[0].trim().equalsIgnoreCase("*")) {
			columnNames = new String[colmns.size()];
			for (int i = 0; i < colmns.size(); i++) {
				colNames.append(colmns.get(i) + " | ");
				columnNames[i] = colmns.get(i);
			}
		} else {
			for (String col : colmns)
				if (Contains(columnNames, col))
					colNames.append(col + " | ");
		}
		System.out.println(colNames.toString());

		int count = 0;
		for (Map.Entry<Integer, Cell> entry : dataSet) {
			Cell cellRecord = entry.getValue();
			PayLoad cellPayLoad = cellRecord.getPayload();

			StringBuffer sb = new StringBuffer();
			for (int i = 0; i < colmns.size(); i++)
				if (Contains(columnNames, colmns.get(i)))
					if (i == 0)
						sb.append(cellRecord.getRowId() + " | ");
					else
						sb.append(cellPayLoad.getData()[i - 1] + " | ");
			System.out.println(sb.toString());
			count++;
		}
		System.out.println("\nFound total of " + count + " records.");
		}catch (Exception e) {
			System.out.println("Error occured while initializing table");
			e.printStackTrace();
		}
	}
	
	public static boolean Contains(String[] array, String item) {
		for (String element : array)
			if (item.trim().equalsIgnoreCase(element.trim()))
				return true;
		return false;
	}
	
	public static void insert(String path, String tableName, String[] columnNames, String[] values) throws Exception {
		Table table = new Table();
		table.initTable(path, tableName);
		for(int i=0;i<columnNames.length;i++)
		System.out.println("col-names--- "+columnNames[i]);
		
		for(int i=0;i<values.length;i++)
			System.out.println("values--- "+values[i]);
		
		table.insertToLeaf(columnNames, values);
		System.out.println("Successfully inserted the record.");
	}
	
	public static void update(String path, String tableName, String[] data, String[] condition) throws Exception {
		Table table = new Table();
		table.initTable(path, tableName);
		Map<Integer, Cell> filteredData = table.selectRecords(new String[] { "*" }, condition);
		Map<Integer, String> columns = table.getColumnNames();
		int total = filteredData.size();

		for (Map.Entry<Integer, Cell> entry : filteredData.entrySet()) {
			Cell cell = entry.getValue();
			PayLoad payLoad = cell.getPayload();
			String[] colNames = columns.values().toArray(new String[0]);
			int rowid = cell.getRowId();

			String[] payLoadData = new String[payLoad.getData().length + 1];
			payLoadData[0] = String.valueOf(rowid);
			for (int i = 0; i < payLoad.getData().length; i++)
			{	
				payLoadData[i + 1] = payLoad.getData()[i];
				//System.out.println(payLoadData[i + 1]+" "+(i+1));
			}
			
			int index = getIndex(colNames, data[0]);
			//System.out.println("--$--"+index);
			payLoadData[index] = data[2];
			table.updateToLeaf(colNames, payLoadData, cell.getPageNumber(), cell.getLocation(), rowid);
		}
		System.out.println("Total of " + total + " records were updated");
	}
	
	private static int getIndex(String[] columns, String column) {
		for (int i = 0; i < columns.length; i++) {
			if (column.trim().equalsIgnoreCase(columns[i].trim()))
				return i;
		}
		return -1;
	}
	
	public static void delete(String path, String tableName, String[] condition) throws Exception {
		Table table = new Table();
		table.initTable(path, tableName);
		Map<Integer, Cell> filteredData = table.selectRecords(new String[] { "*" }, condition);
		int total = filteredData.size();

		for (Map.Entry<Integer, Cell> entry : filteredData.entrySet()) {
			Cell rec = entry.getValue();
			table.deleteRec(rec.getPageNumber(), rec.getLocation());
		}
		System.out.println("Total of " + total + " records were deleted from "+ tableName);
	}

	public static void dropTable(String path, String tableName) throws Exception {
		delete("data/catalog", "davisbase_columns", new String[] { "table_name", "=", tableName });
		delete("data/catalog", "davisbase_tables", new String[] { "table_name", "=", tableName });

		File file = new File(path + "/" + tableName + ".tbl");
		if (!file.delete())
			System.out.println("The table is successfully removed from Meta, but could not be delete from FileSystem.");
		else
			System.out.println("Dropped table " + tableName + " successfully.");
	}
	
}
