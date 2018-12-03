import java.io.IOException;
import java.io.RandomAccessFile;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.*;
import java.time.temporal.TemporalAccessor;
import java.util.*;

public class Table {
	private String TableName = "";
	private String Path = "";
	private boolean IsMetaTable = false;
	private static final int FILESIZE = 512;
	private int NoPages = 0;
	private RandomAccessFile tableFile;
	private Map<Integer, Cell> Columns;

	public void initTable(String path, String tableName) throws Exception {
		this.TableName = tableName;
		this.Path = path;
		tableFile = new RandomAccessFile(this.Path + "/" + this.TableName + ".tbl", "rw");
		IsMetaTable = this.Path.contains("catalog");
		NoPages = (int) tableFile.length() / FILESIZE;
		if (!IsMetaTable)
			fetchMetaData();
	}

	private void fetchMetaData() throws Exception {
		// fetch root_page.
		if (!IsMetaTable) {
			Table metaTable = new Table();
			metaTable.initTable("data/catalog", "davisbase_columns");
			Columns = metaTable.selectRecords(new String[] { "*" }, // Columns needed
					new String[] { "table_name", "=", TableName }); // where tableName = TableName condition
		}
	}

	public boolean isMeta() {
		return IsMetaTable;
	}

	public void createFile(String path, String tableName) throws IOException {
		createTableFile(path, tableName);
		setLeafHeaders(1);
	}

	private void createTableFile(String path, String tableName) throws IOException {
		tableFile = new RandomAccessFile(path + "/" + tableName + ".tbl", "rw");
		tableFile.setLength(0);
		tableFile.setLength(FILESIZE);
		this.NoPages = 1;
	}

	private void setLeafHeaders(int page) throws IOException {
		int fileBegin = (page - 1) * FILESIZE;
		tableFile.seek(fileBegin + 0);
		tableFile.writeByte(0x0D);
		
		tableFile.seek(fileBegin + 1);
		tableFile.writeByte(0x00);
		
		tableFile.seek(fileBegin + 2);
		tableFile.writeShort(512);

		tableFile.seek(fileBegin + 4);
		tableFile.writeInt(0xFFFFFFFF);
	}

	private void setNonLeafHeaders(int page) throws IOException {
		int fileBegin = (page - 1) * FILESIZE;
		tableFile.seek(fileBegin + 0);
		tableFile.writeByte(0x05);
		
		tableFile.seek(fileBegin + 1);
		tableFile.writeByte(0x00);

		tableFile.seek(fileBegin + 2);
		tableFile.writeShort(512);

		tableFile.seek(fileBegin + 4);
		tableFile.writeInt(0xFFFFFFFF);
	}

	public boolean isLeaf(int page) throws IOException {
		tableFile.seek(((page - 1) * FILESIZE) + 0);
		return (int) tableFile.readByte() == 0x0D;
	}

	public boolean isNode(int page) throws IOException {
		tableFile.seek(((page - 1) * FILESIZE) + 0);
		return (int) tableFile.readByte() == 0x05;
	}

	public Map<Integer, String> getColumnNames() {
		Map<Integer, String> colNames = new LinkedHashMap<>();
		if (IsMetaTable && this.TableName.equalsIgnoreCase("davisbase_columns")) {
			colNames.put(1, "row_id");
			colNames.put(2, "table_name");
			colNames.put(3, "column_name");
			colNames.put(4, "data_type");
			colNames.put(5, "ordinal_position");
			colNames.put(6, "is_nullable");
		} else if (IsMetaTable && this.TableName.equalsIgnoreCase("davisbase_tables")) {
			colNames.put(1, "row_id");
			colNames.put(2, "table_name");
		} else {
			Object[] cols = Columns.values().toArray();
			for (int i = 0; i < cols.length; i++) {
				String[] columnData = ((Cell) cols[i]).getPayload().getData();
				colNames.put(Integer.parseInt(columnData[3]), columnData[1]);
			}
		}
		return colNames;
	}

	public Map<Integer, String> getColumnDataTypes() {
		Map<Integer, String> colDataTypes = new LinkedHashMap<>();
		Object[] cols = Columns.values().toArray();
		for (int i = 0; i < cols.length; i++) {
			String[] columnData = ((Cell) cols[i]).getPayload().getData();
			colDataTypes.put(Integer.parseInt(columnData[3]), columnData[2]);
		}
		return colDataTypes;
	}

	public Map<Integer, String> getColumnNullable() {
		Map<Integer, String> isNullable = new LinkedHashMap<>();
		Object[] cols = Columns.values().toArray();
		for (int i = 0; i < cols.length; i++) {
			String[] columnData = ((Cell) cols[i]).getPayload().getData();
			isNullable.put(Integer.parseInt(columnData[3]), columnData[4]);
		}
		return isNullable;
	}

	public Map<Integer, ColumnData> getColumnData() {
		Map<Integer, ColumnData> colDetails = new LinkedHashMap<>();
		Object[] cols = Columns.values().toArray();
		for (int i = 0; i < cols.length; i++) {
			String[] columnData = ((Cell) cols[i]).getPayload().getData();
			colDetails.put(Integer.parseInt(columnData[3]), new ColumnData(columnData[1], columnData[2],
					Integer.parseInt(columnData[3]), columnData[4].equalsIgnoreCase("yes")));
		}
		return colDetails;
	}

	public int fetchNoRecords() throws IOException {
		int page = 1;
		int total = 0;
		while (page != 0xFFFFFFFF) {
			total += getCellCountInPage(page);
			page = fetchNextLeafPage(page);
		}
		return total;
	}

	private int getCellCountInPage(int page) throws IOException {
		tableFile.seek(((page - 1) * FILESIZE) + 1);
		return (int) tableFile.readByte();
	}

	public int fetchNextLeafPage(int page) throws IOException {
		tableFile.seek(((page - 1) * FILESIZE) + 4); //like 4, 512*1 + 4, 512*2 + 4,....etc
		return tableFile.readInt();
	}

	public boolean canInsert(int page, int size) throws IOException {
		// size + 2, considering the pointer to cell in header
		return (size + 2) < (FILESIZE - headerSize(page) - dataSize(page));
	}

	private int dataSize(int page) throws IOException {
		int fstRecLoc = fetchTopRecLoc(page);
		return FILESIZE - fstRecLoc;
	}

	private int fetchTopRecLoc(int page) throws IOException {
		tableFile.seek(((page - 1) * FILESIZE) + 2);
		return (int) tableFile.readShort();
	}

	public int headerSize(int page) throws IOException {
		// fixed header size is 8 (1 + 1 + 2 + 4) for leaf or an internal node
		int noRec = getCellCountInPage(page);
		return 8 + (2 * noRec);
	}

	private void insertRec(int page, int payLoadSize, Cell dataCell) throws IOException {
		this.insertRec(page, payLoadSize, dataCell, -1);
	}

	private void insertRec(int page, int payLoadSize, Cell dataCell, int location) throws IOException {
		// TopMost Record - data length
		int newFstRecLoc = fetchTopRecLoc(page) - payLoadSize;

		// write data
		tableFile.seek((page - 1) * FILESIZE + newFstRecLoc);
		tableFile.writeShort(dataCell.getPayLoadSize());
		tableFile.writeInt(dataCell.getRowId());

		PayLoad payload = dataCell.getPayload();
		tableFile.writeByte(payload.getNoColumns());

		byte[] dataTypes = payload.getDataTypes();
		tableFile.write(dataTypes);

		String data[] = payload.getData();

		for (int i = 0; i < dataTypes.length; i++) {
			switch (dataTypes[i]) {
			case 0x00:
				tableFile.writeByte(0);
				break;
			case 0x01:
				tableFile.writeShort(0);
				break;
			case 0x02:
				tableFile.writeInt(0);
				break;
			case 0x03:
				tableFile.writeLong(0);
				break;
			case 0x04:
				tableFile.writeByte(new Byte(data[i + 1]));
				break;
			case 0x05:
				tableFile.writeShort(new Short(data[i + 1]));
				break;
			case 0x06:
				tableFile.writeInt(new Integer(data[i + 1]));
				break;
			case 0x07:
				tableFile.writeLong(new Long(data[i + 1]));
				break;
			case 0x08:
				tableFile.writeFloat(new Float(data[i + 1]));
				break;
			case 0x09:
				tableFile.writeDouble(new Double(data[i + 1]));
				break;
			case 0x0A:
				long datetime = tableFile.readLong();
				ZoneId zoneId = ZoneId.of("America/Chicago");
				Instant x = Instant.ofEpochSecond(datetime);
				ZonedDateTime zdt2 = ZonedDateTime.ofInstant(x, zoneId);
				zdt2.toLocalTime();
				break;
			case 0x0B:
				long date = tableFile.readLong();
				ZoneId zoneId1 = ZoneId.of("America/Chicago");
				Instant x1 = Instant.ofEpochSecond(date);
				ZonedDateTime zdt3 = ZonedDateTime.ofInstant(x1, zoneId1);
				break;
			default:
				tableFile.writeBytes(data[i + 1]);
				break;
			}
		}

		// write the new TopMost Record to header
		tableFile.seek((page - 1) * FILESIZE + 2);
		tableFile.writeShort(newFstRecLoc);

		if (location == -1) {
			// increment the count on file
			int count = getCellCountInPage(page);
			tableFile.seek((page - 1) * FILESIZE + 1);
			tableFile.writeByte(count + 1);

			// add the pointer to the new rec to the pointer list.
			tableFile.seek((page - 1) * FILESIZE + 8 + ((count == 0) ? 0 : (count * 2)));
			tableFile.writeShort(newFstRecLoc);
		} else {
			int count = getCellCountInPage(page);
			tableFile.seek((page-1)*FILESIZE+8);
			int i = 0;
			int ptr = tableFile.readShort();
			while(i<count&&ptr!=location) {
				ptr = tableFile.readShort();
				i++;
			}
			if(ptr == location) {
				tableFile.seek((page-1)*FILESIZE+8+(i*2));
				tableFile.writeShort(newFstRecLoc);
			}
		}
	}
	

	public void insertToLeaf(String[] colNames, String[] values) throws Exception {
		// +1 byte for number of columns, +6 bytes for the cell headers, every column
		// other than row_id has a byte header
		Map<Integer, String> dataTypes = getColumnDataTypes();
		Map<Integer, ColumnData> columnDetails = getColumnData();
		Map<Integer, String> isNullable = getColumnNullable();
		Object[] nullables = isNullable.values().toArray();
		byte[] dataHeaders = new byte[columnDetails.size() - 1];
		int headerPointer = 0;

		for (int i = 0; i < values.length; i++) {
			if (values[i].equalsIgnoreCase("null") && ((String) nullables[i]).equals("NO")) {
				System.out.println("Cannot insert NULL values in NOT NULL field");
				return;
			}
		}

		int dataSize = 1 + (columnDetails.size() - 1) + 6;

		for (int i : columnDetails.keySet()) {
			ColumnData col = columnDetails.get(i);
			if (col.column_name.equalsIgnoreCase("row_id")) {
				continue;
			}
			int indx = findIndx(colNames, col.column_name);
			if (indx != -1) {
				dataSize += getDataTypeSize(col.data_type, values[indx].length());
				dataHeaders[headerPointer++] = (byte) getSTCofDataType(col.data_type, false, values[indx].length());
			} else if (indx == -1 && col.is_nullable)
				dataHeaders[headerPointer++] = (byte) getSTCofDataType(col.data_type, true, 0);
			else
				throw new Exception("Could not find column '" + col.column_name + "'");
		}

		int pageNo = fetchLastPage();
		// check leaf size
		byte[] plDataType = new byte[columnDetails.size() - 1];
		String[] dataTypeStr = new String[columnDetails.size()];
		dataTypes.values().toArray(dataTypeStr);
		int payLoadSize = getCellSize(values, plDataType, dataTypeStr);
		payLoadSize = payLoadSize + 6;

		boolean canInsert = canInsert(pageNo, payLoadSize);

		if (canInsert) {
			for(int i=0;i<values.length;i++)
				System.out.println("values--"+values[i]);
			Cell cell = createCell(pageNo, fetchNextRowID(), (short) payLoadSize, plDataType, values);
			insertRec(pageNo, payLoadSize, cell);
		} else {
			int row_id = fetchNextRowID();
			int pgNum = splitLeafPage(pageNo);
			Cell cell = createCell(pgNum, row_id, (short) payLoadSize, plDataType, values);
			insertRec(pgNum, payLoadSize, cell);
		}
	}

	public void InsertDataToMeta(String[] colNames, String[] values) throws Exception {
		int pageNo = fetchLastPage();
		int noColmns;
		Map<Integer, String> dataTypes = new LinkedHashMap<>();
		// check leaf size
		if (IsMetaTable && this.TableName.equalsIgnoreCase("davisbase_tables")) {
			noColmns = 2;
			dataTypes.put(1, "int");
			dataTypes.put(2, "text");
		} else if (IsMetaTable && this.TableName.equalsIgnoreCase("davisbase_columns")) {
			noColmns = 6;
			dataTypes.put(1, "int");
			dataTypes.put(2, "text");
			dataTypes.put(3, "text");
			dataTypes.put(4, "text");
			dataTypes.put(5, "tinyint");
			dataTypes.put(6, "text");
		} else
			return;
		byte[] plDataType = new byte[noColmns - 1]; //-1 coz row_id is not in pl
		String[] dataTypeStr = new String[noColmns];
		dataTypes.values().toArray(dataTypeStr);
		int payLoadSize = getCellSize(values, plDataType, dataTypeStr);
		payLoadSize += 6;

		// change offset calculation??
		boolean canInsert = canInsert(pageNo, payLoadSize);

		if (canInsert) {
			Cell cell = createCell(pageNo, fetchNextRowID(), (short) payLoadSize, plDataType, values);
			insertRec(pageNo, payLoadSize, cell);
		} else {
			int row_id = fetchNextRowID();
			int pgNum = splitLeafPage(pageNo);
			Cell cell = createCell(pgNum, row_id, (short) payLoadSize, plDataType, values);
			insertRec(pgNum, payLoadSize, cell);
		}
	}

	public int splitLeafPage(int pageNo) throws IOException {
		// adding page
		int parent = findNonLeafNode(pageNo);
		if (parent == -1) {
			int newPage = (int) (tableFile.length() / FILESIZE) + 1;
			tableFile.setLength(newPage * FILESIZE);
			setNonLeafHeaders(newPage);
			insertToNonLeaf(newPage, pageNo);
			setAsNextPage(newPage, newPage + 1);

			tableFile.setLength((newPage + 1) * FILESIZE);
			setLeafHeaders(newPage + 1);
			setAsNextPage(pageNo, newPage + 1);
			return newPage + 1;
		} else {
			int newPage = (int) (tableFile.length() / FILESIZE) + 1;
			insertToNonLeaf(parent, pageNo);
			setAsNextPage(parent, newPage);
			setAsNextPage(pageNo, newPage);
			return newPage;
		}
	}

	public void insertToNonLeaf(int page, int pagePtr) throws IOException {
		if (isLeaf(page))
			return;
		//update top cell location
		int loc = fetchTopRecLoc(page) - 8;
		tableFile.seek((page - 1) * FILESIZE + 2);
		tableFile.writeShort(loc);

		// increment the count on file
		int count = getCellCountInPage(page);
		tableFile.seek((page - 1) * FILESIZE + 1);
		tableFile.writeByte(count + 1);

		int topRec = findTopRowId(pagePtr);
		tableFile.seek((page - 1) * FILESIZE + loc);
		tableFile.writeInt(pagePtr);
		tableFile.writeInt(topRec);

		// add the pointer to the new rec to the pointer list.
		tableFile.seek((page - 1) * FILESIZE + 8 + ((count == 0) ? 0 : ((count - 1) * 2)));
		tableFile.writeShort(loc);
	}

	private void setAsNextPage(int currPage, int page) throws IOException {
		tableFile.seek((currPage - 1) * FILESIZE + 4);
		tableFile.writeInt(page);
	}

	private int findNonLeafNode(int page) throws IOException {
		int i = 1;
		int lastLeaf = fetchLastPage();
		while (i < lastLeaf) {
			if (isNode(i))
				return i;
			i++;
		}
		return -1;
	}

	public void updateToLeaf(String[] colNames, String[] data, int page, int location, int row_id) throws Exception {
		// +1 byte for number of columns, +6 bytes for the cell headers, every column
		// other than row_id has a byte header
		Map<Integer, String> dataTypes = getColumnDataTypes();
		Map<Integer, ColumnData> columnDetails = getColumnData();
		Map<Integer, String> isNullable = getColumnNullable();
		Object[] nullables = isNullable.values().toArray();
		byte[] dataHeaders = new byte[columnDetails.size() - 1];
		int headerPointer = 0;

		int dataSize = (columnDetails.size() - 1) + 1 + 6;

		for (int i : columnDetails.keySet()) {
			ColumnData col = columnDetails.get(i);
			if (col.column_name.equalsIgnoreCase("row_id")) {
				continue;
			}
			int indx = findIndx(colNames, col.column_name);
			if (indx != -1) {
				dataSize += getDataTypeSize(col.data_type, data[indx].length());
				dataHeaders[headerPointer++] = (byte) getSTCofDataType(col.data_type, false, data[indx].length());
			} else if (indx == -1 && col.is_nullable)
				dataHeaders[headerPointer++] = (byte) getSTCofDataType(col.data_type, true, 0);
			else
				throw new Exception("Could not find column '" + col.column_name + "'");
		}

		// check leaf size
		byte[] plDataType = new byte[columnDetails.size() - 1];
		String[] dataTypeStr = new String[columnDetails.size()];
		dataTypes.values().toArray(dataTypeStr);
		int payLoadSize = getCellSize(data, plDataType, dataTypeStr);
		payLoadSize = payLoadSize + 6;

		boolean canInsert = canInsert(page, payLoadSize);

		if (canInsert) {
			Cell cell = createCell(page, row_id, (short) payLoadSize, plDataType, data);
			insertRec(page, payLoadSize, cell, location);
		} else {
			int pgNum = splitLeafPage(page);
			Cell cell = createCell(pgNum, row_id, (short) payLoadSize, plDataType, data);
			insertRec(pgNum, payLoadSize, cell, location);
		}
	}

	private Cell createCell(int pageNo, int primaryKey, short payLoadSize, byte[] dataType, String[] values) {
		Cell cell = new Cell();
		cell.setPageNumber(pageNo);
		cell.setRowId(primaryKey);
		cell.setPayLoadSize(payLoadSize);

		PayLoad payload = new PayLoad();
		payload.setNumberOfColumns(Byte.parseByte(values.length - 1 + ""));
		payload.setDataTypes(dataType);
		payload.setData(values);

		cell.setPayload(payload);

		return cell;
	} 

	private static int getCellSize(String[] values, byte[] plDataType, String[] dataType) throws Exception {

		int size = 1 + dataType.length - 1;
		for (int i = 1; i < values.length; i++) {
			plDataType[i - 1] = (byte) getSTCofDataType(dataType[i], false, values[i].length());
			size = size + getDataTypeSize(dataType[i], values[i].length());
		}

		return size;
	}

	private int findIndx(String[] columns, String column) {
		for (int i = 0; i < columns.length; i++) {
			if (column.trim().equalsIgnoreCase(columns[i].trim()))
				return i;
		}
		return -1;
	}

	private int fetchLastPage() throws IOException {
		int page = 0;
		int nextPage = 1;
		while (nextPage != 0xFFFFFFFF) {
			page = nextPage;
			nextPage = fetchNextLeafPage(page);
		}
		return page;
	}

	public int fetchNextRowID() throws IOException {
		int page = fetchLastPage();
		int cellCount = getCellCountInPage(page);
		if (cellCount == 0)
			return 1;
		tableFile.seek(((page - 1) * FILESIZE) + (8 + ((cellCount - 1) * 2)));
		int lasRecLoc = tableFile.readShort();
		tableFile.seek(((page - 1) * FILESIZE) + lasRecLoc + 2);
		int cellValue = (int) tableFile.readInt();
		int totRecs = totalRecordCount();
		return ((cellValue > totRecs) ? cellValue : totRecs) + 1;
	}

	public int totalRecordCount() throws IOException {
		int page = 1;
		int count = 0;
		while (page != 0xFFFFFFFF) {
			count += getCellCountInPage(page);
			page = fetchNextLeafPage(page);
		}
		return count;
	}

	public int findTopRowId(int page) throws IOException {
		int cellCount = getCellCountInPage(page);
		if (cellCount == 0)
			return 1;
		
		tableFile.seek(((page - 1) * FILESIZE) + (8 + ((cellCount - 1) * 2)));
		int lasRecLoc = tableFile.readShort();
		tableFile.seek((page - 1) * FILESIZE + lasRecLoc + 2);
		return ((int) tableFile.readInt());
	}

	public Map<Integer, Cell> selectRecords(String[] columnNames, String[] condition) throws Exception {
		Map<Integer, Cell> cells = getAllData();

		if (condition.length > 0) {
			Map<Integer, Cell> filteredRecords = filterData(cells, columnNames, condition);
			return filteredRecords;
		} else {
			return cells;
		}
	}

	private Map<Integer, Cell> filterData(Map<Integer, Cell> cells, String[] resultColumns, String[] condition) throws Exception {
		Map<Integer, Cell> filteredRecords = new LinkedHashMap<>();
		Map<Integer, String> colNames = getColumnNames();

		int ordinalPosition = 2;
		for (Map.Entry<Integer, String> entry : colNames.entrySet()) {
			String columnName = entry.getValue();
			if (columnName.equals(condition[0])) {
				ordinalPosition = entry.getKey();
			}
		}

		for (Map.Entry<Integer, Cell> entry : cells.entrySet()) {
			Cell cell = entry.getValue();
			PayLoad payload = cell.getPayload();
			String[] data = payload.getData();
			byte[] dataSTC = payload.getDataTypes();

			boolean result;

			if (ordinalPosition == 1 && !IsMetaTable)
				result = checkData((byte) 0x06, String.valueOf(cell.getRowId()), condition);
			else
				result = checkData(dataSTC[ordinalPosition - 2], data[ordinalPosition - 2], condition);

			if (result)
				filteredRecords.put(entry.getKey(), entry.getValue());
		}

		return filteredRecords;
	}

	private Map<Integer, Cell> getAllData() throws Exception {
		int page = 0;
		int nextPage = 1;
		Map<Integer, Cell> allrecords = new LinkedHashMap<>();
		while (nextPage != 0xFFFFFFFF) {
			page = nextPage;
			allrecords.putAll(getPageContent(page));
			nextPage = fetchNextLeafPage(page);
		}
		return allrecords;
	}

	private Map<Integer, Cell> getPageContent(int page) throws Exception {
		Map<Integer, Cell> allCells = new LinkedHashMap<>();
		short[] pointerList = getCellPointers(page);
		for (short recLoc : pointerList) {
			PayLoad payLoad = new PayLoad();

			tableFile.seek((page - 1) * FILESIZE + recLoc);
			//read cell size
			payLoad.setSize(tableFile.readShort());
			//read row id
			int rowId = tableFile.readInt();

			payLoad.setNumberOfColumns(tableFile.readByte());

			byte[] dataTypes = new byte[payLoad.getNoColumns()];
			
			//read all data types of columns & set
			tableFile.read(dataTypes, 0, payLoad.getNoColumns());

			payLoad.setDataTypes(dataTypes);

			String[] dataArray = new String[payLoad.getNoColumns()];
			for (int i = 0; i < payLoad.getNoColumns(); i++) {
				int dataSize = getSizeByHeader(dataTypes[i]);

				switch (dataTypes[i]) {
				case 0x00:
					dataArray[i] = "null";
					break;

				case 0x01:
					dataArray[i] = "null";
					break;

				case 0x02:
					dataArray[i] = "null";
					break;

				case 0x03:
					dataArray[i] = "null";
					break;

				case 0x04:
					dataArray[i] = Integer.toString(tableFile.readByte());
					break;

				case 0x05:
					dataArray[i] = Integer.toString(tableFile.readShort());
					break;

				case 0x06:
					dataArray[i] = Integer.toString(tableFile.readInt());
					break;

				case 0x07:
					dataArray[i] = Long.toString(tableFile.readLong());
					break;

				case 0x08:
					dataArray[i] = String.valueOf(tableFile.readFloat());
					break;

				case 0x09:
					dataArray[i] = String.valueOf(tableFile.readDouble());
					break;

				case 0x0A:
					long tmp = tableFile.readLong();
					Date dateTime = new Date(tmp);
					DateTimeFormatter formater = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
					dataArray[i] = formater.format((TemporalAccessor) dateTime);
					break;

				case 0x0B:
					long tmp1 = tableFile.readLong();
					Date date = new Date(tmp1);
					DateTimeFormatter formater1 = DateTimeFormatter.ofPattern("yyyy-MM-dd");
					dataArray[i] = formater1.format((TemporalAccessor) date);
					break;

				default:
					int len = dataSize;
					byte[] bytes = new byte[len];
					tableFile.read(bytes, 0, len);
					dataArray[i] = new String(bytes);
					break;
				}
			}
			payLoad.setData(dataArray);

			Cell cell = new Cell(recLoc, page);
			cell.setPayLoadSize(payLoad.getSize());
			cell.setPayload(payLoad);
			cell.setRowId(rowId);

			allCells.put(rowId, cell);
		}
		return allCells;
	}

	private short[] getCellPointers(int page) throws IOException {
		int cellCount = getCellCountInPage(page);
		short[] recPointers = new short[cellCount];
		tableFile.seek((page - 1) * FILESIZE + 8);
		for (int i = 0; i < cellCount; i++)
			recPointers[i] = tableFile.readShort();
		return recPointers;
	}

	public boolean checkData(byte headerCode, String data, String[] condition) throws Exception {
		if (!isNullHeader(headerCode)) {
			if (headerCode >= 0x04 && headerCode <= 0x07) {
				Long longData = Long.parseLong(data);
				switch (condition[1]) {
				case "=":
					if (longData == Long.parseLong(condition[2]))
						return true;
					break;
				case ">":
					if (longData > Long.parseLong(condition[2]))
						return true;
					break;
				case "<":
					if (longData < Long.parseLong(condition[2]))
						return true;
					break;
				case "<=":
					if (longData <= Long.parseLong(condition[2]))
						return true;
					break;
				case ">=":
					if (longData >= Long.parseLong(condition[2]))
						return true;
					break;
				case "<>":
					if (longData != Long.parseLong(condition[2]))
						return true;
					break;
				default:
					throw new Exception("Unknown comparision operation '" + condition[2] + "'.");
				}
			} else if (headerCode == 0x08 || headerCode == 0x09) {
				Double doubleData = Double.parseDouble(data);
				switch (condition[1]) {
				case "=":
					if (doubleData == Double.parseDouble(condition[2]))
						return true;
					break;
				case ">":
					if (doubleData > Double.parseDouble(condition[2]))
						return true;
					break;
				case "<":
					if (doubleData < Double.parseDouble(condition[2]))
						return true;
					break;
				case "<=":
					if (doubleData <= Double.parseDouble(condition[2]))
						return true;
					break;
				case ">=":
					if (doubleData >= Double.parseDouble(condition[2]))
						return true;
					break;
				case "<>":
					if (doubleData != Double.parseDouble(condition[2]))
						return true;
					break;
				default:
					throw new Exception("Unknown comparision operation '" + condition[2] + "'.");
				}
			} else if (headerCode >= 0x0C) {

				condition[2] = condition[2].replaceAll("'", "");
				condition[2] = condition[2].replaceAll("\"", "");
				switch (condition[1]) {
				case "=":
					if (data.equalsIgnoreCase(condition[2]))
						return true;
					break;
				case "<>":
					if (!data.equalsIgnoreCase(condition[2]))
						return true;
					break;
				default:
					System.out.println("undefined operator return false");
					return false;
				}
			}
		}
		return false;
	}

	private static int getDataTypeSize(String dataType, int length) {
		int size = 0;
		switch (dataType.trim()) {
		case "tinyint": {
			size = 1;
			break;
		}
		case "smallint": {
			size = 2;
			break;
		}
		case "real":
		case "int": {
			size = 4;
			break;
		}

		case "double":
		case "datetime":
		case "date":
		case "bigint": {
			size = 8;
			break;
		}
		default: {
			size = length;
		}
		}
		return size;
	}

	private static int getSTCofDataType(String dataType, boolean isNull, int length) throws Exception{
		switch (dataType) {
		case "tinyint": {
			return isNull ? 0x00 : 0x04;
		}
		case "smallint": {
			return isNull ? 0x01 : 0x05;
		}
		case "int": {
			return isNull ? 0x02 : 0x06;
		}
		case "bigint": {
			return isNull ? 0x03 : 0x07;
		}
		case "real": {
			return isNull ? 0x02 : 0x08;
		}
		case "double": {
			return isNull ? 0x03 : 0x09;
		}
		case "datetime": {
			return isNull ? 0x03 : 0x0A;
		}
		case "date": {
			return isNull ? 0x03 : 0x0B;
		}
		case "text": {
			return 0x0C + length;
		}
		default:
			throw new Exception("Error: Unknown data type");
		}
	}

	private static int getSizeByHeader(int header) throws Exception {
		switch (header) {
		case 0x00:
		case 0x04: {
			return 1;
		}
		case 0x01:
		case 0x05: {
			return 2;
		}
		case 0x02:
		case 0x06:
		case 0x08: {
			return 4;
		}

		case 0x03:
		case 0x07:
		case 0x09:
		case 0x0A:
		case 0x0B: {
			return 8;
		}
		default:
			if (header >= 0x0C)
				return header - 0x0C;
			else
				throw new Exception("Unknown Header");
		}
	}

	private static boolean isNullHeader(int header) {
		return header >= 0x00 && header <= 0x03;
	}

	public void closeFile() throws IOException {
		tableFile.close();
	}

	public void deleteRec(int pageNumber, short location) throws IOException {
		short[] pointers = getCellPointers(pageNumber);
		int cellCount = getCellCountInPage(pageNumber);
		int index = findIndex(pointers, location);
		if (index != -1) {
			tableFile.seek((pageNumber - 1) * FILESIZE + 8 + (index * 2));
			for (int i = index + 1; i < pointers.length; i++) {
				tableFile.writeShort(pointers[i]);
			}
			tableFile.writeShort(0);
			setRecordCount(pageNumber, cellCount - 1);
		}
	}

	private void setRecordCount(int page, int count) throws IOException {
		tableFile.seek((page - 1) * FILESIZE + 1);
		tableFile.writeByte(count);
	}

	private int findIndex(short[] array, short item) {
		for (int i = 0; i < array.length; i++)
			if (array[i] == item)
				return i;
		return -1;
	}
}
