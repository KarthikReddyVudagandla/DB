public class PayLoad {

	private byte noColumns;

	public byte getNoColumns() {
		return noColumns;
	}

	public void setNumberOfColumns(byte numberOfColumns) {
		this.noColumns = numberOfColumns;
	}

	private byte[] dataTypes;

	public byte[] getDataTypes() {
		return dataTypes;
	}

	public void setDataTypes(byte[] dataType) {
		this.dataTypes = dataType;
	}

	private String[] dataArray;

	public String[] getData() {
		return dataArray;
	}

	public void setData(String[] data) {
		this.dataArray = data;
	}

	private int payLoadSize;

	public void setSize(int size) {
		payLoadSize = size;
	}

	public int getSize() {
		return payLoadSize;
	}
}
