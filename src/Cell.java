public class Cell {

	private short location;

	public short getLocation() {
		return location;
	}

	public void setLocation(short location) {
		this.location = location;
	}

	private int pageNumber;

	public int getPageNumber() {
		return pageNumber;
	}

	public void setPageNumber(int pageNumber) {
		this.pageNumber = pageNumber;
	}

	private int payLoadSize;

	public int getPayLoadSize() {
		return payLoadSize;
	}

	public void setPayLoadSize(int payLoadSize) {
		this.payLoadSize = payLoadSize;
	}

	private int rowId;

	public int getRowId() {
		return rowId;
	}

	public void setRowId(int rowId) {
		this.rowId = rowId;
	}

	private PayLoad payload;

	public PayLoad getPayload() {
		return payload;
	}

	public void setPayload(PayLoad payload) {
		this.payload = payload;
	}

	public Cell(short location, int page) {
		this.location = location;
		pageNumber = page;
	}

	public Cell() {
	}
}
