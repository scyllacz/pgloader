package cz.scylla.main.storage;

public class Ref
{
	private final String	table;
	private final String	columnName;

	public Ref(final String table, final String columnName)
	{
		this.table = table;
		this.columnName = columnName;
	}

	public String getTableName()
	{
		return table;
	}

	public String getColumnName()
	{
		return columnName;
	}

	@Override
	public String toString()
	{
		return "Ref [table=" + table + ", columnName=" + columnName + "]";
	}

}
