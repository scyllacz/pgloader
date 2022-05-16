package cz.scylla.main.storage;

import jxl.CellType;

public class StorageValue
{

	private final String	value;
	private final CellType	type;

	public StorageValue(final String value, final CellType type)
	{
		this.value = value;
		this.type = type;
	}

	public String getValue()
	{
		return value;
	}

	public CellType getType()
	{
		return type;
	}

	@Override
	public String toString()
	{
		return "StorageValue [value=" + value + ", type=" + type + "]";
	}
}
