package cz.scylla.main.storage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StorageRow
{

	private final Map<String, StorageValue>	valueByColumnName	= new HashMap<>();
	private final List<StorageValue>		values	= new ArrayList<>();

	public void add(final String key, final StorageValue value)
	{
		valueByColumnName.put(key, value);
		values.add(value);
	}

	public StorageValue getValue(final String key)
	{
		return valueByColumnName.get(key);
	}

	public List<StorageValue> getValues()
	{
		return values;
	}

	public boolean isValid()
	{
		return !(values.stream().filter(val -> val.getValue().isEmpty()).count() > 0);
	}

	@Override
	public String toString()
	{
		return "StorageRow [byKey=" + valueByColumnName + ", values=" + values + "]";
	}
}
