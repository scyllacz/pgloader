package cz.scylla.main.storage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import jxl.Cell;
import jxl.CellType;
import jxl.Sheet;

public class StorageTable
{
	private final static Pattern					KEY_PATTERN			= Pattern.compile("([A-Z_]*):\\[key\\]");
	private final static Pattern					REF_PATTERN			= Pattern.compile("([A-Z_]*).([A-Z_]*)");

	private final Sheet								table;
	private final String							tableName;

	private final List<String>						columnNames			= new ArrayList<>();

	private final Map<Integer, Ref>					refs				= new HashMap<>();
	private final Map<Integer, Integer>				keyValues			= new HashMap<>();

	private final Map<String, Map<String, Integer>>	keyAliasInColumns	= new HashMap<>();

	private final List<StorageRow>					rows				= new ArrayList<>();

	private int										orderProcessedTable;
	private Boolean									processed			= false;

	public StorageTable(final String name) {
		this.table = null;
		this.tableName = name;
	}

	public StorageTable(final Sheet table) {
		this.table = table;
		this.tableName = table.getName();
	}

	public void processTable(final Storage storage) {
		if (this.isProcessed()) { return; }

		System.out.println("StorageTable.processTable: " + tableName);

		for (int x = 1; x < table.getColumns(); x++) {
			final Cell nameCell = table.getCell(x, 0);
			final String name = nameCell.getContents();

			if (name.isEmpty()) {
				break;
			}

			final Matcher nameMatcher = KEY_PATTERN.matcher(name);
			final String columnName;

			if (nameMatcher.find()) {
				columnName = nameMatcher.group(1);
				keyValues.put(x - 1, 0);
				keyAliasInColumns.put(columnName, new HashMap<>());
			} else {
				columnName = name;
			}

			columnNames.add(columnName);

			final Cell refCell = table.getCell(x, 1);
			final String ref = refCell.getContents();

			final Matcher refMatcher = REF_PATTERN.matcher(ref);

			if (refMatcher.find()) {
				refs.put(x - 1, new Ref(refMatcher.group(1), refMatcher.group(2)));
			}
		}

		for (int y = 2; y < table.getRows(); y++) {
			final StorageRow row = new StorageRow();

			final Cell useCell = table.getCell(0, y);
			final String useRow = useCell.getContents();

			// not use this row
			if (!useRow.contains("TRUE")) {
				continue;
			}

			for (int x = 0; x < columnNames.size(); x++) {
				final Cell cell = table.getCell(x + 1, y);
				final String columnName = columnNames.get(x);

				String value = cell.getContents();
				CellType type = cell.getType();

				final Ref ref = refs.get(x);

				if (value.contains("NULL")) {
					type = CellType.EMPTY;
				} else {
					if (ref != null) {
						final StorageTable refTable = storage.getTable(ref.getTableName());
						if(refTable == null) {
							System.out.println("Missing reference table " + ref.getTableName());
						}

						refTable.processTable(storage);

						value = String.valueOf(refTable.getRealKey(ref.getColumnName(), value));
						type = CellType.NUMBER;
					}

					final Map<String, Integer> keyForColumn = keyAliasInColumns.get(columnName);

					if (keyForColumn != null) {
						final int currentKey;

						if (keyAliasInColumns.get(columnName).containsKey(value)) {
							currentKey = keyAliasInColumns.get(columnName).get(value);
						} else {
							currentKey = keyValues.get(x) + 1;

							keyAliasInColumns.get(columnName).put(value, currentKey);
							keyValues.put(x, currentKey);
						}

						value = String.valueOf(currentKey);
						type = CellType.NUMBER;
					}
				}


				row.add(columnName, new StorageValue(value, type));
			}

			rows.add(row);
		}

		this.orderProcessedTable = storage.tableProcessed();

		System.out.println("\t" + tableName + " load " + rows.size() + " rows.");

		processed = true;
	}

	public Boolean isProcessed() {
		return processed;
	}

	public int getOrderProcessedTable() {
		return orderProcessedTable;
	}

	public String getName() {
		return tableName;
	}

	public void addStorageRow(final StorageRow row) {
		rows.add(row);
	}

	public StorageRow getStorageRowByName(final String name) {
		return rows.get(columnNames.indexOf(name));
	}

	public List<StorageRow> getValues() {
		return rows;
	}

	public Integer getRealKey(final String columnName, final String key) {
		return keyAliasInColumns.get(columnName).get(key);
	}

	public List<String> getQueries() {
		final List<String> queries = new ArrayList<>();

		for (int x = 1; x <= rows.size(); x++) {
			final StringBuilder builder = new StringBuilder();

			builder.append("INSERT INTO ");
			builder.append(tableName);
			builder.append(" (");
			final int size = columnNames.size();
			for (int i = 1; i <= size; i++) {
				builder.append(columnNames.get(i - 1));

				if (i != size) {
					builder.append(",");
				}
			}
			builder.append(") VALUES (");
			final List<StorageValue> simpleValues = rows.get(x - 1).getValues();
			final int valSize = simpleValues.size();

			for (int z = 1; z <= valSize; z++) {
				final StorageValue storageValue = simpleValues.get(z - 1);

				if (storageValue.getType() == CellType.NUMBER || storageValue.getType() == CellType.EMPTY) {
					builder.append(storageValue.getValue().replace(",", "."));
				} else {
					builder.append("'" + storageValue.getValue() + "'");
				}

				if (z != valSize) {
					builder.append(",");
				}
			}
			builder.append(");");

			queries.add(builder.toString());
		}

		return queries;
	}
}
