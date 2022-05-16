package cz.scylla.main.storage;

import java.util.*;

import jxl.Sheet;
import jxl.Workbook;

public class Storage
{

	private final List<Workbook>			workbooks	= new ArrayList<>();

	private final Map<String, StorageTable>	tableByName		= new HashMap<>();
	private final List<StorageTable>		tables	= new ArrayList<>();

	private int								indexProcessedTable;

	public void add(final Workbook workbook) {
		workbooks.add(workbook);
	}

	public void loadTables(List<String> tablesName) {
		for (final Workbook workbook : workbooks) {
			for (final Sheet table : workbook.getSheets()) {
				final String name = table.getName();

				if (tablesName.size() > 0 && !tablesName.contains(name.toLowerCase())) {
					continue;
				}

				if (name.indexOf("!") == -1) {
					final StorageTable stTable = new StorageTable(table);

					tableByName.put(name, stTable);
					tables.add(stTable);
				}
			}
		}
	}

	public void processTables() {
		for (final StorageTable table : tables) {
			table.processTable(this);
		}

		Collections.sort(tables, Comparator.comparingInt(StorageTable::getOrderProcessedTable));
	}

	public int tableProcessed() {
		return indexProcessedTable++;
	}

	public List<StorageTable> getList() {
		return tables;
	}

	public StorageTable getTable(final String name) {
		return tableByName.get(name);
	}

	public int getWorkbooksSize() {
		return workbooks.size();
	}

	public boolean existTable(final String name) {
		return tableByName.containsKey(name);
	}

	public StorageTable registerNewTable(final String name) {
		final StorageTable table = new StorageTable(name);

		tableByName.put(name, table);
		tables.add(table);

		return table;
	}
}
