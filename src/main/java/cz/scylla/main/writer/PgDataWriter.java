package cz.scylla.main.writer;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import cz.scylla.main.storage.Storage;
import cz.scylla.main.storage.StorageTable;
import cz.scyllalib.pgsql.PgConnector;

public class PgDataWriter
{

	private final Connection	connection;

	public PgDataWriter()
	{
		connection = PgConnector.INSTANCE.getConnection().getConnection();
	}

	public void write(final Storage storage) throws SQLException {
		connection.setAutoCommit(false);

		final Statement statement = connection.createStatement();

		for (final StorageTable table : storage.getList()) {
			statement.executeUpdate("TRUNCATE TABLE " + table.getName() + " CASCADE;");

			for (final String query : table.getQueries()) {
				System.out.println("\tQ: " + query);
				statement.executeUpdate(query);
			}
		}

		statement.close();

		connection.commit();
		connection.close();
	}

}
