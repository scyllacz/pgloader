package cz.scylla.main;

import cz.scylla.main.storage.Storage;
import cz.scylla.main.writer.PgDataWriter;
import cz.scyllalib.log.Log;
import cz.scyllalib.pgsql.PgConnector;
import cz.scyllalib.pgsql.PgException;
import cz.scyllalib.utils.Config;
import jxl.Workbook;
import jxl.read.biff.BiffException;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Main {

    private static final Log log = Log.getLogger(Main.class);

    private final static String FILE_EXTENSION_A = ".xls";
    private final static String FILE_EXTENSION_B = ".xlsx";

    private final static String FILE_DIRECTORY = "data";
    public static final String  DEFAULT_CONFIG_PATH = "config/pgloader.properties";

    private final Storage       storage;
    private final PgDataWriter  writer;

    public static void main(String[] args) {
        String firstArgument = args.length > 0 ? args[0] : null;
        String secondArgument = args.length > 1 ? args[1] : null;
        List<String> tables = secondArgument == null ? new ArrayList() : Arrays.asList(secondArgument.split(","));
        String configPath = firstArgument != null ? firstArgument : DEFAULT_CONFIG_PATH;

        new Main(configPath, tables);
    }

    public Main(String configPath, List<String> tables) {
        File configFile = new File(configPath);
        if (!configFile.exists() || configFile.isDirectory()) {
            log.error("Config not exists " + configFile);
            System.exit(1);
        }

        Config config = Config.INSTANCE;
        config.loadProperties(configFile);

        final String host = config.getPropertyAsString("dbHost", "127.0.0.1");
        final String port = config.getPropertyAsString("dbPort", "5432");
        final String dbName = config.getPropertyAsString("dbName");
        final String username = config.getPropertyAsString("dbUser", "root");
        final String password = config.getPropertyAsString("dbPassword", "-");

        PgConnector.create(host, port, dbName, username, password);

        long ms = System.currentTimeMillis();

        storage = new Storage();
        writer = new PgDataWriter();

        final String fileDirectory = config.getPropertyAsString("dataDirectory", FILE_DIRECTORY);
        final File folder = new File(fileDirectory);

        if (!folder.exists()) {
            log.error("Data directory not exists " + fileDirectory);
            System.exit(1);
        }

        for (final File file : folder.listFiles()) {
            final String name = file.getName();
            if (!name.toLowerCase().contains(FILE_EXTENSION_A) && name.toLowerCase().contains(FILE_EXTENSION_B)) {
                continue;
            }

            if (name.substring(0, 1).equals(".")) {
                continue;
            }

            System.out.println("Loading file " + name);

            try {
                storage.add(Workbook.getWorkbook(file));
            } catch (BiffException | IOException e) {
                e.printStackTrace();
                break;
            }
        }

        if (storage.getWorkbooksSize() == 0) {
            System.out.println("No workbooks to load.");
            return;
        }

        storage.loadTables(tables);
        storage.processTables();

        System.out.println("Data stored in memory. Done in " + (System.currentTimeMillis() - ms) + "ms.");

        ms = System.currentTimeMillis();

        try {
            writer.write(storage);
            System.out.println("Data successfully written to the database. Done in " + (System.currentTimeMillis() - ms) + "ms.");
        } catch (SQLException e) {
            throw new PgException("PgDataWriter.write: " + e);
        }
    }

}
