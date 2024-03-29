package be.garagepoort.mcsqlmigrations;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class Migrations {

    private static final Logger LOGGER = LoggerFactory.getLogger(Migrations.class);

    private final List<? extends Migration> migrationsScripts;
    private final SqlConnectionProvider sqlConnectionProvider;

    public Migrations(SqlConnectionProvider sqlConnectionProvider, MigrationsProvider migrationsProvider) {
        this.migrationsScripts = migrationsProvider.getMigrations();
        this.sqlConnectionProvider = sqlConnectionProvider;
    }

    public void run(String migrationsTableName) {
        if (sqlConnectionProvider.getDatabaseType() == DatabaseType.MYSQL) {
            createMigrationTableMysql(migrationsTableName);
        } else {
            createMigrationTableSqlite(migrationsTableName);
        }
        runMigrations(migrationsTableName);
    }

    private void createMigrationTableSqlite(String migrationsTableName) {
        try (Connection connect = sqlConnectionProvider.getConnection();
             Statement stmt = connect.createStatement()) {
            LOGGER.info("Creating migration table");

            String sql = "CREATE TABLE IF NOT EXISTS " + migrationsTableName + " (\n"
                + "	id integer PRIMARY KEY,\n"
                + "	version integer NOT NULL\n"
                + ");";
            stmt.execute(sql);
        } catch (SQLException e) {
            throw new SqlMigrationException("Failure creating migration table: {}", e);
        }
    }

    private void createMigrationTableMysql(String migrationsTableName) {
        try (Connection connect = sqlConnectionProvider.getConnection();
             Statement stmt = connect.createStatement()) {
            LOGGER.info("Creating migration table");

            String sql = "CREATE TABLE IF NOT EXISTS " + migrationsTableName + " (\n"
                + "	id BIGINT PRIMARY KEY AUTO_INCREMENT,\n"
                + "	version integer NOT NULL\n"
                + ");";
            stmt.execute(sql);
        } catch (SQLException e) {
            throw new SqlMigrationException("Failure creating migration table: {}", e);
        }
    }

    private void runMigrations(String migrationsTableName) {
        try (Connection connect = sqlConnectionProvider.getConnection()) {
            LOGGER.info("Starting migrations");
            connect.setAutoCommit(false);
            int maxVersion = getMaxVersion(migrationsTableName, connect);

            List<Migration> validMigrations = migrationsScripts.stream().filter(m -> m.getVersion() > maxVersion)
                .sorted(Comparator.comparingInt(Migration::getVersion))
                .collect(Collectors.toList());

            for (Migration migration : validMigrations) {
                try {

                    if (migration.getStatement(connect) != null) {
                        executeStatement(connect, migration.getStatement(connect));
                    }
                    if (migration.getStatements(connect) != null) {
                        for (String s : migration.getStatements(connect)) {
                            executeStatement(connect, s);
                        }
                    }

                    try (PreparedStatement migrationStatement = connect.prepareStatement("INSERT INTO " + migrationsTableName + " (version) VALUES (?);")) {
                        migrationStatement.setInt(1, migration.getVersion());
                        migrationStatement.execute();
                    }

                    connect.commit();
                } catch (SQLException e) {
                    connect.rollback();
                    connect.setAutoCommit(true);
                    throw new SqlMigrationException("Failure executing migrations: {}", e);
                }
            }
            connect.commit();
            connect.setAutoCommit(true);
        } catch (SQLException e) {
            throw new SqlMigrationException("Failure connecting to the database: {}", e);
        }
    }

    private void executeStatement(Connection connect, String statement) throws SQLException {
        try (Statement stmt = connect.createStatement()) {
            stmt.execute(statement);
        }
    }

    private int getMaxVersion(String migrationsTableName, Connection connect) {
        try (Statement stmt = connect.createStatement()) {
            ResultSet resultSet = stmt.executeQuery("SELECT max(version) as max from " + migrationsTableName);
            int max = resultSet.next() ? resultSet.getInt("max") : 0;
            LOGGER.info("Latest migration version = {}", max);
            return max;
        } catch (SQLException e) {
            throw new SqlMigrationException("Failure retrieving max migration version: {}", e);
        }
    }
}
