package be.garagepoort.mcsqlmigrations.helpers;

import be.garagepoort.mcsqlmigrations.SqlConnectionProvider;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;

/**
 * The reason for this implementation is to make sure we do not access the sqlite file concurrently.
 */
public class SqliteQueryService implements SqlQueryService {

    private final SqlConnectionProvider sqlConnectionProvider;

    public SqliteQueryService(SqlConnectionProvider sqlConnectionProvider) {
        this.sqlConnectionProvider = sqlConnectionProvider;
    }

    @Override
    public Connection getConnection() {
        return sqlConnectionProvider.getConnection();
    }

    @Override
    public synchronized int insertQuery(Connection connection, String query, SqlParameterSetter parameterSetter) {
        return SqlQueryService.super.insertQuery(connection, query, parameterSetter);
    }

    @Override
    public synchronized void updateQuery(Connection connection, String query, SqlParameterSetter parameterSetter) {
        SqlQueryService.super.updateQuery(connection, query, parameterSetter);
    }

    @Override
    public synchronized void deleteQuery(Connection connection, String query, SqlParameterSetter parameterSetter) {
        SqlQueryService.super.deleteQuery(connection, query, parameterSetter);
    }

    @Override
    public synchronized <T> Optional<T> findOne(Connection connection, String query, SqlParameterSetter parameterSetter, RowMapper<T> rowMapper) {
        return SqlQueryService.super.findOne(connection, query, parameterSetter, rowMapper);
    }

    @Override
    public synchronized <T> T getOne(Connection connection, String query, SqlParameterSetter parameterSetter, RowMapper<T> rowMapper) {
        return SqlQueryService.super.getOne(connection, query, parameterSetter, rowMapper);
    }

    @Override
    public synchronized <T> T getOne(Connection connection, String query, RowMapper<T> rowMapper) {
        return SqlQueryService.super.getOne(connection, query, rowMapper);
    }

    @Override
    public synchronized <T> List<T> find(Connection connection, String query, RowMapper<T> rowMapper) {
        return SqlQueryService.super.find(connection, query, rowMapper);
    }

    @Override
    public synchronized <T> List<T> find(Connection connection, String query, SqlParameterSetter parameterSetter, RowMapper<T> rowMapper) {
        return SqlQueryService.super.find(connection, query, parameterSetter, rowMapper);
    }

    @Override
    public synchronized Integer getGeneratedId(Connection connection, PreparedStatement insert) throws SQLException {
        ResultSet generatedKeys;
        Statement statement = connection.createStatement();
        generatedKeys = statement.executeQuery("SELECT last_insert_rowid()");
        int generatedKey = -1;
        if (generatedKeys.next()) {
            generatedKey = generatedKeys.getInt(1);
        }
        return generatedKey;
    }
}
