package be.garagepoort.mcsqlmigrations.helpers;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class QueryBuilder {

    private final Connection connection;
    private final SqlQueryService sqlQueryService;

    public QueryBuilder(SqlQueryService sqlQueryService) {
        this.connection = sqlQueryService.getConnection();
        this.sqlQueryService = sqlQueryService;
    }

    public QueryBuilder startTransaction() {
        try {
            connection.setAutoCommit(false);
            return this;
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    public void commit() {
        try {
            connection.commit();
            closeConnection();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    public <T> Optional<T> findOne(String query, SqlQueryService.SqlParameterSetter parameterSetter, SqlQueryService.RowMapper<T> rowMapper) {
        Optional<T> one = sqlQueryService.findOne(connection, query, parameterSetter, rowMapper);
        closeConnection();
        return one;
    }

    private void closeConnection() {
        try {
            if (connection.getAutoCommit()) {
                connection.close();
            }
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    public <T> T getOne(String query, SqlQueryService.RowMapper<T> rowMapper) {
        T one = sqlQueryService.getOne(connection, query, (rs) -> {
        }, rowMapper);
        this.closeConnection();
        return one;
    }

    public <T> T getOne(String query, SqlQueryService.SqlParameterSetter parameterSetter, SqlQueryService.RowMapper<T> rowMapper) {
        T one = sqlQueryService.getOne(connection, query, parameterSetter, rowMapper);
        this.closeConnection();
        return one;
    }

    public <T> List<T> find(String query, SqlQueryService.RowMapper<T> rowMapper) {
        List<T> ts = sqlQueryService.find(connection, query, (rs) -> {
        }, rowMapper);
        this.closeConnection();
        return ts;
    }

    public <T> List<T> find(String query, SqlQueryService.SqlParameterSetter parameterSetter, SqlQueryService.RowMapper<T> rowMapper) {
        List<T> ts = sqlQueryService.find(connection, query, parameterSetter, rowMapper);
        this.closeConnection();
        return ts;
    }

    public <K, V> Map<K, V> findMap(String query, SqlQueryService.KeyMapper<K> keyMapper, SqlQueryService.ValueMapper<V> valueMapper) {
        Map<K, V> map = sqlQueryService.findMap(connection, query, s -> {
        }, keyMapper, valueMapper);
        this.closeConnection();
        return map;
    }

    public <K, V> Map<K, V> findMap(String query, SqlQueryService.SqlParameterSetter parameterSetter, SqlQueryService.KeyMapper<K> keyMapper, SqlQueryService.ValueMapper<V> valueMapper) {
        Map<K, V> map = sqlQueryService.findMap(connection, query, keyMapper, valueMapper);
        this.closeConnection();
        return map;
    }

    public int insertQuery(String query, SqlQueryService.SqlParameterSetter parameterSetter) {
        int i = sqlQueryService.insertQuery(connection, query, parameterSetter);
        this.closeConnection();
        return i;
    }

    public void updateQuery(String query, SqlQueryService.SqlParameterSetter parameterSetter) {
        sqlQueryService.updateQuery(connection, query, parameterSetter);
        this.closeConnection();
    }

    public void deleteQuery(String query, SqlQueryService.SqlParameterSetter parameterSetter) {
        sqlQueryService.deleteQuery(connection, query, parameterSetter);
        this.closeConnection();
    }
}
