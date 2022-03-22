package be.garagepoort.mcsqlmigrations.helpers;

import be.garagepoort.mcsqlmigrations.SqlConnectionProvider;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class SqliteQueryBuilder extends QueryBuilder {
    private static final Object LOCK = new Object();

    public SqliteQueryBuilder(SqlConnectionProvider connectionProvider) {
        super(connectionProvider);
    }

    public QueryBuilder startTransaction() {
        synchronized (LOCK) {
            return super.startTransaction();
        }
    }

    public void commit() {
        synchronized (LOCK) {
            super.commit();
        }
    }

    public <T> Optional<T> findOne(String query, SqlParameterSetter parameterSetter, RowMapper<T> rowMapper) {
        synchronized (LOCK) {
            return super.findOne(query, parameterSetter, rowMapper);
        }
    }

    public <T> T getOne(String query, RowMapper<T> rowMapper) {
        synchronized (LOCK) {
            return super.getOne(query, rowMapper);
        }
    }

    public <T> T getOne(String query, SqlParameterSetter parameterSetter, RowMapper<T> rowMapper) {
        synchronized (LOCK) {
            return super.getOne(query, parameterSetter, rowMapper);
        }
    }

    public <T> List<T> find(String query, RowMapper<T> rowMapper) {
        synchronized (LOCK) {
            return super.find(query, rowMapper);
        }
    }

    public <T> List<T> find(String query, SqlParameterSetter parameterSetter, RowMapper<T> rowMapper) {
        synchronized (LOCK) {
            return super.find(query, parameterSetter, rowMapper);
        }
    }

    public <K, V> Map<K, V> findMap(String query, KeyMapper<K> keyMapper, ValueMapper<V> valueMapper) {
        synchronized (LOCK) {
            return super.findMap(query, keyMapper, valueMapper);
        }
    }

    public <K, V> Map<K, V> findMap(String query, SqlParameterSetter parameterSetter, KeyMapper<K> keyMapper, ValueMapper<V> valueMapper) {
        synchronized (LOCK) {
            return super.findMap(query, parameterSetter, keyMapper, valueMapper);
        }
    }

    public int insertQuery(String query, SqlParameterSetter parameterSetter) {
        synchronized (LOCK) {
            return super.insertQuery(query, parameterSetter);
        }
    }

    public void updateQuery(String query, SqlParameterSetter parameterSetter) {
        synchronized (LOCK) {
            super.updateQuery(query, parameterSetter);
        }
    }

    public void deleteQuery(String query, SqlParameterSetter parameterSetter) {
        synchronized (LOCK) {
            super.deleteQuery(query, parameterSetter);
        }
    }

    @Override
    public Integer getGeneratedId(Connection connection, PreparedStatement insert) throws SQLException {
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
