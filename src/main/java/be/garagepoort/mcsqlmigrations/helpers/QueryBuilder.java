package be.garagepoort.mcsqlmigrations.helpers;

import be.garagepoort.mcsqlmigrations.SqlConnectionProvider;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public abstract class QueryBuilder {

    protected final Connection connection;
    private boolean transactional = false;

    public QueryBuilder(SqlConnectionProvider connectionProvider) {
        this.connection = connectionProvider.getConnection();
    }

    public QueryBuilder startTransaction() {
        try {
            connection.setAutoCommit(false);
            transactional = true;
            return this;
        } catch (SQLException e) {
            throw new DatabaseException(e);
        } finally {
            closeConnection();
        }
    }

    public void commit() {
        try {
            connection.commit();
            transactional = false;
            closeConnection();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        } finally {
            closeConnection();
        }
    }

    private void closeConnection() {
        try {
            if (!transactional) {
                connection.close();
            }
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    public <T> Optional<T> findOne(String query, SqlParameterSetter parameterSetter, RowMapper<T> rowMapper) {
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            parameterSetter.accept(ps);
            try (ResultSet rs = ps.executeQuery()) {
                boolean first = rs.next();
                if (first) {
                    return Optional.of(rowMapper.apply(rs));
                }
            }
            return Optional.empty();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        } finally {
            this.closeConnection();
        }
    }

    public <T> T getOne(String query, RowMapper<T> rowMapper) {
        return getOne(query, (rs) -> {
        }, rowMapper);
    }

    public <T> T getOne(String query, SqlParameterSetter parameterSetter, RowMapper<T> rowMapper) {
        try (PreparedStatement ps = connection.prepareStatement(query)) {
            parameterSetter.accept(ps);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rowMapper.apply(rs);
            }
        } catch (SQLException e) {
            throw new DatabaseException(e);
        } finally {
            this.closeConnection();
        }
    }

    public <T> List<T> find(String query, RowMapper<T> rowMapper) {
        return find(query, (rs) -> {
        }, rowMapper);
    }

    public <T> List<T> find(String query, SqlParameterSetter parameterSetter, RowMapper<T> rowMapper) {
        try (PreparedStatement ps = connection.prepareStatement(query)
        ) {
        List<T> results = new ArrayList<>();
            parameterSetter.accept(ps);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    results.add(rowMapper.apply(rs));
                }
            }
        return results;
        } catch (SQLException e) {
            throw new DatabaseException(e);
        } finally {
            this.closeConnection();
        }
    }

    public <K, V> Map<K, V> findMap(String query, KeyMapper<K> keyMapper, ValueMapper<V> valueMapper) {
        return findMap(query, s -> {
        }, keyMapper, valueMapper);
    }

    public <K, V> Map<K, V> findMap(String query, SqlParameterSetter parameterSetter, KeyMapper<K> keyMapper, ValueMapper<V> valueMapper) {
        Map<K, V> results = new HashMap<>();
        try (PreparedStatement ps = connection.prepareStatement(query)
        ) {
            parameterSetter.accept(ps);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    results.put(keyMapper.apply(rs), valueMapper.apply(rs));
                }
            }
        } catch (SQLException e) {
            throw new DatabaseException(e);
        } finally {
            this.closeConnection();
        }
        return results;
    }

    public int insertQuery(String query, SqlParameterSetter parameterSetter) {
        try (PreparedStatement insert = connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS)) {
            parameterSetter.accept(insert);
            insert.executeUpdate();
            return getGeneratedId(connection, insert);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        } finally {
            this.closeConnection();
        }
    }

    public void updateQuery(String query, SqlParameterSetter parameterSetter) {
        try (PreparedStatement insert = connection.prepareStatement(query)) {
            parameterSetter.accept(insert);
            insert.executeUpdate();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        } finally {
            this.closeConnection();
        }
    }

    public void deleteQuery(String query, SqlParameterSetter parameterSetter) {
        try (PreparedStatement insert = connection.prepareStatement(query)) {
            parameterSetter.accept(insert);
            insert.executeUpdate();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        } finally {
            this.closeConnection();
        }
    }

    protected abstract Integer getGeneratedId(Connection connection, PreparedStatement insert) throws SQLException;

    public interface SqlParameterSetter {
        void accept(PreparedStatement preparedStatement) throws SQLException;
    }

    public interface RowMapper<T> {
        T apply(ResultSet rs) throws SQLException;
    }

    public interface KeyMapper<K> {
        K apply(ResultSet rs) throws SQLException;
    }

    public interface ValueMapper<K> {
        K apply(ResultSet rs) throws SQLException;
    }
}
