package be.garagepoort.mcsqlmigrations.helpers;

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

public interface SqlQueryService {

    Connection getConnection();

    default <T> Optional<T> findOne(Connection sql, String query, SqlParameterSetter parameterSetter, RowMapper<T> rowMapper) {
        try (PreparedStatement ps = sql.prepareStatement(query)) {
            parameterSetter.accept(ps);
            try (ResultSet rs = ps.executeQuery()) {
                boolean first = rs.next();
                if (first) {
                    return Optional.of(rowMapper.apply(rs));
                }
            }
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
        return Optional.empty();
    }

    default <T> T getOne(Connection sql, String query, RowMapper<T> rowMapper) {
        return getOne(sql, query, (rs) -> {
        }, rowMapper);
    }

    default <T> T getOne(Connection sql, String query, SqlParameterSetter parameterSetter, RowMapper<T> rowMapper) {
        try (PreparedStatement ps = sql.prepareStatement(query)) {
            parameterSetter.accept(ps);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rowMapper.apply(rs);
            }
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    default <T> List<T> find(Connection sql, String query, RowMapper<T> rowMapper) {
        return find(sql, query, (rs) -> {
        }, rowMapper);
    }

    default <T> List<T> find(Connection sql, String query, SqlParameterSetter parameterSetter, RowMapper<T> rowMapper) {
        List<T> results = new ArrayList<>();
        try (PreparedStatement ps = sql.prepareStatement(query)
        ) {
            parameterSetter.accept(ps);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    T mappedRow = rowMapper.apply(rs);
                    results.add(mappedRow);
                }
            }
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
        return results;
    }

    default <K, V> Map<K, V> findMap(Connection sql, String query, KeyMapper<K> keyMapper, ValueMapper<V> valueMapper) {
        return findMap(sql, query, s -> {
        }, keyMapper, valueMapper);
    }

    default <K, V> Map<K, V> findMap(Connection sql, String query, SqlParameterSetter parameterSetter, KeyMapper<K> keyMapper, ValueMapper<V> valueMapper) {
        Map<K, V> results = new HashMap<>();
        try (PreparedStatement ps = sql.prepareStatement(query)
        ) {
            parameterSetter.accept(ps);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    results.put(keyMapper.apply(rs), valueMapper.apply(rs));
                }
            }
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
        return results;
    }

    default int insertQuery(Connection sql, String query, SqlParameterSetter parameterSetter) {
        try (PreparedStatement insert = sql.prepareStatement(query, Statement.RETURN_GENERATED_KEYS)) {
            parameterSetter.accept(insert);
            insert.executeUpdate();
            return getGeneratedId(sql, insert);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    default void updateQuery(Connection sql, String query, SqlParameterSetter parameterSetter) {
        try (PreparedStatement insert = sql.prepareStatement(query)) {
            parameterSetter.accept(insert);
            insert.executeUpdate();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    default void deleteQuery(Connection sql, String query, SqlParameterSetter parameterSetter) {
        try (PreparedStatement insert = sql.prepareStatement(query)) {
            parameterSetter.accept(insert);
            insert.executeUpdate();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    Integer getGeneratedId(Connection connection, PreparedStatement insert) throws SQLException;

    interface SqlParameterSetter {
        void accept(PreparedStatement preparedStatement) throws SQLException;
    }

    interface RowMapper<T> {
        T apply(ResultSet rs) throws SQLException;
    }

    interface KeyMapper<K> {
        K apply(ResultSet rs) throws SQLException;
    }

    interface ValueMapper<K> {
        K apply(ResultSet rs) throws SQLException;
    }
}
