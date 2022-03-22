package be.garagepoort.mcsqlmigrations.helpers;

public class QueryBuilderFactory {

    private final SqlQueryService sqlQueryService;

    public QueryBuilderFactory(SqlQueryService sqlQueryService) {
        this.sqlQueryService = sqlQueryService;
    }

    public QueryBuilder create() {
        return new QueryBuilder(sqlQueryService);
    }
}
