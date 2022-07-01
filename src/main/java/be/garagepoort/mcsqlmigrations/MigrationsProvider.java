package be.garagepoort.mcsqlmigrations;

import java.util.List;

public interface MigrationsProvider {

    List<? extends Migration> getMigrations();
}
