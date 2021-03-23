package be.garagepoort.mcsqlmigrations;

import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class MigrationsLoader {

    public static List<Migration> load(List<String> packages) {
        try {
            List<Migration> result = new ArrayList<>();
            for (String aPackage : packages) {
                Reflections reflections = new Reflections(aPackage, new TypeAnnotationsScanner(), new SubTypesScanner());
                Set<Class<? extends Migration>> subTypesOf = reflections.getSubTypesOf(Migration.class);
                for (Class<? extends Migration> aClass : subTypesOf) {
                    Constructor<?>[] constructors = aClass.getConstructors();
                    Constructor<?> constructor = constructors[0];
                    result.add((Migration) constructor.newInstance());
                }
            }
            return result;
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new MigrationsException(e);
        }
    }
}
