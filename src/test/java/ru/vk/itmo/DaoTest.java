package ru.vk.itmo;

import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import ru.vk.itmo.test.DaoFactory;

import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.CodeSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Target({ElementType.ANNOTATION_TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@ParameterizedTest
@ArgumentsSource(DaoTest.DaoList.class)
@ExtendWith(DaoTest.DaoList.class)
@Timeout(10)
public @interface DaoTest {

    int stage();

    /**
     * 0 - no max stage, test will be executed if stage <= factory stage
     * n > 0 - test will be executed if stage <= factory stage >= maxStage
     */
    int maxStage() default 0;

    class DaoList implements ArgumentsProvider, ExecutionCondition {
        private static final AtomicInteger ID = new AtomicInteger();
        private static final ExtensionContext.Namespace NAMESPACE = ExtensionContext.Namespace.create("dao");

        private List<Class<?>> getFactories(ExtensionContext context) throws Exception {
            if (context.getStore(NAMESPACE).get("factories") == null) {
                CodeSource codeSource = DaoFactory.class.getProtectionDomain().getCodeSource();
                Path path = Path.of(codeSource.getLocation().toURI());
                try (Stream<Path> walk = Files.walk(path)) {
                    @SuppressWarnings("SimplifyStreamApiCallChains")
                    List<Class<?>> factories = walk
                            .filter(p -> p.getFileName().toString().endsWith(".class"))
                            .map(p -> getDaoClass(path, p))
                            .filter(Objects::nonNull)
                            .collect(Collectors.toList());

                    List<Class<?>> maxFactories = new ArrayList<>();
                    long maxStage = 0;
                    for (Class<?> factory : factories) {
                        DaoFactory annotation = factory.getAnnotation(DaoFactory.class);
                        long stage = ((long) annotation.stage()) << 32 | annotation.week();
                        if (stage < maxStage) {
                            continue;
                        }
                        if (stage > maxStage) {
                            maxStage = stage;
                            maxFactories.clear();
                        }
                        maxFactories.add(factory);
                    }

                    if (maxFactories.isEmpty()) {
                        throw new IllegalStateException("No DaoFactory declared under ru.vk.itmo.test.<username> package");
                    }
                    context.getStore(NAMESPACE).put("factories", maxFactories);
                }
            }
            //noinspection unchecked
            return (List<Class<?>>) context.getStore(NAMESPACE).get("factories");
        }

        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext context) throws Exception {
            List<Class<?>> maxFactories = getFactories(context);

            if (maxFactories.isEmpty()) {
                throw new IllegalStateException("No DaoFactory declared under ru.vk.itmo.test.<username> package");
            }

            return maxFactories.stream().map(c -> {
                try {
                    Class<?> parameterType = context.getRequiredTestMethod().getParameterTypes()[0];
                    if (parameterType == Dao.class) {
                        Dao<String, Entry<String>> dao = createDao(context, c);
                        return Arguments.of(dao);
                    } else if (parameterType == Supplier.class) {
                        return Arguments.of((Supplier<Dao<String, Entry<String>>>) () -> {
                            try {
                                return createDao(context, c);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
                    }
                    throw new IllegalArgumentException("Unknown type:" + parameterType);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }

        private Class<?> getDaoClass(Path path, Path p) {
            StringBuilder result = new StringBuilder();
            for (Path subPath : path.relativize(p)) {
                result.append(subPath).append(".");
            }
            String className = result.substring(0, result.length() - ".class.".length());
            Class<?> clazz;
            try {
                clazz = Class.forName(className, false, DaoFactory.class.getClassLoader());
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
            if (clazz.getAnnotation(DaoFactory.class) == null) {
                return null;
            }
            if (!clazz.getPackageName().startsWith("ru.vk.itmo.test.")) {
                throw new IllegalArgumentException("DaoFactory should be under package ru.vk.itmo.test.<username>");
            }
            return clazz;
        }

        private Dao<String, Entry<String>> createDao(ExtensionContext context, Class<?> clazz) throws IOException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
            Path tmp = Files.createTempDirectory("dao");

            DaoFactory.Factory<?, ?> f = (DaoFactory.Factory<?, ?>) clazz.getDeclaredConstructor().newInstance();
            Dao<String, Entry<String>> dao = f.createStringDao(new Config(tmp));

            ExtensionContext.Store.CloseableResource res = () -> {
                dao.close();
                if (!Files.exists(tmp)) {
                    return;
                }
                Files.walkFileTree(tmp, new SimpleFileVisitor<>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                        Files.delete(file);
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                        Files.delete(dir);
                        return FileVisitResult.CONTINUE;
                    }
                });
            };

            context.getStore(NAMESPACE).put(ID.incrementAndGet() + "", res);

            return dao;
        }

        @Override
        public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
            try {
                List<Class<?>> factories = getFactories(context);
                int minStage = context.getRequiredTestMethod().getAnnotation(DaoTest.class).stage();
                int maxStage = context.getRequiredTestMethod().getAnnotation(DaoTest.class).maxStage();
                if (factories.isEmpty()) {
                    throw new IllegalStateException("No DaoFactory declared under ru.vk.itmo.test.<username> package");
                }
                int daoStage = factories.get(0).getAnnotation(DaoFactory.class).stage();
                if (minStage > daoStage || (maxStage > 0 && maxStage < daoStage)) {
                    return ConditionEvaluationResult.disabled("Implementation is not ready");
                }
                return ConditionEvaluationResult.enabled("Implementation is ready");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

}