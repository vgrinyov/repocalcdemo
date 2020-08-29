package net.grinyov.repocalcdemo;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class RepositorySizeServiceTest {

    private static final int NTHREADS = 100;
    private RepositorySizeService testingService;
    private ExecutorService executorService;
    private Map<String, Future<Long>> cache;

    @Before
    public void setUp() throws Exception {
        executorService = Executors.newFixedThreadPool(NTHREADS);
        cache = new ConcurrentHashMap<>(100);
        testingService = new RepositorySizeService(executorService, cache);
    }

    @After
    public void tearDown() throws Exception {
        executorService.shutdown();
    }

    @Test
    public void testSyncRepeatedCall() throws Exception {
        String repoId1 = "sameRepoId";
        String repoId2 = "testRepoId1";
        String repoId3 = "sameRepoId";
        long repoSize = testingService.calculateRepoSize(repoId1);
        long repoSize2 = testingService.calculateRepoSize(repoId2);
        TimeUnit.SECONDS.sleep(1);
        long repoSize3 = testingService.calculateRepoSize(repoId3);
        assertEquals(repoSize, repoSize3);
        assertNotEquals(repoSize, repoSize2);
        assertNotEquals(repoSize2, repoSize3);
        System.out.println("repoSize =" + repoSize);
        System.out.println("repoSize2 =" + repoSize2);
        System.out.println("repoSize3 =" + repoSize3);
    }

    @Test
    public void testAsyncRepeatedCall() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        List<Future<Long>> futures = new ArrayList<>();
        List<Callable> callables = initCallableTestData();
        callables.forEach(
                testData -> futures.add(executor.submit(testData)
                ));
        TimeUnit.SECONDS.sleep(5);
        List<Long> results = extractCalculatedSize(futures);
        assertEquals(results.get(0), results.get(2));
        assertNotEquals(results.get(0), results.get(1));
        assertNotEquals(results.get(0), results.get(3));
        assertNotEquals(results.get(1), results.get(2));
        assertNotEquals(results.get(1), results.get(3));
        results.forEach(System.out::println);
        executor.shutdown();
    }

    private List<Callable> initCallableTestData() {
        List<String> repoIds = initTestData();
        List<Callable> callableList = new ArrayList<>();
        repoIds.forEach(
                id ->
                        callableList.add(() ->
                                testingService.calculateRepoSize(id))
        );
        return callableList;
    }

    private List<String> initTestData() {
        return List.of("sameRepoId", "testRepoId1", "sameRepoId", "testRepoId2");
    }

    private List<Long> extractCalculatedSize(List<Future<Long>> futures) {
        List<Long> results = new ArrayList<>();
        futures.forEach(
                future -> results.add(getResult(future))
        );
        return results;
    }

    private long getResult(Future<Long> future) {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            throw new IllegalStateException("Unexpected error", e);
        }
        return 0;
    }
}

