package net.grinyov.repocalcdemo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class RepositorySizeService {

    private static final Logger LOGGER = LoggerFactory.getLogger(RepositorySizeService.class);

    private static final long DEFAULT_TIMEOUT = 300;

    private final ExecutorService executor;
    private final Map<String, Future<Long>> cache;

    public RepositorySizeService(ExecutorService executor, Map<String, Future<Long>> cache) {
        this.executor = executor;
        this.cache = cache;
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r);
            thread.setName("SchedulerThread");
            thread.setDaemon(true);
            return thread;
        });
        scheduler.scheduleAtFixedRate(() -> {
            System.out.println("Created the new scheduler and started cleaning process");
            cache.forEach((key, value) -> {
                if (value.isDone() || value.isCancelled())  {
                    System.out.println("The data for repository " + key + "was removed from cache");
                    cache.remove(key);
                }
            });
        }, 1, DEFAULT_TIMEOUT/5, TimeUnit.SECONDS);
    }

    public long calculateRepoSize(String id) {
        System.out.println("Started calculating the size of repository " +  id);
        LOGGER.debug("Started calculating the size of repository {}",  id);
        if (cache.containsKey(id)) {
            System.out.println("The size of the repository " + id +"  is still being calculated.");
            LOGGER.debug("The size of the repository {} is still being calculated.", id);
            return getResult(cache.get(id));
        }
        CompletionService<Long> completionService = new ExecutorCompletionService<>(executor);
        Future<Long> future = completionService.submit(() -> {
            // here calculate repo size
            System.out.println("calculating the size by thread " + Thread.currentThread().getName());
            LOGGER.debug("calculating the size by thread {}", Thread.currentThread().getName());
            TimeUnit.SECONDS.sleep(3);
            System.out.println("Finished calculating the size by thread " + Thread.currentThread().getName());
            LOGGER.debug("Finished calculating the size by thread {}", Thread.currentThread().getName());
            return Math.abs(new Random().nextLong());
        });
        cache.putIfAbsent(id, future);
        while (!future.isDone()) {
            return getResult(future);
        }
        return 0;
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