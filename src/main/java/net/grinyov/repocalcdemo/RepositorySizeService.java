package net.grinyov.repocalcdemo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
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
import java.util.concurrent.locks.ReentrantLock;


public class RepositorySizeService {

    private static final Logger LOGGER = LoggerFactory.getLogger(RepositorySizeService.class);

    private static final long DEFAULT_TIMEOUT = 300;

    private final ExecutorService executor;
    private final Map<String, Future<Long>> cache;
    private ReentrantLock lock = new ReentrantLock();

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
            LOGGER.debug("Created the new scheduler and started cleaning process");
            cache.forEach((key, value) -> {
                if (value.isDone() || value.isCancelled()) {
                    LOGGER.debug("The data for repository {} was removed from cache", key);
                    cache.remove(key);
                }
            });
        }, 1, DEFAULT_TIMEOUT / 5, TimeUnit.SECONDS);
    }

    public long calculateRepoSize(String id) {
        LOGGER.debug("Started calculating the size of repository {}", id);
        lock.lock();
        if (cache.containsKey(id)) {
            try {
                LOGGER.debug("The size of the repository {} is still being calculated.", id);
                return getResult(cache.get(id));
            } finally {
                lock.unlock();
            }
        }
        CompletionService<Long> completionService = new ExecutorCompletionService<>(executor);
        Future<Long> future = completionService.submit(() -> {
            // here calculate repo size
            LOGGER.debug("calculating the size by thread {}", Thread.currentThread().getName());
            List<Integer> woprkList = new ArrayList<>();
            for (int i = 0; i < 10_000_000; i++) {
                woprkList.add(new Random().nextInt());
            }
            long acc = woprkList.stream()
                    .sorted()
                    .reduce(0, Integer::sum);
            LOGGER.debug("Finished calculating the size by thread {}", Thread.currentThread().getName());
            return Math.abs(acc);
        });
        cache.putIfAbsent(id, future);
        return getResult(future);
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