package com.github.seregamorph.maven.test.builder;

import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.apache.maven.project.MavenProject;

class SignalingExecutorCompletionService {

    /**
     * See {@link com.github.seregamorph.maven.test.builder.SignalMojo#ATTR_SIGNALER}
     */
    private static final String ATTR_SIGNALER = "signaler";

    private final ExecutorService executor;
    private final BlockingQueue<Try<MavenProject>> signaledQueue;

    SignalingExecutorCompletionService(ExecutorService executor) {
        this.executor = Objects.requireNonNull(executor);
        this.signaledQueue = new LinkedBlockingQueue<>();
    }

    Future<?> submit(MavenProject project, Callable<MavenProject> call) {
        Objects.requireNonNull(call);
        return executor.submit(new FutureTask<>(() -> {
            AtomicBoolean signaled = new AtomicBoolean(false);
            project.setContextValue(ATTR_SIGNALER, (Consumer<MavenProject>) $ ->{
                // no race condition here with "if (!signaled.get())" block, because it's the same thread
                signaled.set(true);
                signaledQueue.add(Try.success(project));
            });
            try {
                MavenProject result = call.call();
                if (!signaled.get()) {
                    signaledQueue.add(Try.success(result));
                }
                return result;
            } catch (Throwable e) {
                signaledQueue.add(Try.failure(e));
                if (e instanceof Exception) {
                    throw e;
                } else {
                    throw new RuntimeException(e);
                }
            }
        }));
    }

    public MavenProject takeSignaled() throws InterruptedException, ExecutionException {
        Try<MavenProject> t = signaledQueue.take();
        return t.get();
    }

    private abstract static class Try<T> {
        abstract T get() throws ExecutionException;

        static <T> Try<T> success(T value) {
            return new Try<T>() {
                @Override
                T get() {
                    return value;
                }
            };
        }

        static <T> Try<T> failure(Throwable e) {
            return new Try<>() {
                @Override
                T get() throws ExecutionException {
                    throw new ExecutionException(e);
                }
            };
        }
    }
}
