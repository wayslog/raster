#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/syscall.h>
#include <unistd.h>

/* The frozen drivers create their device pool before their workload threads.
 * This contract gives the pool, coordinator and each worker distinct affinity;
 * it must be verified at workload entry when changing a driver. */
typedef int (*create_fn)(pthread_t *, const pthread_attr_t *, void *(*)(void *), void *);
static create_fn create_real;
static pthread_once_t resolve_once = PTHREAD_ONCE_INIT;
static _Atomic unsigned next_thread;
static _Atomic unsigned started_workers;
static _Atomic unsigned failures;
static unsigned first_worker, worker_count;
static int coordinator_cpu, io_cpu, worker_cpus[CPU_SETSIZE];
static int initialized;

struct start_context {
    void *(*entry)(void *);
    void *argument;
    unsigned ordinal;
};
static void fail(const char *message) {
    fprintf(stderr, "RASTER_AFFINITY error=%s\n", message);
    abort();
}
static unsigned number(const char *name) {
    const char *value = getenv(name);
    char *end;
    if (!value || !*value) fail("missing_configuration");
    errno = 0;
    unsigned long parsed = strtoul(value, &end, 10);
    if (errno || *end || parsed >= CPU_SETSIZE || *value == '-') fail("invalid_number");
    return (unsigned)parsed;
}
static void pin(int cpu, const char *role, unsigned index) {
    cpu_set_t expected, actual;
    CPU_ZERO(&expected);
    CPU_SET(cpu, &expected);
    if (pthread_setaffinity_np(pthread_self(), sizeof(expected), &expected) ||
        pthread_getaffinity_np(pthread_self(), sizeof(actual), &actual) ||
        !CPU_EQUAL(&expected, &actual)) fail("affinity_mismatch");
    flockfile(stderr);
    fprintf(stderr, "RASTER_AFFINITY role=%s index=%u cpu=%d tid=%ld\n",
            role, index, cpu, (long)syscall(SYS_gettid));
    funlockfile(stderr);
}
__attribute__((constructor)) static void configure(void) {
    cpu_set_t allowed, assigned;
    if (pthread_getaffinity_np(pthread_self(), sizeof(allowed), &allowed)) fail("cannot_read_mask");
    coordinator_cpu = (int)number("RASTER_BENCH_COORDINATOR_CPU");
    io_cpu = (int)number("RASTER_BENCH_IO_CPU");
    first_worker = number("RASTER_BENCH_WORKER_FIRST");
    worker_count = number("RASTER_BENCH_WORKER_COUNT");
    if (coordinator_cpu == io_cpu) fail("overlapping_roles");
    CPU_ZERO(&assigned);
    CPU_SET(coordinator_cpu, &assigned);
    CPU_SET(io_cpu, &assigned);
    const char *list = getenv("RASTER_BENCH_WORKER_CPUS");
    if (!list) fail("missing_worker_cpus");
    for (unsigned i = 0; i < worker_count; ++i) {
        char *end;
        if (!*list || *list == '-') fail("missing_worker_cpu");
        errno = 0;
        unsigned long cpu = strtoul(list, &end, 10);
        if (errno || end == list || cpu >= CPU_SETSIZE || CPU_ISSET(cpu, &assigned)) fail("invalid_worker_cpu");
        if ((i + 1 < worker_count && *end != ',') || (i + 1 == worker_count && *end)) fail("worker_count_mismatch");
        worker_cpus[i] = (int)cpu;
        CPU_SET(cpu, &assigned);
        list = *end ? end + 1 : end;
    }
    if (*list) fail("unexpected_worker_cpu");
    for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu) {
        if (CPU_ISSET(cpu, &assigned) && !CPU_ISSET(cpu, &allowed)) fail("cpu_outside_process_mask");
    }
    initialized = 1;
    pin(coordinator_cpu, "coordinator", 0);
}
__attribute__((destructor)) static void verify_completion(void) {
    if (!initialized) return;
    unsigned workers = atomic_load_explicit(&started_workers, memory_order_relaxed);
    unsigned errors = atomic_load_explicit(&failures, memory_order_relaxed);
    fprintf(stderr, "RASTER_AFFINITY complete workers=%u expected=%u failures=%u\n", workers, worker_count, errors);
    if (workers != worker_count || errors) fail("incomplete_worker_set");
}
static void resolve_create(void) {
    create_real = (create_fn)dlsym(RTLD_NEXT, "pthread_create");
    if (!create_real) fail("missing_pthread_create");
}
static void *start_pinned(void *raw) {
    struct start_context *context = raw;
    void *(*entry)(void *) = context->entry;
    void *argument = context->argument;
    unsigned ordinal = context->ordinal;
    free(context);
    if (ordinal >= first_worker && ordinal - first_worker < worker_count) {
        unsigned worker = ordinal - first_worker;
        pin(worker_cpus[worker], "worker", worker);
        atomic_fetch_add_explicit(&started_workers, 1, memory_order_relaxed);
    } else {
        pin(io_cpu, "auxiliary", ordinal);
    }
    return entry(argument);
}
int pthread_create(pthread_t *thread, const pthread_attr_t *attributes,
                   void *(*entry)(void *), void *argument) {
    if (!initialized) fail("uninitialized_library");
    pthread_once(&resolve_once, resolve_create);
    struct start_context *context = malloc(sizeof(*context));
    if (!context) {
        atomic_fetch_add_explicit(&failures, 1, memory_order_relaxed);
        return ENOMEM;
    }
    context->entry = entry;
    context->argument = argument;
    context->ordinal = atomic_fetch_add_explicit(&next_thread, 1, memory_order_relaxed);
    int status = create_real(thread, attributes, start_pinned, context);
    if (status) {
        atomic_fetch_add_explicit(&failures, 1, memory_order_relaxed);
        free(context);
    }
    return status;
}
