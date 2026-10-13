#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <stdlib.h>

typedef int (*create_fn)(pthread_t *, const pthread_attr_t *, void *(*)(void *), void *);
static create_fn create_real;
static pthread_once_t resolve_once = PTHREAD_ONCE_INIT;
static _Atomic unsigned next_cpu;
struct start_context {
    void *(*entry)(void *);
    void *argument;
    cpu_set_t affinity;
};
static void resolve_create(void) {
    create_real = (create_fn)dlsym(RTLD_NEXT, "pthread_create");
    if (!create_real) abort();
}
static void *start_pinned(void *raw) {
    struct start_context *context = raw;
    void *(*entry)(void *) = context->entry;
    void *argument = context->argument;
    if (pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &context->affinity)) abort();
    free(context);
    return entry(argument);
}
int pthread_create(pthread_t *thread, const pthread_attr_t *attributes,
                   void *(*entry)(void *), void *argument) {
    pthread_once(&resolve_once, resolve_create);
    cpu_set_t allowed;
    if (pthread_getaffinity_np(pthread_self(), sizeof(allowed), &allowed)) return EINVAL;
    unsigned count = CPU_COUNT(&allowed);
    if (!count) return EINVAL;
    unsigned chosen = atomic_fetch_add_explicit(&next_cpu, 1, memory_order_relaxed) % count;
    struct start_context *context = malloc(sizeof(*context));
    if (!context) return ENOMEM;
    context->entry = entry;
    context->argument = argument;
    CPU_ZERO(&context->affinity);
    for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu) {
        if (CPU_ISSET(cpu, &allowed) && chosen-- == 0) {
            CPU_SET(cpu, &context->affinity);
            break;
        }
    }
    int status = create_real(thread, attributes, start_pinned, context);
    if (status) free(context);
    return status;
}
