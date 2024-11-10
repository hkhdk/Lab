#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <assert.h>
#include <sys/time.h>
#include <errno.h>
#include "qthread.h"


//  switch-x86_64.S
extern void switch_thread(void **location_for_old_sp, void *new_value);
// extern void start_thread(void *stack, void *func, void *arg1, void *arg2);
extern void *setup_stack(void *_stack, size_t len, void *func, void *arg1, void *arg2);

static long get_usecs(void);

const int MAX_SIZE = 33;  // 最多有 (33 - 1) = 32个线程
const int STACK_SIZE = 2048; // 假设是2048

/* this is your qthread structure.
 */
struct qthread {
    struct qthread *next; // 不是链表，可以不要
    void *stack_pointer;  // 堆栈指针
    long int sleep_time;  // 睡眠到哪个时间点
    void* val;   // 线程退出时的返回值
    int isExit;  // 线程是否退出
    int isWait;  // 线程是否需要等待，而不是就绪
};

/* You'll probably want to define a thread queue structure, and
 * functions to append and remove threads. (Note that you only need to
 * remove the oldest item from the head, makes removal a lot easier)
 */

// 模拟循环队列
struct threadq {
    int head;
    int tail;
    qthread_t array[MAX_SIZE];

    int empty(void) {
        return head == tail;
    }

    int full(void)
    {
        return (tail + 1) % MAX_SIZE == head;
    }

    void append(qthread_t thread) {
        if (!full()) {
            array[tail] = thread;
            tail = (tail + 1) % MAX_SIZE;
        }
    }

    qthread_t pop(void) {
        qthread_t thread = array[head];
        array[head] = NULL;
        head = (head + 1) % MAX_SIZE;
        return thread;
    }
};

struct qthread_mutex {
    struct qthread *owner;  // 当前拥有该信号量的线程
    struct threadq waiters; // 所有等待该信号量的线程
};

struct qthread_cond {
    struct threadq waiters;  // 所有等待该条件变量的线程
};

qthread_t current_thread;  // 当前运行线程
struct threadq active_threads;  // 就绪队列

qthread_t sleep_threads[MAX_SIZE];  // 当前正在睡眠的线程

/* I suggest factoring your code so that you have a 'schedule'
 * function which selects the next thread to run and @switches to it,
 * or goes to sleep if there aren't any threads left to run.
 *
 * NOTE - if you end up switching back to the same thread, do *NOT*
 * use do_switch - check for this case and return from schedule(), 
 * or else @you'll crash.
 */

// 线程调度器
void schedule(void *save_location) {
    if (!active_threads.empty()) {
        current_thread = active_threads.pop(); // 从就绪队列中选择一个线程运行
        if (current_thread != save_location)  // 线程不同，执行上下文切换
            switch_thread(&save_location, current_thread->stack_pointer);  // 切换线程，保存原来线程栈指针，设置新的栈指针
    }
}

/* qthread_init - set up a thread structure for the main (OS-provided) thread
 */
void qthread_init(void) {  // 初始化一个当前线程(主线程)
    current_thread = (qthread_t)malloc(sizeof(struct qthread));
    current_thread->stack_pointer = NULL;
    active_threads.head = 0;
    active_threads.tail = 0;
}

qthread_t create_2arg_thread(f_2arg_t f, void *arg1, void *arg2)  // 创建新线程
{
    qthread_t new_thread = (qthread_t)malloc(sizeof(struct qthread));
    // 分配栈空间
    void *stack = malloc(STACK_SIZE);
    // 设置栈(把所有线程参数放进栈里面)
    new_thread->stack_pointer = setup_stack(stack, STACK_SIZE, (void*)f, (void*)arg1, arg2);
    new_thread->sleep_time = 0;

    // 把新创建的线程放进就绪队列，等待调度
    active_threads.append(new_thread);
    return new_thread;
}

void wrapper(void *arg1, void *arg2)
{
    // 执行线程要运行的函数
    f_1arg_t f = (f_1arg_t)arg1;
    // 执行完返回参数(1、2、3...)，对应test.c里面的run_test1函数
    void *tmp = f(arg2);
    // 创建的线程执行完后，自动退出
    qthread_exit(tmp);
}

qthread_t qthread_create(f_1arg_t f, void *arg)
{
    return create_2arg_thread(wrapper, (void*)f, arg);
}


/* qthread_yield - yield to the next @runnable thread.
 */
void qthread_yield(void) {  //  让出调度器
    // 如果当前线程无需等待 && 没有退出 && 无需睡眠，直接放进就绪队列
    if (!current_thread->isWait && !current_thread->isExit && current_thread->sleep_time == 0) 
        active_threads.append(current_thread);

    // 唤醒睡眠时间到的线程
    for (int i = 0; i < MAX_SIZE; ++i)
    {
        if (sleep_threads[i] != NULL && get_usecs() >= sleep_threads[i]->sleep_time) {
            active_threads.append(sleep_threads[i]);  //  睡眠时间到，放进就绪队列
            sleep_threads[i]->sleep_time = 0;
            sleep_threads[i] = NULL;
        }
    }
    // 调度其它线程
    schedule(current_thread);
}


/* qthread_exit, qthread_join - exit argument is returned by
 * qthread_join. Note that join blocks if the thread hasn't exited
 * yet, and is allowed to crash @if the thread doesn't exist.
 */
void qthread_exit(void *val) {
    // 退出当前线程，将返回值val保存起来
    current_thread->val = val;
    current_thread->isExit = 1;
    qthread_yield();
}

void *qthread_join(qthread_t thread) {
    while (!thread->isExit) {
        qthread_yield();  // 等待的线程没有退出，一直join
    }
    return thread->val;
}

qthread_mutex_t *qthread_mutex_create(void) {  // 创建互斥锁
    qthread_mutex_t *mutex = (qthread_mutex_t*)malloc(sizeof(qthread_mutex_t));
    mutex->owner = NULL;
    mutex->waiters.head = 0;
    mutex->waiters.tail = 0;
    return mutex;
}

void qthread_mutex_destroy(qthread_mutex_t *mutex) {  // 释放互斥锁
    free(mutex);
    mutex = NULL;
}

void qthread_mutex_lock(qthread_mutex_t *mutex) {  // 当前线程尝试获取互斥锁
    if (mutex->owner == NULL) {
        // 如果未被持有，当前线程获取锁
        mutex->owner = current_thread;
    } else {
        if (mutex->owner != current_thread) {
            // 如果不是当前线程持有，当前线程进入等待队列
            mutex->waiters.append(current_thread);
            current_thread->isWait = 1;
            // 让其它线程被调度
            qthread_yield();
        }
    }
}

void qthread_mutex_unlock(qthread_mutex_t *mutex) {
    if (mutex->owner != current_thread) {
        // 如果当前线程不是锁的持有者，不能解锁
        return;
    }
    // 解锁
    mutex->owner = NULL;
    // 是否有其它线程在等待这个锁
    if (!mutex->waiters.empty()) {
        // 唤醒等待队列中的线程，加入到就绪队列中
        while (!mutex->waiters.empty()) {
            qthread_t thread = mutex->waiters.pop();
            thread->isWait = 0;
            active_threads.append(thread);
        }
    }
}

qthread_cond_t *qthread_cond_create(void) {   //  创建条件变量
    qthread_cond_t *cond = (qthread_cond_t*)malloc(sizeof(qthread_cond_t));
    cond->waiters.head = 0;
    cond->waiters.tail = 0;
    return cond;
}

void qthread_cond_destroy(qthread_cond_t *cond) {  // 释放条件变量
    free(cond);
    cond = NULL;
}

void qthread_cond_wait(qthread_cond_t *cond, qthread_mutex_t *mutex) {

    // 将当前线程添加到条件变量的等待队列中
    cond->waiters.append(current_thread);
    current_thread->isWait = 1;

    // 条件不满足，进入该条件变量的等待队列中，同时释放互斥锁，以便其他线程可以进入临界区
    qthread_mutex_unlock(mutex);

    // 让出调度器
    qthread_yield();

    // 切换回来，重新获取锁
    qthread_mutex_lock(mutex);
}

void qthread_cond_signal(qthread_cond_t *cond) {   //  唤醒一个线程，加入到就绪队列中
    if (!cond->waiters.empty()) {
        qthread_t thread = cond->waiters.pop();
        thread->isWait = 0;
        active_threads.append(thread);
    }
}

void qthread_cond_broadcast(qthread_cond_t *cond) {   // 把所有等待该条件变量的线程加到就绪队列中
    while (!cond->waiters.empty()) {
        qthread_t thread = cond->waiters.pop();
        thread->isWait = 0;
        active_threads.append(thread);
    }
}

static long get_usecs(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec * 1000000 + tv.tv_usec;
}


/* POSIX replacement API. This semester we're only implementing 'usleep'
 *
 * If there are no runnable threads, your scheduler needs to wait, 
 * using one or more calls to the system usleep() function, until 
 * a thread blocked in 'qthread_usleep' is ready to wake up. 
 */


/* qthread_usleep - yield to next runnable thread, making arrangements
 * to be put back on the active list after 'usecs' timeout. 
 */
void qthread_usleep(long int usecs) {  // 线程休眠
    current_thread->sleep_time = get_usecs() + usecs; // 设置线程要休眠到什么时候
    for (int i = 0; i < MAX_SIZE; ++i)
    {
        if (sleep_threads[i] == NULL)
        {
            sleep_threads[i] = current_thread;  // 把要睡眠的线程放进睡眠数组里
            break;
        }
    }
    qthread_yield();  // 让出调度器
}