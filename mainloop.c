/*
 *
 *  BlueZ - Bluetooth protocol stack for Linux
 *
 *  Copyright (C) 2011-2014  Intel Corporation
 *  Copyright (C) 2002-2010  Marcel Holtmann <marcel@holtmann.org>
 *
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 *
 */

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <stdio.h>
#include <errno.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <sys/signalfd.h>
#include <sys/timerfd.h>
#include <sys/epoll.h>
#include <stdbool.h>
#include <pthread.h>

#include "src/shared/util.h"
#include "src/shared/queue.h"

#include "mainloop.h"

#define LOG_TAG "mainloop"

#define pr_info(fmt, ...) \
	printf("<%s>%s() "fmt"\n", LOG_TAG, __func__, ##__VA_ARGS__)
#define pr_err(fmt, ...)  \
	printf("<%s>%s() ERR: "fmt"\n", LOG_TAG, __func__, ##__VA_ARGS__)

#define MAX_EPOLL_EVENTS 10

struct mainloop_data {
	int fd;
	uint32_t events;
	mainloop_event_func callback;
	mainloop_destroy_func destroy;
	void *user_data;
	int epoll_fd;
};

struct timeout_data {
	int fd;
	mainloop_timeout_func callback;
	mainloop_destroy_func destroy;
	void *user_data;
};

struct signal_data {
	int fd;
	sigset_t mask;
	mainloop_signal_func callback;
	mainloop_destroy_func destroy;
	void *user_data;
};

struct thread_data {
	pthread_t tid;

	int epoll_fd;
	int epoll_terminate;
	int exit_status;

	struct queue *mainloop_list;

	/* per-thread struct, better not use signal
	 * with multi-threaded program */
	struct signal_data *signal_data;
};

static struct queue *threads_data;
static pthread_rwlock_t threads_lock = PTHREAD_RWLOCK_INITIALIZER;

static void signal_callback(int fd, uint32_t events, void *user_data);
static bool match_by_fd(const void *data, const void *user_data);
static void destroy_mainloop_data(void *user_data);

static bool match_tid(const void *data, const void *match_data)
{
	const pthread_t *tid = match_data;
	const struct thread_data *thread_data = data;

	return pthread_equal(thread_data->tid, *tid);
}

static struct thread_data *find_thread_data_by_tid(pthread_t tid)
{
	struct thread_data *thread_data;

	pthread_rwlock_rdlock(&threads_lock);
	thread_data = queue_find(threads_data, match_tid, &tid);
	pthread_rwlock_unlock(&threads_lock);

	return thread_data;
}

void mainloop_init(void)
{
	pthread_t tid;
	struct thread_data *tdata;

	pthread_rwlock_wrlock(&threads_lock);
	if (!threads_data)
		threads_data = queue_new();

	if (!threads_data) {
		pr_err("Couldn't alloc threads data");
		pthread_rwlock_unlock(&threads_lock);
		return;
	}
	pthread_rwlock_unlock(&threads_lock);

	tid = pthread_self();
	tdata = find_thread_data_by_tid(tid);
	if (tdata)
		return;
	tdata = malloc(sizeof(struct thread_data));

	memset(tdata, 0, sizeof(struct thread_data));
	tdata->tid = tid;
	tdata->epoll_fd = epoll_create1(EPOLL_CLOEXEC);
	tdata->mainloop_list = queue_new();
	if (!tdata->mainloop_list)
		pr_err("Couldn't alloc mainloop_list queue");
	tdata->epoll_terminate = 0;

	pthread_rwlock_wrlock(&threads_lock);
	queue_push_tail(threads_data, tdata);
	pthread_rwlock_unlock(&threads_lock);

	pr_info("tid (%lu)", tid);
}

void mainloop_quit(void)
{
	pthread_t tid;
	struct thread_data *tdata;

	tid = pthread_self();
	tdata = find_thread_data_by_tid(tid);
	if (!tdata) {
		pr_err("Quit, no corresponding thread data found");
		return;
	}

	tdata->epoll_terminate = 1;
	pr_info("tid (%lu)", tid);
}

void mainloop_exit_success(void)
{
	pthread_t tid;
	struct thread_data *tdata;

	tid = pthread_self();
	tdata = find_thread_data_by_tid(tid);
	if (!tdata) {
		pr_err("Exit, no corresponding thread data found");
		return;
	}

	tdata->exit_status = EXIT_SUCCESS;
	tdata->epoll_terminate = 1;
}

void mainloop_exit_failure(void)
{
	pthread_t tid;
	struct thread_data *tdata;

	tid = pthread_self();
	tdata = find_thread_data_by_tid(tid);
	if (!tdata) {
		pr_err("Exit failure, no corresponding thread data found");
		return;
	}

	tdata->exit_status = EXIT_FAILURE;
	tdata->epoll_terminate = 1;
}

int mainloop_run(void)
{
	struct signal_data *signal_data;
	pthread_t tid;
	struct thread_data *tdata;
	int return_status;

	tid = pthread_self();
	tdata = find_thread_data_by_tid(tid);
	if (!tdata) {
		pr_err("Run, couldn't find thread data, tid (%lu)", tid);
		return EXIT_FAILURE;
	}

	signal_data = tdata->signal_data;
	if (signal_data) {
		if (sigprocmask(SIG_BLOCK, &signal_data->mask, NULL) < 0)
			return EXIT_FAILURE;

		signal_data->fd = signalfd(-1, &signal_data->mask,
						SFD_NONBLOCK | SFD_CLOEXEC);
		if (signal_data->fd < 0)
			return EXIT_FAILURE;

		if (mainloop_add_fd(signal_data->fd, EPOLLIN,
				signal_callback, signal_data, NULL) < 0) {
			close(signal_data->fd);
			return EXIT_FAILURE;
		}
	}

	tdata->exit_status = EXIT_SUCCESS;

	while (!tdata->epoll_terminate) {
		struct epoll_event events[MAX_EPOLL_EVENTS];
		int n, nfds;

		nfds = epoll_wait(tdata->epoll_fd, events, MAX_EPOLL_EVENTS, -1);
		if (nfds < 0)
			continue;

		for (n = 0; n < nfds; n++) {
			struct mainloop_data *data = events[n].data.ptr;
			void *ptr;

			ptr = queue_find(tdata->mainloop_list, NULL, data);
			if (!ptr) {
				pr_err("Couldn't find fd in mainloop_list");
				continue;
			}

			data->callback(data->fd, events[n].events,
							data->user_data);
		}
	}

	pr_info("tid (%lu) exit of mainloop run", tid);

	if (signal_data) {
		mainloop_remove_fd(signal_data->fd);
		close(signal_data->fd);

		if (signal_data->destroy)
			signal_data->destroy(signal_data->user_data);
	}

	queue_destroy(tdata->mainloop_list, destroy_mainloop_data);
	tdata->mainloop_list = NULL;

	close(tdata->epoll_fd);
	tdata->epoll_fd = 0;

	pthread_rwlock_wrlock(&threads_lock);
	queue_remove(threads_data, tdata);
	pthread_rwlock_unlock(&threads_lock);
	return_status = tdata->exit_status;
	free(tdata);

	return return_status;
}

int mainloop_add_fd(int fd, uint32_t events, mainloop_event_func callback,
				void *user_data, mainloop_destroy_func destroy)
{
	struct mainloop_data *data;
	struct epoll_event ev;
	int err;

	pthread_t tid;
	struct thread_data *tdata;

	tid = pthread_self();

	pr_info("++, tid (%lu), fd %d", tid, fd);

	tdata = find_thread_data_by_tid(tid);
	if (!tdata) {
		pr_err("Couldn't find thread data, tid (%lu)", tid);
		return -EINVAL;
	}

	if (fd < 0 || !callback)
		return -EINVAL;

	data = malloc(sizeof(*data));
	if (!data)
		return -ENOMEM;

	memset(data, 0, sizeof(*data));
	data->fd = fd;
	data->events = events;
	data->callback = callback;
	data->destroy = destroy;
	data->user_data = user_data;
	data->epoll_fd = tdata->epoll_fd;

	memset(&ev, 0, sizeof(ev));
	ev.events = events;
	ev.data.ptr = data;

	err = epoll_ctl(tdata->epoll_fd, EPOLL_CTL_ADD, data->fd, &ev);
	if (err < 0) {
		free(data);
		return err;
	}

	if (!queue_push_tail(tdata->mainloop_list, data)) {
		pr_err("Couldn't add data to mainloop_list");
		epoll_ctl(tdata->epoll_fd, EPOLL_CTL_DEL, data->fd, NULL);
		free(data);
		return -1;
	}
	pr_info("--");

	return 0;
}

int mainloop_modify_fd(int fd, uint32_t events)
{
	struct mainloop_data *data;
	struct epoll_event ev;
	int err;

	pthread_t tid;
	struct thread_data *tdata;

	tid = pthread_self();
	tdata = find_thread_data_by_tid(tid);
	if (!tdata) {
		pr_err("No corresponding thread data, tid (%lu)", tid);
		return -EINVAL;
	}

	if (fd < 0)
		return -EINVAL;

	data = queue_find(tdata->mainloop_list, match_by_fd, INT_TO_PTR(fd));
	if (!data) {
		pr_err("Couldn't find mainloop data");
		return -ENXIO;
	}

	memset(&ev, 0, sizeof(ev));
	ev.events = events;
	ev.data.ptr = data;

	err = epoll_ctl(tdata->epoll_fd, EPOLL_CTL_MOD, data->fd, &ev);
	if (err < 0)
		return err;

	data->events = events;

	return 0;
}

int mainloop_remove_fd(int fd)
{
	struct mainloop_data *data;
	int err;

	pthread_t tid;
	struct thread_data *tdata;

	tid = pthread_self();

	pr_info("++, tid (%lu), fd %d", tid, fd);

	tdata = find_thread_data_by_tid(tid);
	if (!tdata)
		return -EINVAL;

	if (fd < 0)
		return -EINVAL;

	data = queue_find(tdata->mainloop_list, match_by_fd, INT_TO_PTR(fd));
	if (!data)
		return -ENXIO;

	queue_remove(tdata->mainloop_list, data);

	err = epoll_ctl(tdata->epoll_fd, EPOLL_CTL_DEL, data->fd, NULL);

	if (data->destroy)
		data->destroy(data->user_data);

	free(data);

	pr_info("--");

	return err;
}

static void timeout_destroy(void *user_data)
{
	struct timeout_data *data = user_data;

	close(data->fd);
	data->fd = -1;

	if (data->destroy)
		data->destroy(data->user_data);

	free(data);
}

static void timeout_callback(int fd, uint32_t events, void *user_data)
{
	struct timeout_data *data = user_data;
	uint64_t expired;
	ssize_t result;

	if (events & (EPOLLERR | EPOLLHUP))
		return;

	result = read(data->fd, &expired, sizeof(expired));
	if (result != sizeof(expired))
		return;

	if (data->callback)
		data->callback(data->fd, data->user_data);
}

static inline int timeout_set(int fd, unsigned int msec)
{
	struct itimerspec itimer;
	unsigned int sec = msec / 1000;

	memset(&itimer, 0, sizeof(itimer));
	itimer.it_interval.tv_sec = 0;
	itimer.it_interval.tv_nsec = 0;
	itimer.it_value.tv_sec = sec;
	itimer.it_value.tv_nsec = (msec - (sec * 1000)) * 1000 * 1000;

	return timerfd_settime(fd, 0, &itimer, NULL);
}

int mainloop_add_timeout(unsigned int msec, mainloop_timeout_func callback,
				void *user_data, mainloop_destroy_func destroy)
{
	struct timeout_data *data;

	if (!callback)
		return -EINVAL;

	data = malloc(sizeof(*data));
	if (!data)
		return -ENOMEM;

	memset(data, 0, sizeof(*data));
	data->callback = callback;
	data->destroy = destroy;
	data->user_data = user_data;

	data->fd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
	if (data->fd < 0) {
		free(data);
		return -EIO;
	}

	if (msec > 0) {
		if (timeout_set(data->fd, msec) < 0) {
			close(data->fd);
			free(data);
			return -EIO;
		}
	}

	pr_info("timerfd %d", data->fd);

	if (mainloop_add_fd(data->fd, EPOLLIN | EPOLLONESHOT,
				timeout_callback, data, timeout_destroy) < 0) {
		close(data->fd);
		free(data);
		return -EIO;
	}

	return data->fd;
}

int mainloop_modify_timeout(int id, unsigned int msec)
{
	if (msec > 0) {
		if (timeout_set(id, msec) < 0)
			return -EIO;
	}

	if (mainloop_modify_fd(id, EPOLLIN | EPOLLONESHOT) < 0)
		return -EIO;

	return 0;
}

int mainloop_remove_timeout(int id)
{
	return mainloop_remove_fd(id);
}

static void signal_callback(int fd, uint32_t events, void *user_data)
{
	struct signal_data *data = user_data;
	struct signalfd_siginfo si;
	ssize_t result;

	if (events & (EPOLLERR | EPOLLHUP)) {
		mainloop_quit();
		return;
	}

	result = read(fd, &si, sizeof(si));
	if (result != sizeof(si))
		return;

	if (data->callback)
		data->callback(si.ssi_signo, data->user_data);
}

static bool match_by_fd(const void *data, const void *user_data)
{
	const struct mainloop_data *md = data;
	int fd = PTR_TO_INT(user_data);

	return md->fd == fd;
}

static void destroy_mainloop_data(void *user_data)
{
	struct mainloop_data *data = user_data;

	pr_info("tid (%lu), fd %d", pthread_self(), data->fd);

	epoll_ctl(data->epoll_fd, EPOLL_CTL_DEL, data->fd, NULL);

	if (data->destroy)
		data->destroy(data->user_data);

	free(data);
}

int mainloop_set_signal(sigset_t *mask, mainloop_signal_func callback,
				void *user_data, mainloop_destroy_func destroy)
{
	struct signal_data *data;

	pthread_t tid;
	struct thread_data *tdata;

	tid = pthread_self();
	tdata = find_thread_data_by_tid(tid);
	if (!tdata) {
		pr_err("signal: No corresponding thread data, tid (%lu)", tid);
		return -EINVAL;
	}

	if (!mask || !callback)
		return -EINVAL;

	data = malloc(sizeof(*data));
	if (!data)
		return -ENOMEM;

	memset(data, 0, sizeof(*data));
	data->callback = callback;
	data->destroy = destroy;
	data->user_data = user_data;

	data->fd = -1;
	memcpy(&data->mask, mask, sizeof(sigset_t));

	free(tdata->signal_data);
	tdata->signal_data = data;

	return 0;
}
