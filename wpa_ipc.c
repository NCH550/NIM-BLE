/*
 *  Copyright (C) 2018 Realtek Semiconductor Corp.
 *  Copyright (c) 2004-2007, Jouni Malinen <j@w1.fi>
 *
 * This software may be distributed under the terms of the BSD license.
 */
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <errno.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <poll.h>
#include <stdlib.h>
#include <time.h>
#include <ctype.h>

#include "wpa_ipc.h"

#define CONFIG_CTRL_IFACE_CLIENT_DIR "/tmp"

typedef long os_time_t;

struct os_reltime {
	os_time_t sec;
	os_time_t usec;
};

int os_get_reltime(struct os_reltime *t)
{
	static clockid_t clock_id = CLOCK_MONOTONIC;
	struct timespec ts;
	int res;

	res = clock_gettime(clock_id, &ts);
	if (res == 0) {
		t->sec = ts.tv_sec;
		t->usec = ts.tv_nsec / 1000;
		return 0;
	}
}

static inline void os_reltime_sub(struct os_reltime *a, struct os_reltime *b,
				  struct os_reltime *res)
{
	res->sec = a->sec - b->sec;
	res->usec = a->usec - b->usec;
	if (res->usec < 0) {
		res->sec--;
		res->usec += 1000000;
	}
}

static inline int os_reltime_expired(struct os_reltime *now,
				     struct os_reltime *ts,
				     os_time_t timeout_secs)
{
	struct os_reltime age;

	os_reltime_sub(now, ts, &age);
	return (age.sec > timeout_secs) ||
	       (age.sec == timeout_secs && age.usec > 0);
}

struct wpa_ctrl *wpa_ctrl_open2(const char *ctrl_path)
{
	struct wpa_ctrl *ctrl;
	static int counter = 0;
	int ret;
	size_t res;
	int tries = 0;
	int flags;

	if (!ctrl_path)
		return NULL;

	ctrl = malloc(sizeof(*ctrl));
	if (!ctrl)
		return NULL;
	memset(ctrl, 0, sizeof(*ctrl));

	ctrl->s = socket(PF_UNIX, SOCK_DGRAM, 0);
	if (ctrl->s < 0) {
		free(ctrl);
		return NULL;
	}

	ctrl->local.sun_family = AF_UNIX;
	counter++;
try_again:
	ret = snprintf(ctrl->local.sun_path,
		       sizeof(ctrl->local.sun_path),
		       CONFIG_CTRL_IFACE_CLIENT_DIR "/" "wpa_ctrl_%d-%d",
		       (int) getpid(), counter);

	if (ret <= 0) {
		close(ctrl->s);
		free(ctrl);
		return NULL;
	}
	tries++;
	if (bind(ctrl->s, (struct sockaddr *) &ctrl->local,
		    sizeof(ctrl->local)) < 0) {
		if (errno == EADDRINUSE && tries < 2) {
			/*
			 * getpid() returns unique identifier for this instance
			 * of wpa_ctrl, so the existing socket file must have
			 * been left by unclean termination of an earlier run.
			 * Remove the file and try again.
			 */
			unlink(ctrl->local.sun_path);
			goto try_again;
		}
		close(ctrl->s);
		free(ctrl);
		return NULL;
	}

	ctrl->dest.sun_family = AF_UNIX;
	strncpy(ctrl->dest.sun_path, ctrl_path, sizeof(ctrl->dest.sun_path));

	if (connect(ctrl->s, (struct sockaddr *) &ctrl->dest,
		    sizeof(ctrl->dest)) < 0) {
		close(ctrl->s);
		unlink(ctrl->local.sun_path);
		free(ctrl);
		return NULL;
	}

	/*
	 * Make socket non-blocking so that we don't hang forever if
	 * target dies unexpectedly.
	 */
	flags = fcntl(ctrl->s, F_GETFL);
	if (flags >= 0) {
		flags |= O_NONBLOCK;
		if (fcntl(ctrl->s, F_SETFL, flags) < 0) {
			perror("fcntl(ctrl->s, O_NONBLOCK)");
			/* Not fatal, continue on.*/
		}
	}

	return ctrl;
}

int wpa_ctrl_request(struct wpa_ctrl *ctrl, const char *cmd, size_t cmd_len,
		     char *reply, size_t *reply_len,
		     void (*msg_cb)(char *msg, size_t len))
{
	struct timeval tv;
	struct os_reltime started_at;
	int res;
	fd_set rfds;
	const char *_cmd;
	char *cmd_buf = NULL;
	size_t _cmd_len;

	{
		_cmd = cmd;
		_cmd_len = cmd_len;
	}

	errno = 0;
	started_at.sec = 0;
	started_at.usec = 0;
retry_send:
	if (send(ctrl->s, _cmd, _cmd_len, 0) < 0) {
		if (errno == EAGAIN || errno == EBUSY || errno == EWOULDBLOCK)
		{
			/*
			 * Must be a non-blocking socket... Try for a bit
			 * longer before giving up.
			 */
			if (started_at.sec == 0)
				os_get_reltime(&started_at);
			else {
				struct os_reltime n;
				os_get_reltime(&n);
				/* Try for a few seconds. */
				if (os_reltime_expired(&n, &started_at, 5))
					goto send_err;
			}
			sleep(1);
			goto retry_send;
		}
	send_err:
		free(cmd_buf);
		return -1;
	}
	free(cmd_buf);

	for (;;) {
		tv.tv_sec = 10;
		tv.tv_usec = 0;
		FD_ZERO(&rfds);
		FD_SET(ctrl->s, &rfds);
		res = select(ctrl->s + 1, &rfds, NULL, NULL, &tv);
		if (res < 0)
			return res;
		if (FD_ISSET(ctrl->s, &rfds)) {
			res = recv(ctrl->s, reply, *reply_len, 0);
			if (res < 0)
				return res;
			if (res > 0 && reply[0] == '<') {
				/* This is an unsolicited message from
				 * wpa_supplicant, not the reply to the
				 * request. Use msg_cb to report this to the
				 * caller. */
				if (msg_cb) {
					/* Make sure the message is nul
					 * terminated. */
					if ((size_t) res == *reply_len)
						res = (*reply_len) - 1;
					reply[res] = '\0';
					msg_cb(reply, res);
				}
				continue;
			}
			*reply_len = res;
			break;
		} else {
			return -2;
		}
	}
	return 0;
}

void wpa_ctrl_close(struct wpa_ctrl *ctrl)
{
	if (ctrl == NULL)
		return;
	printf("Remove sock %s\n", ctrl->local.sun_path);
	unlink(ctrl->local.sun_path);
	if (ctrl->s >= 0)
		close(ctrl->s);
	free(ctrl);
}

static void wpa_cli_msg_cb(char *msg, size_t len)
{
	printf("%s: %s\n", msg, __func__);
}


static int wpa_ctrl_command(struct wpa_ctrl *ctrl, char *cmd,
			    char *buf, size_t *length)
{
	size_t len;
	int ret;

	if (!ctrl) {
		printf("Not connected to wpa_supplicant - command dropped.\n");
		return -1;
	}
	len = *length;
	ret = wpa_ctrl_request(ctrl, cmd, strlen(cmd), buf, &len,
			       wpa_cli_msg_cb);
	if (ret == -2) {
		printf("'%s' command timed out.\n", cmd);
		return -2;
	} else if (ret < 0) {
		printf("'%s' command failed.\n", cmd);
		return -1;
	}

	buf[len] = '\0';
	*length = len;

	return 0;
}

static int operation_result(char *buf)
{
	char *tbuf = buf;
	char *t;

	/* string: OK or ... */
	t = strsep(&tbuf, "\n");
	if (!t)
		return -1;

	if (t[0] == 'O' && t[1] == 'K'){
		printf("result is OK\n");
		return 0;
	} else {
		printf("result: |%s|\n", t);
		return -1;
	}
}

static uint8_t ch_to_index(int channel)
{
	if (channel < 2412 || channel > 2484)
		return 0;

	if (channel == 2484)
		return 14;
	else
		return ((channel - 2412)/5 + 1);
}

int wpa_ctrl_command2(struct wpa_ctrl *ctrl, char *cmd)
{
	char buf[128];
	int result;
	size_t len;

	len = sizeof(buf);
	strcpy(buf, "FAIL");
	result = wpa_ctrl_command(ctrl, cmd, buf, &len);
	if (result) {
		fprintf(stderr, "%s execution failed, %d\n", cmd, result);
		return -1;
	}
	/* printf("%s", buf); */
	if (!strncmp(buf, "OK", 2))
		return 0;
	else
		return -1;
}

int scan_on(struct wpa_ctrl *ctrl)
{
	char cmd[16];

	sprintf(cmd, "SCAN");
	return wpa_ctrl_command2(ctrl, cmd);
}

int scan_results(struct wpa_ctrl *ctrl, struct ap_info *ap_info, int count)
{
	char cmd[64];
	char buf[4096];
	char *buf2 = buf;
	int result;
	size_t len;
	char *t;
	int i;
	int j;

	if (!ctrl) {
		printf("%s: Not connected to wpa_supplicant.\n", __func__);
		return -1;
	}

	if (!ap_info)
		return -1;

	sprintf(cmd, "SCAN_RESULTS");
	len = sizeof(buf);
	strcpy(buf, "FAIL");
	result = wpa_ctrl_command(ctrl, cmd, buf, &len);
	if (result) {
		fprintf(stderr, "command execution failed, %d\n", result);
		return -1;
	}

	/* string: bssid / frequency / signal level / flags / ssid */
	t = strsep(&buf2, "\n");
	if (!t)
		return -1;

	i = 0;
	j = 0;
	while ((t = strsep(&buf2, "\n")) != NULL) {
		if (!t[0])
			continue;
		char *tptr = t;
		char *p;
		int len = strlen(tptr);

		if (i + 1 > count) {
			printf("ap_info is full, %d\n", i);
			break;
		}

		/* bssid */
		t = strsep(&tptr, " \t");
		if (t) {
			for (j = 5; j >= 0; j--, t += 3)
				ap_info[i].mac[j] = strtol(t, NULL, 16);
		} else {
			printf("No MAC info, index %d\n", i);
			break;
		}

		/* frequency */
		t = strsep(&tptr, " \t");
		if (t) {
			ap_info[i].channel = ch_to_index(strtoul(t, NULL, 10));
		} else {
			printf("No Channel info, index %d\n", i);
			break;
		}

		/* signal level (RSSI) */
		t = strsep(&tptr, " \t");
		if (t) {
			ap_info[i].rssi = (int8_t)strtol(t, NULL, 10);
		} else {
			printf("No RSSI info, index %d\n", i);
			break;
		}

		/* flags (Encryption) */
		t = strsep(&tptr, " \t");
		if (t) {
			p = strstr(t, "WPA");
			if (p)
				ap_info[i].encrypt = 1;
			else
				ap_info[i].encrypt = 0; /* TODO: 2 for WEP */
		} else {
			printf("No Encryption info, index %d\n", i);
			break;
		}

		/* ssid */
		t = strsep(&tptr, " \t");
		if (t) {
			j = sizeof(ap_info[i].ssid);
			if (strlen(t) + 1 > sizeof(ap_info[i].ssid)) {
				memcpy(ap_info[i].ssid, t, j);
				ap_info[i].ssid[j - 1] = 0;
			} else {
				int l;

				l = strlen(t);
				memcpy(ap_info[i].ssid, t, l);
				ap_info[i].ssid[l] = 0;
				j -= l + 1;
				if (tptr != NULL) {
					ap_info[i].ssid[l] = ' ';
					strncpy(&ap_info[i].ssid[l + 1], tptr, j);
					ap_info[i].ssid[sizeof(ap_info[i].ssid) - 1] = 0;
				}
			}
		} else {
			printf("No SSID info, index %d\n", i);
		}
		i++;
	}

	return i;
}

int connect_to(struct wpa_ctrl *ctrl, struct conn_req *req)
{
	int result;
	char cmd[64];
	char buf[64];
	size_t len;
	int network_number = -1;
	char *keymgmts[] = {
		"NONE",
		"WPA-PSK",
		"NONE", /* FIXME: This is WEP */
	};

	if (!ctrl) {
		printf("%s: Not connected to wpa_supplicant.\n", __func__);
		return -1;
	}

	if (!req)
		return -1;

	sprintf(cmd, "REMOVE_NETWORK 0");
	result = wpa_ctrl_command2(ctrl, cmd);
	if (result)
		fprintf(stderr, "%s execution failed, %d\n", cmd, result);

	sprintf(cmd, "ADD_NETWORK");
	len = sizeof(buf);
	strcpy(buf, "FAIL");
	result = wpa_ctrl_command(ctrl, cmd, buf, &len);
	if (result) {
		fprintf(stderr, "%s execution failed, %d\n", cmd, result);
		return -1;
	}
	if (len > 0 && isdigit(buf[0]))
		network_number = strtol(buf, NULL, 10);
	if (network_number == -1)
		return -1;

	sprintf(cmd, "SET_NETWORK %d ssid \"%s\"", network_number, req->ssid);
	result = wpa_ctrl_command2(ctrl, cmd);
	if (result) {
		fprintf(stderr, "%s execution failed, %d\n", cmd, result);
		return -1;
	}

	if (req->encrypt > 2) {
		fprintf(stderr, "Encrypt %u is wrong, reset it to 0\n",
			req->encrypt);
		req->encrypt = 0;
	}
	sprintf(cmd, "SET_NETWORK %d key_mgmt %s", network_number,
		keymgmts[req->encrypt]);
	result = wpa_ctrl_command2(ctrl, cmd);
	if (result) {
		fprintf(stderr, "%s execution failed, %d\n", cmd, result);
		return -1;
	}

	sprintf(cmd, "SET_NETWORK %d psk \"%s\"", network_number,
		req->password);
	result = wpa_ctrl_command2(ctrl, cmd);
	if (result) {
		fprintf(stderr, "%s execution failed, %d\n", cmd, result);
		return -1;
	}

	sprintf(cmd, "SET_NETWORK %d proto RSN", network_number);
	result = wpa_ctrl_command2(ctrl, cmd);
	if (result) {
		fprintf(stderr, "%s execution failed, %d\n", cmd, result);
		return -1;
	}

	sprintf(buf, "%2.2x:%2.2x:%2.2x:%2.2x:%2.2x:%2.2x",
		req->mac[5], req->mac[4], req->mac[3],
		req->mac[2], req->mac[1], req->mac[0]);
	sprintf(cmd, "SET_NETWORK %d bssid %s", network_number, buf);
	result = wpa_ctrl_command2(ctrl, cmd);
	if (result) {
		fprintf(stderr, "%s execution failed, %d\n", cmd, result);
		return -1;
	}

	sprintf(cmd, "ENABLE_NETWORK %d", network_number);
	result = wpa_ctrl_command2(ctrl, cmd);
	if (result) {
		fprintf(stderr, "%s execution failed, %d\n", cmd, result);
		return -1;
	}

	sprintf(cmd, "SELECT_NETWORK %d", network_number);
	result = wpa_ctrl_command2(ctrl, cmd);
	if (result) {
		fprintf(stderr, "%s execution failed, %d\n", cmd, result);
		return -1;
	}

	return 0;
}

int connection_status(struct wpa_ctrl *ctrl, struct conn_status *status)
{
	char cmd[64];
	char buf[4096];
	int result;
	size_t len;
	int i = 0;
	struct key_val {
		char *key;
		char *val;
	} key_vals[16];
	int num;
	char *t;
	char *tbuf;
	char *ptr;

	if (!ctrl) {
		printf("%s: Not connected to wpa_supplicant.\n", __func__);
		return -1;
	}

	if (!status)
		return -1;

	sprintf(cmd, "STATUS");
	len = sizeof(buf);
	strcpy(buf, "FAIL");
	result = wpa_ctrl_command(ctrl, cmd, buf, &len);
	if (result) {
		fprintf(stderr, "command execution failed, %d\n", result);
		return -1;
	}

	if (!strncmp(buf, "FAIL", 4)) {
		fprintf(stderr, "Not status message\n");
		return -1;
	}

	tbuf = buf;
	i = 0;
	while ((t = strsep(&tbuf, "\n")) != NULL) {
		ptr = strsep(&t, "=");
		if (ptr && t) {
			key_vals[i].key = ptr;
			key_vals[i].val = t;
			i++;
		}
	}
	num = i;

	/* Network not found:
	 * ------
	 * > status
	 * wpa_state=SCANNING (or ...)
	 * address=4c:ed:de:24:0d:2e
	 * ------
	 * Network found:
	 * ------
	 * > status
	 * bssid=82:2a:a8:97:58:73
	 * freq=2462
	 * ssid=RealKungFu-2.4G
	 * id=1
	 * mode=station
	 * pairwise_cipher=CCMP
	 * group_cipher=CCMP
	 * key_mgmt=WPA2-PSK
	 * wpa_state=COMPLETED
	 * address=4c:ed:de:24:0d:2e
	 * ------
	 */
	i = 0;
	while (i < num) {
		/* See wpa_supplicant_state_txt() in wpa_supplicant */
		if (!strcmp(key_vals[i].key, "wpa_state")) {
			if (!strcmp(key_vals[i].val, "COMPLETED")) {
				break;
			} else {
				printf("Connection not completed, %s=%s\n",
				       key_vals[i].key,
				       key_vals[i].val);
				status->status = 1;
				return 0;
			}
		}
		i++;
	}

	if (i >= num) {
		status->status = 1;
		fprintf(stderr, "wpa_state not found\n");
		return -1;
	}

	status->status = 0;

	i = strlen(key_vals[2].val) + 1;
	memset(status->ssid, 0, sizeof(status->ssid));
	len = sizeof(status->ssid) > i ? i : sizeof(status->ssid);
	strncpy(status->ssid, key_vals[2].val, len);
	status->ssid[len - 1] = 0;

	if (!strncmp(key_vals[7].val, "NONE", 4))
		status->encrypt = 0; /* FIXME: 2 for WEP */
	else
		status->encrypt = 1;

	t = key_vals[0].val;
	for (i = 5; i >= 0; i--, t += 3)
		status->mac[i] = strtol(t, NULL, 16);

	/* TODO: How to get rssi */
	/* Look up the ap info according to bssid */
}

