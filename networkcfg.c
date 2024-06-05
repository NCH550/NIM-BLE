/*
 *  BlueZ - Bluetooth protocol stack for Linux
 *
 *  Copyright (C) 2018 Realtek Semiconductor Corp. All rights reserved.
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 */

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#include <stdio.h>
#include <stdbool.h>
#include <stdint.h>
#include <time.h>
#include <stdlib.h>
#include <getopt.h>
#include <unistd.h>
#include <errno.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <poll.h>
#include <fcntl.h>

#include "lib/bluetooth.h"
#include "lib/hci.h"
#include "lib/hci_lib.h"
#include "lib/l2cap.h"
#include "lib/uuid.h"

#include "src/shared/mainloop.h"
#include "src/shared/util.h"
#include "src/shared/att.h"
#include "src/shared/queue.h"
#include "src/shared/timeout.h"
#include "src/shared/gatt-db.h"
#include "src/shared/gatt-server.h"

#define THREAD_DEMO
//#define WPA_SOCK

#ifdef WPA_SOCK
#include "wpa_ipc.h"
#define CTRL_PATH	"/var/run/wpa_supplicant"
#define CTRL_FILE	"wlp2s0b1"
#endif

#define BT_ATT_OP_HANDLE_VAL_NOT	BT_ATT_OP_HANDLE_NFY

#define UUID_GAP			0x1800
#define UUID_GATT			0x1801

#define ATT_CID 4

#define PRLOG(...) \
	do { \
		printf(__VA_ARGS__); \
		print_prompt(); \
	} while (0)

#ifndef MIN
#define MIN(a, b) ((a) < (b) ? (a) : (b))
#endif

#define COLOR_OFF	"\x1B[0m"
#define COLOR_RED	"\x1B[0;91m"
#define COLOR_GREEN	"\x1B[0;92m"
#define COLOR_YELLOW	"\x1B[0;93m"
#define COLOR_BLUE	"\x1B[0;94m"
#define COLOR_MAGENTA	"\x1B[0;95m"
#define COLOR_BOLDGRAY	"\x1B[1;30m"
#define COLOR_BOLDWHITE	"\x1B[1;37m"

#define NETCFG_SCAN		0x0101
#define NETCFG_CONNECT		0x0102
#define NETCFG_GET_CONNSTATUS	0x0103
#define NETCFG_CANCEL_CONN	0x0104
#define NETCFG_VERSION		0x0105
#define COMM_HEADER	4
#define ATT_NOTIF		0x00
#define ATT_INDIC		0x01

/* static const char test_device_name[] = "Very Long Test Device Name For Testing "
 * 				"ATT Protocol Operations On GATT Server";
 */
static const char test_device_name[] = "WLAN Peripheral";
static bool verbose = false;
static uint16_t att_mtu = 517;

struct server {
	int fd;
	struct bt_att *att;
	struct gatt_db *db;
	struct bt_gatt_server *gatt;

	uint8_t *device_name;
	size_t name_len;

	uint16_t gatt_svc_chngd_handle;
	bool svc_chngd_enabled;

	/* demo characteristics */
	uint16_t value_handle;
	bool enabled;			/* notification flag */
	unsigned int timeout_id;
};

static pthread_mutex_t tx_q_mutex = PTHREAD_MUTEX_INITIALIZER;
/* This queue is used to transmit data from outer thread to
 * mainloop, then mainloop invokes inner function to send the data
 * to remote.
 */
static struct queue *tx_queue;
static int sock_fds[2];
#ifdef THREAD_DEMO
static pthread_t thread_id;
static int thread_running;
int send_enabled;
static pthread_mutex_t rx_q_mutex = PTHREAD_MUTEX_INITIALIZER;
static struct queue *rx_queue;
#endif

#ifdef WPA_SOCK
static struct ap_info ap_info[32];
static int ap_count;
struct wpa_ctrl *ctrl;
struct conn_status conn_sta;
static void netcfg_get_connstatus(int type);
#endif

static struct server *gatt_server;
static void server_destroy(struct server *server);

static void print_prompt(void)
{
	printf(COLOR_BLUE "[GATT server]" COLOR_OFF "# ");
	fflush(stdout);
}

static inline uint16_t __get_unaligned_le16(const uint8_t *p)
{
	return p[0] | p[1] << 8;
}

static inline void __put_unaligned_le16(uint16_t val, uint8_t *p)
{
	*p++ = val;
	*p++ = val >> 8;
}

static inline uint32_t __get_unaligned_le32(const uint8_t *p)
{
	return p[0] | p[1] << 8 | p[2] << 16 | p[3] << 24;
}

static inline void __put_unaligned_le32(uint32_t val, uint8_t *p)
{
	*p++ = val;
	*p++ = val >> 8;
	*p++ = val >> 16;
	*p++ = val >> 24;
}

static int sock_transmit(uint8_t type, uint8_t *data, uint16_t len)
{
	uint8_t *p;
	ssize_t result;
	uint8_t byte = 1;

	p = malloc(COMM_HEADER + len);
	if (!p) {
		fprintf(stderr, "Failed to alloc buffer for transmission\n");
		return -1;
	}

	p[0] = type;
	p[1] = 0;
	__put_unaligned_le16(len, p + 2);
	memcpy(p + COMM_HEADER, data, len);

	pthread_mutex_lock(&tx_q_mutex);
	result = queue_push_tail(tx_queue, p);
	pthread_mutex_unlock(&tx_q_mutex);
	if (!result) {
		fprintf(stderr, "Queue push error\n");
		return -1;
	}

	/* printf("Wake up mainloop\n"); */

	/* Wake up mainloop */
	result = write(sock_fds[1], &byte, 1);
	if (result <= 0)
		fprintf(stderr, "Write error, %s\n", strerror(errno));

	return result;
}

static void command_result(uint8_t status)
{
	uint8_t buf[6];
	int result;

	buf[0] = 0xaa;
	buf[1] = 0x02;
	buf[2] = 0x01;
	__put_unaligned_le16(0x0001, buf + 3);
	buf[5] = status;

	result = sock_transmit(ATT_NOTIF, buf, sizeof(buf));
	if (result <= 0)
		fprintf(stderr, "Transmit netcfg command status error\n");
}

#ifdef WPA_SOCK
#endif

static void netcfg_version(void)
{
	uint8_t buf[] = { 0xaa, 0x02, 0x05, 0x00, 0x01, 0x01 };
	int result;

	result = sock_transmit(ATT_NOTIF, buf, sizeof(buf));
	if (result <= 0)
		fprintf(stderr, "Transmit netcfg version error\n");
}

static void netcfg_scan(void)
{
#ifdef WPA_SOCK
	int result;
	int i;
	uint8_t buf[5 + sizeof(ap_info[0])] = { 0xaa, 0x02, 0x04, 0x00, 0x00 };

	result = scan_on(ctrl);
	if (!result)
		printf("Scan on successfully\n");
	else
		command_result(0xff);

	command_result(0x00);

	sleep(2); /* Wait for the operation done */

	result = scan_results(ctrl, ap_info, ARRAY_SIZE(ap_info));
	if (result > 0)
		ap_count = result;
	buf[2] = 0x02;
	__put_unaligned_le16(sizeof(ap_info[0]), &buf[3]);
	for (i = 0; i < ap_count; i++) {
		/* uint8_t tbuf[18];
		 * PRLOG("SSID: %s\n", ap_info[i].ssid);
		 * sprintf(tbuf, "%02x:%02x:%02x:%02x:%02x:%02x",
		 * 	ap_info[i].mac[5], ap_info[i].mac[4], ap_info[i].mac[3],
		 * 	ap_info[i].mac[2], ap_info[i].mac[1], ap_info[i].mac[0]);
		 * PRLOG("MAC: %s\n", tbuf);
		 */
		memcpy(&buf[5], &ap_info[i], sizeof(ap_info[0]));
		result = sock_transmit(ATT_NOTIF, buf,
				       sizeof(ap_info[0]) + 5);
		if (result <= 0) {
			fprintf(stderr, "Transmit %dst ap info error\n", i);
			return;
		}
	}

	/* Send scan complete event */
	result = sock_transmit(ATT_NOTIF, buf, 5);
	if (result <= 0) {
		fprintf(stderr, "Transmit scan complete event error\n");
		return;
	}

	netcfg_get_connstatus(NETCFG_SCAN);
#else
	command_result(0xff);
#endif
}

const char *encrypt_str[] = {
	"NONE", "WPA", "WEP",
};
static void netcfg_connect(uint8_t *data, uint16_t len)
{
#ifdef WPA_SOCK
	struct conn_req *req = (void *)data;
	int result;
	char buf[32];
	struct conn_status sta;

	PRLOG("The connecting AP information:\n");
	PRLOG("Band: %s\n", req->band ? "5G" : "2G");
	if (req->encrypt > 2)
		req->encrypt = 0;
	PRLOG("Encrypt: %s\n", encrypt_str[req->encrypt]);
	PRLOG("SSID: %s\n", req->ssid);
	sprintf(buf, "%02x:%02x:%02x:%02x:%02x:%02x",
		req->mac[5], req->mac[4], req->mac[3],
		req->mac[2], req->mac[1], req->mac[0]);
	PRLOG("AP MAC: %s\n", buf);
	PRLOG("Password: %s\n", req->password);

	conn_sta.encrypt = req->encrypt;
	memcpy(conn_sta.ssid, req->ssid, sizeof(conn_sta.ssid));
	memcpy(conn_sta.mac, req->mac, sizeof(conn_sta.mac));

	result = connect_to(ctrl, req);
	if (result < 0) {
		fprintf(stderr, "Connection setup failed\n");
		return;
	}
	sleep(2); /* Wait for the connection complete */
	command_result(0x00);

	sta.status = 0x01;
	connection_status(ctrl, &sta);
	if (sta.status == 0x00) {
		sprintf(buf, "dhcpcd %s", CTRL_FILE);
		result = system(buf);
	}

	netcfg_get_connstatus(NETCFG_CONNECT);
#else
	command_result(0xff);
#endif
}

static void netcfg_get_connstatus(int type)
{
#ifdef WPA_SOCK
	int j;
	int result;
	struct conn_status *status = &conn_sta;

	uint8_t buf[5 + sizeof(*status)] = { 0xaa, 0x02, 0x03, };

	status->status = 0x01;
	connection_status(ctrl, status);
	if (status->status != 0x00) {
		switch (type) {
		case NETCFG_CONNECT:
			status->status = 0x01;
			break;
		case NETCFG_SCAN:
			status->status = 0x01;
			break;
		default:
			status->status = 0xff;
			break;
		}
		goto send_step;
	}
	status->rssi = -1;
	PRLOG("status: |SSID: %s|\n", status->ssid);
	PRLOG("mac(bssid): %02x:%02x:%02x:%02x:%02x:%02x\n",
	      status->mac[5], status->mac[4], status->mac[3],
	      status->mac[2], status->mac[1], status->mac[0]);

	for (j = 0; j < ap_count; j++) {
		if (!memcmp(status->mac, ap_info[j].mac, sizeof(status->mac))) {
			PRLOG("AP %d:\n", j);
			PRLOG("ssid: |%s|\n", ap_info[j].ssid);
			PRLOG("channel: %u\n", ap_info[j].channel);
			PRLOG("encrypt: %u\n", ap_info[j].encrypt);
			PRLOG("rssi: %d\n", ap_info[j].rssi);
			PRLOG("bssid: %2.2x:%2.2x:%2.2x:%2.2x:%2.2x:%2.2x\n",
			      ap_info[j].mac[5], ap_info[j].mac[4],
			      ap_info[j].mac[3], ap_info[j].mac[2],
			      ap_info[j].mac[1], ap_info[j].mac[0]);
			status->rssi = ap_info[j].rssi;
			break;
		}
	}

send_step:
	__put_unaligned_le16(sizeof(*status), &buf[3]);
	memcpy(&buf[5], status, sizeof(*status));
	result = sock_transmit(ATT_NOTIF, buf, 5 + sizeof(*status));
	if (result <= 0)
		fprintf(stderr, "Transmit conn status error\n");
#else
	fprintf(stderr, "Get connection status cmd not implemented\n");
#endif
}

static void netcfg_cancel_conn(void)
{
#ifdef WPA_SOCK
	int result;

	result = wpa_ctrl_command2(ctrl, "DISCONNECT");
	if (result)
		command_result(0xff);
	else
		command_result(0x00);
#else
	command_result(0xff);
#endif
}

static void rx_data_process(uint8_t *data, uint16_t len)
{
	uint16_t command;
	uint16_t params_len;

	if (len < 5) {
		fprintf(stderr, "Invalid packet len %d\n", len);
		command_result(0xff);
		return;
	}

	if (data[0] != 0xaa) {
		fprintf(stderr, "Invalid header\n");
		command_result(0xff);
		return;
	}

	command = (uint16_t)data[1] << 8 | data[2];
	params_len = (uint16_t)data[4] << 8 | data[3];
	if (params_len + 5 != len) {
		fprintf(stderr, "Invalid params len %d, %d\n", params_len, len);
		command_result(0xff);
		return;
	}

	switch (command) {
	case NETCFG_VERSION:
		PRLOG("Get version\n");
		netcfg_version();
		break;
	case NETCFG_SCAN:
		PRLOG("Start scan...\n");
		netcfg_scan();
		break;
	case NETCFG_CONNECT:
		PRLOG("Connect...\n");
		netcfg_connect(data + 5, len - 5);
		break;
	case NETCFG_GET_CONNSTATUS:
		PRLOG("Get connection status\n");
		netcfg_get_connstatus(0);
		break;
	case NETCFG_CANCEL_CONN:
		PRLOG("Cancel connection\n");
		netcfg_cancel_conn();
		break;
	default:
		PRLOG("Unsupported command %04x\n", command);
		break;
	}
}

static int netcfg_cmd_receive(const uint8_t *data, uint16_t len)
{
	uint8_t *p;
	ssize_t result = 0;
	uint8_t byte = 1;

	p = malloc(COMM_HEADER + len);
	if (!p) {
		fprintf(stderr, "%s: Failed to alloc buffer\n", __func__);
		return -1;
	}

	p[0] = 0;
	p[1] = 0;
	__put_unaligned_le16(len, p + 2);
	memcpy(p + COMM_HEADER, data, len);

#ifdef THREAD_DEMO
	pthread_mutex_lock(&rx_q_mutex);
	result = queue_push_tail(rx_queue, p);
	pthread_mutex_unlock(&rx_q_mutex);
	if (!result) {
		fprintf(stderr, "%s: Queue push error\n", __func__);
		return -1;
	}

	/* Wake up the thread */
	result = write(sock_fds[0], &byte, 1);
	if (result <= 0)
		fprintf(stderr, "%s: Write error, %s\n", __func__,
			strerror(errno));
#else
	rx_data_process(p + 4, len);
	free(p);
#endif

	return result;
}

static void set_random_address(int fd)
{
	le_set_random_address_cp cmd;
	int urandom_fd;

	memset(&cmd, 0, sizeof(cmd));

	urandom_fd = open("/dev/urandom", O_RDONLY);
	if (urandom_fd < 0) {
		fprintf(stderr, "Failed to open /dev/urandom device\n");
		cmd.bdaddr.b[5] = 0xc0;
		cmd.bdaddr.b[4] = 0x55;
		cmd.bdaddr.b[3] = 0x44;
		cmd.bdaddr.b[2] = 0x33;
		cmd.bdaddr.b[1] = 0x22;
		cmd.bdaddr.b[0] = 0x11;

	} else {
		ssize_t len;

		len = read(urandom_fd, cmd.bdaddr.b, sizeof(cmd.bdaddr.b));
		if (len < 0 || len != sizeof(cmd.bdaddr.b)) {
			fprintf(stderr, "Failed to read random data\n");
			return;
		}

		cmd.bdaddr.b[5] &= 0x3f;
		cmd.bdaddr.b[5] |= 0xc0;
	}

	if (hci_send_cmd(fd, OGF_LE_CTL, OCF_LE_SET_RANDOM_ADDRESS, sizeof(cmd),
			 &cmd) < 0)
		perror("Send msg for set random address error");
}

static void set_adv_parameters(int fd)
{
	le_set_advertising_parameters_cp cmd;

	cmd.min_interval = cpu_to_le16(0x0020);
	cmd.max_interval = cpu_to_le16(0x0020);
	/* 0x00: Connectable undirected advertising
	 * 0x03: Non connectable undirected advertising
	 * */
	cmd.advtype = 0x00;
	/* 0: public address
	 * 1: random address */
	cmd.own_bdaddr_type = 0x01;
	cmd.direct_bdaddr_type = 0x00;
	memset(cmd.direct_bdaddr.b, 0, 6);
	cmd.chan_map = 0x07;
	cmd.filter = 0x00;

	if (hci_send_cmd(fd, OGF_LE_CTL, OCF_LE_SET_ADVERTISING_PARAMETERS,
			 sizeof(cmd), &cmd))
		perror("Send msg for set adv params error");
}

static void set_scan_enable(int fd, uint8_t enable, uint8_t filter_dup)
{
	le_set_scan_enable_cp scan_cp;
	/* uint8_t status; */

	if (enable)
		enable = 0x01;

	memset(&scan_cp, 0, sizeof(scan_cp));
	scan_cp.enable = enable;
	scan_cp.filter_dup = filter_dup;

	if (hci_send_cmd(fd, OGF_LE_CTL, OCF_LE_SET_SCAN_ENABLE, sizeof(scan_cp),
			 &scan_cp) < 0)
		perror("Send cmd for set scan enable error");
}

static void set_adv_enable(int fd, uint8_t enable)
{
	if (enable)
		enable = 0x01;

	if (hci_send_cmd(fd, OGF_LE_CTL, OCF_LE_SET_ADVERTISE_ENABLE, 1,
			 &enable) < 0)
		perror("Send cmd for set adv enable error");
}

static void set_adv_data(int fd)
{
	le_set_advertising_data_cp cmd;
	int i = 0;
	int n;
	const char *name = "WLANConfig";

	cmd.length = 0;
	memset(cmd.data, 0, sizeof(cmd.data));
	/* set adv data */
	cmd.data[i] = 0x02; /* Field length */
	cmd.length += (1 + cmd.data[i++]);
	cmd.data[i++] = 0x01; /* Flags */
	/* LE General Discoverable Mode, BR/EDR Not Supported */
	cmd.data[i++] = (0x02 | 0x04);

	cmd.data[i] = 0x03;
	cmd.length += (1 + cmd.data[i++]);
	cmd.data[i++] = 0x03; /* complete list of 15-bit service class uuids */
	//cmd.data[i++] = 0x12;
	//cmd.data[i++] = 0x18;
	cmd.data[i++] = 0x0a;
	cmd.data[i++] = 0xa0;

	//cmd.data[i] = 0x03;
	//cmd.length += (1 + cmd.data[i++]);
	//cmd.data[i++] = 0x19; /* appearance */
	//cmd.data[i++] = 0xc1;
	//cmd.data[i++] = 0x03;

	n = strlen(name);
	cmd.data[i] = 1 + n;
	cmd.length += (1 + cmd.data[i++]);
	cmd.data[i++] = 0x09; /* complete local name */
	memcpy(&cmd.data[i], name, n);
	i += n;

	//cmd.data[i] = 0x05;
	//cmd.length += (1 + cmd.data[i++]);
	//cmd.data[i++] = 0xff; /* manufacture-specific data */
	//cmd.data[i++] = 0x5d;
	//cmd.data[i++] = 0x00;
	//cmd.data[i++] = 0x04;
	//cmd.data[i++] = 0x00;

	if (hci_send_cmd(fd, OGF_LE_CTL, OCF_LE_SET_ADVERTISING_DATA,
			 sizeof(cmd), &cmd))
		perror("Send cmd for set adv data error");

	//i = 0;
	//cmd.length = 0;
	//memset(cmd.data, 0, sizeof(cmd.data));
	//cmd.data[i] = 0x03;
	//cmd.length += (1 + cmd.data[i++]);
	//cmd.data[i++] = 0x19; /* appearance */
	//cmd.data[i++] = 0xc1;
	//cmd.data[i++] = 0x03;
	//if (hci_send_cmd(fd, OGF_LE_CTL, OCF_LE_SET_SCAN_RESPONSE_DATA,
	//		 sizeof(cmd), &cmd))
	//	error("Send cmd for set rsp data error");
}

static void start_advertising(uint16_t dev_id)
{
	/* uint8_t enable; */
	int hci_fd;

	PRLOG("Start advertising\n");

	hci_fd = hci_open_dev(dev_id);
	if (hci_fd < 0) {
		perror("Failed to open hci dev");
		return;
	}

	set_scan_enable(hci_fd, 0, 1);
	set_adv_enable(hci_fd, 0);
	set_adv_data(hci_fd);
	set_random_address(hci_fd);
	set_adv_parameters(hci_fd);
	set_adv_enable(hci_fd, 1);

	hci_close_dev(hci_fd);
}

static void att_disconnect_cb(int err, void *user_data)
{
	struct server *server = user_data;
	PRLOG("Device disconnected: %s\n", strerror(err));

	/* if notification is enabled, remove the timer and clear the flag */
	if (server->timeout_id) {
		timeout_remove(server->timeout_id);
		server->timeout_id = 0;
	}

	server->enabled = false;

	if (gatt_server) {
		server_destroy(gatt_server);
		gatt_server = NULL;
	}

	// start_advertising(0);

	/* mainloop_quit(); */
}

void att_debug_cb(const char *str, void *user_data)
{
	const char *prefix = user_data;

	PRLOG(COLOR_BOLDGRAY "%s" COLOR_BOLDWHITE "%s\n" COLOR_OFF, prefix,
									str);
}

void gatt_debug_cb(const char *str, void *user_data)
{
	const char *prefix = user_data;

	PRLOG(COLOR_GREEN "%s%s\n" COLOR_OFF, prefix, str);
}

static void gap_device_name_read_cb(struct gatt_db_attribute *attrib,
					unsigned int id, uint16_t offset,
					uint8_t opcode, struct bt_att *att,
					void *user_data)
{
	struct server *server = user_data;
	uint8_t error = 0;
	size_t len = 0;
	const uint8_t *value = NULL;

	PRLOG("GAP Device Name Read called\n");

	len = server->name_len;

	if (offset > len) {
		error = BT_ATT_ERROR_INVALID_OFFSET;
		goto done;
	}

	len -= offset;
	value = len ? &server->device_name[offset] : NULL;

done:
	gatt_db_attribute_read_result(attrib, id, error, value, len);
}

static void gap_device_name_write_cb(struct gatt_db_attribute *attrib,
					unsigned int id, uint16_t offset,
					const uint8_t *value, size_t len,
					uint8_t opcode, struct bt_att *att,
					void *user_data)
{
	struct server *server = user_data;
	uint8_t error = 0;

	PRLOG("GAP Device Name Write called\n");

	/* If the value is being completely truncated, clean up and return */
	if (!(offset + len)) {
		free(server->device_name);
		server->device_name = NULL;
		server->name_len = 0;
		goto done;
	}

	/* Implement this as a variable length attribute value. */
	if (offset > server->name_len) {
		error = BT_ATT_ERROR_INVALID_OFFSET;
		goto done;
	}

	if (offset + len != server->name_len) {
		uint8_t *name;

		name = realloc(server->device_name, offset + len);
		if (!name) {
			error = BT_ATT_ERROR_INSUFFICIENT_RESOURCES;
			goto done;
		}

		server->device_name = name;
		server->name_len = offset + len;
	}

	if (value)
		memcpy(server->device_name + offset, value, len);

done:
	gatt_db_attribute_write_result(attrib, id, error);
}

static void gap_device_name_ext_prop_read_cb(struct gatt_db_attribute *attrib,
					unsigned int id, uint16_t offset,
					uint8_t opcode, struct bt_att *att,
					void *user_data)
{
	uint8_t value[2];

	PRLOG("Device Name Extended Properties Read called\n");

	value[0] = BT_GATT_CHRC_EXT_PROP_RELIABLE_WRITE;
	value[1] = 0;

	gatt_db_attribute_read_result(attrib, id, 0, value, sizeof(value));
}

static void gatt_service_changed_cb(struct gatt_db_attribute *attrib,
					unsigned int id, uint16_t offset,
					uint8_t opcode, struct bt_att *att,
					void *user_data)
{
	PRLOG("Service Changed Read called\n");

	gatt_db_attribute_read_result(attrib, id, 0, NULL, 0);
}

static void gatt_svc_chngd_ccc_read_cb(struct gatt_db_attribute *attrib,
					unsigned int id, uint16_t offset,
					uint8_t opcode, struct bt_att *att,
					void *user_data)
{
	struct server *server = user_data;
	uint8_t value[2];

	PRLOG("Service Changed CCC Read called\n");

	value[0] = server->svc_chngd_enabled ? 0x02 : 0x00;
	value[1] = 0x00;

	gatt_db_attribute_read_result(attrib, id, 0, value, sizeof(value));
}

static void gatt_svc_chngd_ccc_write_cb(struct gatt_db_attribute *attrib,
					unsigned int id, uint16_t offset,
					const uint8_t *value, size_t len,
					uint8_t opcode, struct bt_att *att,
					void *user_data)
{
	struct server *server = user_data;
	uint8_t ecode = 0;

	PRLOG("Service Changed CCC Write called\n");

	if (!value || len != 2) {
		ecode = BT_ATT_ERROR_INVALID_ATTRIBUTE_VALUE_LEN;
		goto done;
	}

	if (offset) {
		ecode = BT_ATT_ERROR_INVALID_OFFSET;
		goto done;
	}

	if (value[0] == 0x00)
		server->svc_chngd_enabled = false;
	else if (value[0] == 0x02)
		server->svc_chngd_enabled = true;
	else
		ecode = 0x80;

	PRLOG("Service Changed Enabled: %s\n",
				server->svc_chngd_enabled ? "true" : "false");

done:
	gatt_db_attribute_write_result(attrib, id, ecode);
}

static void gatt_db_read(struct gatt_db_attribute *attrib,
			 unsigned int id, uint16_t offset,
			 uint8_t opcode, struct bt_att *att,
			 void *user_data)
{
	/* struct server *server = user_data; */
	uint8_t error = 0;
	size_t len = 0;
	const uint8_t *value = NULL;
	static uint8_t buf[20] = {
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
		10, 11, 12, 13, 14, 15, 16, 17, 18, 19
	};

	PRLOG("Read callback\n");

	len = sizeof(buf);
	value = buf;

/* done: */
	gatt_db_attribute_read_result(attrib, id, error, value, len);

}

static void gatt_db_write(struct gatt_db_attribute *attrib,
			  unsigned int id, uint16_t offset,
			  const uint8_t *value, size_t len,
			  uint8_t opcode, struct bt_att *att,
			  void *user_data)
{
	/* struct server *server = user_data; */
	uint8_t ecode = 0;

	/* PRLOG("Write attribute callback\n"); */

	if (!value) {
		ecode = BT_ATT_ERROR_INVALID_ATTRIBUTE_VALUE_LEN;
		goto done;
	}

	if (offset) {
		ecode = BT_ATT_ERROR_INVALID_OFFSET;
		goto done;
	}

	/* PRLOG("Data received, len %d\n", len); */
	netcfg_cmd_receive(value, len);

done:
	gatt_db_attribute_write_result(attrib, id, ecode);

}

static int gatt_send_notification(uint8_t *buf, uint16_t len)
{
	struct server *server = gatt_server;
	int result;
	uint8_t pdu[128];

	if (!server || !server->att) {
		fprintf(stderr, "There is no server/att\n");
		return -ENODEV;
	}

	if (!server->enabled) {
		fprintf(stderr, "Notification is disabled\n");
		return -1;
	}

	printf("Send notification\n");

	pdu[0] = server->value_handle & 0xff;
	pdu[1] = (server->value_handle >> 8) & 0xff;

	if (sizeof(pdu) - 2 < len) {
		fprintf(stderr, "Truncate notify data\n");
		len = sizeof(pdu) - 2;
	}

	memcpy(pdu + 2, buf, len);

	result = !!bt_att_send(server->att, BT_ATT_OP_HANDLE_VAL_NOT, pdu,
		    len + 2, NULL, NULL, NULL);

	if (!result) {
		fprintf(stderr, "Send notification error\n");
		return -EIO;
	}

	return 0;
}

bool notification_timeout(void *user_data)
{
	/* struct server *server = user_data;
	 * uint16_t len = 4;
	 * bool result;
	 */
	uint8_t pdu[4];

	pdu[0] = 0xaa;
	pdu[1] = 0x00 + (rand() % 40);
	gatt_send_notification(pdu, 2);

	/* periodic */
	return true;
}

static void ccc_read_cb(struct gatt_db_attribute *attrib,
					unsigned int id, uint16_t offset,
					uint8_t opcode, struct bt_att *att,
					void *user_data)
{
	struct server *server = user_data;
	uint8_t value[2];

	PRLOG("CCCD read callback\n");

	value[0] = server->enabled ? 0x01 : 0x00;
	value[1] = 0x00;

	gatt_db_attribute_read_result(attrib, id, 0, value, 2);
}

static void ccc_write_cb(struct gatt_db_attribute *attrib,
					unsigned int id, uint16_t offset,
					const uint8_t *value, size_t len,
					uint8_t opcode, struct bt_att *att,
					void *user_data)
{
	struct server *server = user_data;
	uint8_t ecode = 0;

	PRLOG("CCCD write callback\n");

	if (!value || len != 2) {
		ecode = BT_ATT_ERROR_INVALID_ATTRIBUTE_VALUE_LEN;
		goto done;
	}

	if (offset) {
		ecode = BT_ATT_ERROR_INVALID_OFFSET;
		goto done;
	}

	if (value[0] == 0x00)
		server->enabled = false;
	else if (value[0] == 0x01) {
		if (server->enabled) {
			fprintf(stderr, "already enabled\n");
			goto done;
		}

		server->enabled = true;
	} else
		ecode = 0x80;

	PRLOG("Enabled: %s\n", server->enabled ? "true" : "false");

	/* Open a perioc timer to send test data to remote if
	 * notification or indication is enabled
	 *
	 * server->att is set in server_create()
	 * */

#ifdef NOTIFICATION_TIMER
	if (server->enabled) {
		/* server->att = bt_att_ref(att); */
		/* Disconnection callback has been registered in server
		 * creation. */
		server->timeout_id = timeout_add(1000, notification_timeout,
						 server, NULL);
	} else {
		/* bt_att_unref(server->att); */
		if (server->timeout_id) {
			timeout_remove(server->timeout_id);
			server->timeout_id = 0;
		}
	}
#endif

done:
	gatt_db_attribute_write_result(attrib, id, ecode);
}

static void confirm_write(struct gatt_db_attribute *attr, int err,
							void *user_data)
{
	if (!err)
		return;

	fprintf(stderr, "Error caching attribute %p - err: %d\n", attr, err);
	exit(1);
}

static void populate_gap_service(struct server *server)
{
	bt_uuid_t uuid;
	struct gatt_db_attribute *service, *tmp;
	uint16_t appearance;

	/* Add the GAP service */
	bt_uuid16_create(&uuid, UUID_GAP);
	service = gatt_db_add_service(server->db, &uuid, true, 6);

	/*
	 * Device Name characteristic. Make the value dynamically read and
	 * written via callbacks.
	 */
	bt_uuid16_create(&uuid, GATT_CHARAC_DEVICE_NAME);
	gatt_db_service_add_characteristic(service, &uuid,
					BT_ATT_PERM_READ | BT_ATT_PERM_WRITE,
					BT_GATT_CHRC_PROP_READ |
					BT_GATT_CHRC_PROP_EXT_PROP,
					gap_device_name_read_cb,
					gap_device_name_write_cb,
					server);

	bt_uuid16_create(&uuid, GATT_CHARAC_EXT_PROPER_UUID);
	gatt_db_service_add_descriptor(service, &uuid, BT_ATT_PERM_READ,
					gap_device_name_ext_prop_read_cb,
					NULL, server);

	/*
	 * Appearance characteristic. Reads and writes should obtain the value
	 * from the database.
	 */
	bt_uuid16_create(&uuid, GATT_CHARAC_APPEARANCE);
	tmp = gatt_db_service_add_characteristic(service, &uuid,
							BT_ATT_PERM_READ,
							BT_GATT_CHRC_PROP_READ,
							NULL, NULL, server);

	/*
	 * Write the appearance value to the database, since we're not using a
	 * callback.
	 */
	put_le16(128, &appearance);
	gatt_db_attribute_write(tmp, 0, (void *) &appearance,
							sizeof(appearance),
							BT_ATT_OP_WRITE_REQ,
							NULL, confirm_write,
							NULL);

	gatt_db_service_set_active(service, true);
}

static void populate_gatt_service(struct server *server)
{
	bt_uuid_t uuid;
	struct gatt_db_attribute *service, *svc_chngd;

	/* Add the GATT service */
	bt_uuid16_create(&uuid, UUID_GATT);
	service = gatt_db_add_service(server->db, &uuid, true, 4);

	bt_uuid16_create(&uuid, GATT_CHARAC_SERVICE_CHANGED);
	svc_chngd = gatt_db_service_add_characteristic(service, &uuid,
			BT_ATT_PERM_READ,
			BT_GATT_CHRC_PROP_READ | BT_GATT_CHRC_PROP_INDICATE,
			gatt_service_changed_cb,
			NULL, server);
	server->gatt_svc_chngd_handle = gatt_db_attribute_get_handle(svc_chngd);

	bt_uuid16_create(&uuid, GATT_CLIENT_CHARAC_CFG_UUID);
	gatt_db_service_add_descriptor(service, &uuid,
				BT_ATT_PERM_READ | BT_ATT_PERM_WRITE,
				gatt_svc_chngd_ccc_read_cb,
				gatt_svc_chngd_ccc_write_cb, server);

	gatt_db_service_set_active(service, true);
}

static void populate_netcfg_service(struct server *server)
{
	bt_uuid_t uuid;
	struct gatt_db_attribute *service;
	struct gatt_db_attribute *attr;
	struct gatt_db *db = server->db;
	//uint128_t svc_uuid = { {
	//	0x12, 0xA2, 0x4D, 0x2E,
	//	0xFE, 0x14, 0x48, 0x8E,
	//	0x93, 0xD2, 0x17, 0x3C,
	//	0xFF, 0xE0, 0x00, 0x00
	//} };
	uint128_t svc_uuid = { {
		0x00, 0x00, 0xe0, 0xff,
		0x3c, 0x17, 0xd2, 0x93,
		0x8e, 0x48, 0x14, 0xfe,
		0x2e, 0x4d, 0xa2, 0x12,
	} };

	/* Register service definition */
	/* bt_uuid16_create(&uuid, 0xff01); */
	bt_uuid128_create(&uuid, svc_uuid);
	service = gatt_db_add_service(db, &uuid, true, 6);
	if (!service)
		return;

	/* Register a characteristic for read and notification */
	bt_uuid16_create(&uuid, 0xffe1);
	attr = gatt_db_service_add_characteristic(service, &uuid,
			BT_ATT_PERM_READ,
			BT_GATT_CHRC_PROP_NOTIFY | BT_GATT_CHRC_PROP_READ |
			  BT_GATT_CHRC_PROP_INDICATE,
			gatt_db_read, NULL, server);
	if (!attr) {
		fprintf(stderr, "Register a characteristic error\n");
		return;
	}

	server->value_handle = gatt_db_attribute_get_handle(attr);

	/* Register a CCCD */
	bt_uuid16_create(&uuid, GATT_CLIENT_CHARAC_CFG_UUID);
	attr = gatt_db_service_add_descriptor(service, &uuid,
					BT_ATT_PERM_READ | BT_ATT_PERM_WRITE,
					ccc_read_cb,
					ccc_write_cb, server);
	if (!attr) {
		fprintf(stderr, "Register CCCD error\n");
		return;
	}

	/* Register a characteristic for writing */
	bt_uuid16_create(&uuid, 0xffe9);
	attr = gatt_db_service_add_characteristic(service, &uuid,
			BT_ATT_PERM_WRITE,
			BT_GATT_CHRC_PROP_WRITE,
			NULL, gatt_db_write, server);
	if (!attr) {
		fprintf(stderr, "Register a writing characteristic error\n");
		return;
	}

	gatt_db_service_set_active(service, true);
}

static void populate_db(struct server *server)
{
	populate_gap_service(server);
	populate_gatt_service(server);
	populate_netcfg_service(server);
}

static struct server *server_create(int fd, uint16_t mtu)
{
	struct server *server;
	size_t name_len = strlen(test_device_name);

	server = new0(struct server, 1);
	if (!server) {
		fprintf(stderr, "Failed to allocate memory for server\n");
		return NULL;
	}

	server->att = bt_att_new(fd, false);
	if (!server->att) {
		fprintf(stderr, "Failed to initialze ATT transport layer\n");
		goto fail;
	}

	if (!bt_att_set_close_on_unref(server->att, true)) {
		fprintf(stderr, "Failed to set up ATT transport layer\n");
		goto fail;
	}

	if (!bt_att_register_disconnect(server->att, att_disconnect_cb, server,
									NULL)) {
		fprintf(stderr, "Failed to set ATT disconnect handler\n");
		goto fail;
	}

	server->name_len = name_len + 1;
	server->device_name = malloc(name_len + 1);
	if (!server->device_name) {
		fprintf(stderr, "Failed to allocate memory for device name\n");
		goto fail;
	}

	memcpy(server->device_name, test_device_name, name_len);
	server->device_name[name_len] = '\0';

	server->fd = fd;
	server->db = gatt_db_new();
	if (!server->db) {
		fprintf(stderr, "Failed to create GATT database\n");
		goto fail;
	}

	server->gatt = bt_gatt_server_new(server->db, server->att, mtu, 0);
	if (!server->gatt) {
		fprintf(stderr, "Failed to create GATT server\n");
		goto fail;
	}

	if (verbose) {
		bt_att_set_debug(server->att, BT_ATT_DEBUG_VERBOSE,
						att_debug_cb, "att: ", NULL);
		bt_gatt_server_set_debug(server->gatt, gatt_debug_cb,
							"server: ", NULL);
	}

	srand(time(NULL));

	/* bt_gatt_server already holds a reference */
	populate_db(server);

	return server;

fail:
	gatt_db_unref(server->db);
	free(server->device_name);
	bt_att_unref(server->att);
	free(server);

	return NULL;
}

static void server_destroy(struct server *server)
{
	if (server->timeout_id) {
		timeout_remove(server->timeout_id);
		server->timeout_id = 0;
	}
	bt_gatt_server_unref(server->gatt);
	gatt_db_unref(server->db);
}

static void usage(void)
{
	printf("btgatt-server\n");
	printf("Usage:\n\tbtgatt-server [options]\n");

	printf("Options:\n"
		"\t-i, --index <id>\t\tSpecify adapter index, e.g. hci0\n"
		"\t-m, --mtu <mtu>\t\t\tThe ATT MTU to use\n"
		"\t-s, --security-level <sec>\tSet security level (low|"
								"medium|high)\n"
		"\t-t, --type [random|public] \t The source address type\n"
		"\t-v, --verbose\t\t\tEnable extra logging\n"
		"\t-r, --heart-rate\t\tEnable Heart Rate service\n"
		"\t-h, --help\t\t\tDisplay help\n");
}

static struct option main_options[] = {
	{ "index",		1, 0, 'i' },
	{ "mtu",		1, 0, 'm' },
	{ "security-level",	1, 0, 's' },
	{ "type",		1, 0, 't' }, /* address type */
	{ "verbose",		0, 0, 'v' },
	{ "help",		0, 0, 'h' },
	{ }
};

static int l2cap_le_att_listen(bdaddr_t *src, int sec, uint8_t src_type)
{
	int sk;
	struct sockaddr_l2 srcaddr;
	struct bt_security btsec;

	sk = socket(PF_BLUETOOTH, SOCK_SEQPACKET, BTPROTO_L2CAP);
	if (sk < 0) {
		perror("Failed to create L2CAP socket");
		return -1;
	}

	/* Set up source address */
	memset(&srcaddr, 0, sizeof(srcaddr));
	srcaddr.l2_family = AF_BLUETOOTH;
	srcaddr.l2_cid = htobs(ATT_CID);
	srcaddr.l2_bdaddr_type = src_type;
	bacpy(&srcaddr.l2_bdaddr, src);

	if (bind(sk, (struct sockaddr *) &srcaddr, sizeof(srcaddr)) < 0) {
		perror("Failed to bind L2CAP socket");
		goto fail;
	}

	/* Set the security level */
	memset(&btsec, 0, sizeof(btsec));
	btsec.level = sec;
	if (setsockopt(sk, SOL_BLUETOOTH, BT_SECURITY, &btsec,
							sizeof(btsec)) != 0) {
		fprintf(stderr, "Failed to set L2CAP security level\n");
		goto fail;
	}

	if (listen(sk, 10) < 0) {
		perror("Listening on socket failed");
		goto fail;
	}

	printf("Started listening on ATT channel. Waiting for connections\n");

	return sk;

fail:
	close(sk);
	return -1;
}

static int l2cap_le_att_accept(int sk)
{
	int nsk;
	socklen_t optlen;
	struct sockaddr_l2 addr;
	char ba[18];

	if (sk < 0)
		goto fail;

	memset(&addr, 0, sizeof(addr));
	optlen = sizeof(addr);
	nsk = accept(sk, (struct sockaddr *) &addr, &optlen);
	if (nsk < 0) {
		perror("Accept failed");
		goto fail;
	}

	ba2str(&addr.l2_bdaddr, ba);
	PRLOG("Connect from %s\n", ba);

	return nsk;
fail:
	return -1;

}

static void server_listen_cb(int fd, uint32_t events, void *user_data)
{
	int accept_fd;
	struct server *server;

	if (events & (EPOLLRDHUP | EPOLLHUP | EPOLLERR)) {
		mainloop_remove_fd(fd);
		return;
	}

	accept_fd = l2cap_le_att_accept(fd);
	if (accept_fd < 0) {
		fprintf(stderr, "Accept error\n");
		return;
	}

	server = server_create(accept_fd, att_mtu);
	if (!server) {
		fprintf(stderr, "Server creation failed\n");
		close(accept_fd);
		return;
	}

	gatt_server = server;
}

static void notify_usage(void)
{
	printf("Usage: notify [options] <value_handle> <value>\n"
					"Options:\n"
					"\t -i, --indicate\tSend indication\n"
					"e.g.:\n"
					"\tnotify 0x0001 00 01 00\n");
}

static struct option notify_options[] = {
	{ "indicate",	0, 0, 'i' },
	{ }
};

static bool parse_args(char *str, int expected_argc,  char **argv, int *argc)
{
	char **ap;

	for (ap = argv; (*ap = strsep(&str, " \t")) != NULL;) {
		if (**ap == '\0')
			continue;

		(*argc)++;
		ap++;

		if (*argc > expected_argc)
			return false;
	}

	return true;
}

static void conf_cb(void *user_data)
{
	PRLOG("Received confirmation\n");
}

static void cmd_test(struct server *server, char *cmd_str)
{
	int opt, i;
	char *argvbuf[516];
	char **argv = argvbuf;
	int argc = 1;
	uint16_t handle;
	char *endptr = NULL;
	int length;
	uint8_t *value = NULL;
	bool indicate = false;

	if (!parse_args(cmd_str, 514, argv + 1, &argc)) {
		printf("Too many arguments\n");
		notify_usage();
		return;
	}

	optind = 0;
	argv[0] = "notify";
	while ((opt = getopt_long(argc, argv, "+i", notify_options,
								NULL)) != -1) {
		switch (opt) {
		case 'i':
			indicate = true;
			break;
		default:
			notify_usage();
			return;
		}
	}

	argc -= optind;
	argv += optind;

	if (argc < 1) {
		notify_usage();
		return;
	}

	handle = strtol(argv[0], &endptr, 16);
	if (!endptr || *endptr != '\0' || !handle) {
		printf("Invalid handle: %s\n", argv[0]);
		return;
	}

	length = argc - 1;

	if (length > 0) {
		if (length > UINT16_MAX) {
			printf("Value too long\n");
			return;
		}

		value = malloc(length);
		if (!value) {
			printf("Failed to construct value\n");
			return;
		}

		for (i = 1; i < argc; i++) {
			if (strlen(argv[i]) != 2) {
				printf("Invalid value byte: %s\n",
								argv[i]);
				goto done;
			}

			value[i-1] = strtol(argv[i], &endptr, 16);
			if (endptr == argv[i] || *endptr != '\0'
							|| errno == ERANGE) {
				printf("Invalid value byte: %s\n",
								argv[i]);
				goto done;
			}
		}
	}

	if (indicate) {
		if (!bt_gatt_server_send_indication(server->gatt, handle,
							value, length,
							conf_cb, NULL, NULL))
			printf("Failed to initiate indication\n");
	} else if (!bt_gatt_server_send_notification(server->gatt, handle,
							value, length, false))
		printf("Failed to initiate notification\n");

done:
	free(value);
}

static void cmd_help(struct server *server, char *cmd_str);

typedef void (*command_func_t)(struct server *server, char *cmd_str);

static struct {
	char *cmd;
	command_func_t func;
	char *doc;
} command[] = {
	{ "help", cmd_help, "\tDisplay help message" },
	{ "test", cmd_test, "\tTest" },
	{ }
};

static void cmd_help(struct server *server, char *cmd_str)
{
	int i;

	printf("Commands:\n");
	for (i = 0; command[i].cmd; i++)
		printf("\t%-15s\t%s\n", command[i].cmd, command[i].doc);
}

static void prompt_read_cb(int fd, uint32_t events, void *user_data)
{
	ssize_t read;
	size_t len = 0;
	char *line = NULL;
	char *cmd = NULL, *args;
	struct server *server = gatt_server;
	int i;

	if (events & (EPOLLRDHUP | EPOLLHUP | EPOLLERR)) {
		mainloop_quit();
		return;
	}

	read = getline(&line, &len, stdin);
	if (read < 0)
		return;

	if (read <= 1) {
		cmd_help(server, NULL);
		print_prompt();
		return;
	}

	line[read-1] = '\0';
	args = line;

	while ((cmd = strsep(&args, " \t")))
		if (*cmd != '\0')
			break;

	if (!cmd)
		goto failed;

	for (i = 0; command[i].cmd; i++) {
		if (strcmp(command[i].cmd, cmd) == 0)
			break;
	}

	if (command[i].cmd)
		command[i].func(server, args);
	else
		fprintf(stderr, "Unknown command: %s\n", line);

failed:
	print_prompt();

	free(line);
}

static void signal_cb(int signum, void *user_data)
{
	switch (signum) {
	case SIGINT:
	case SIGTERM:
		printf("Receive quit signal %d\n", signum);
		mainloop_quit();
		break;
	default:
		break;
	}
}

/* Functions provided in shared-mainloop library is not multi-thread safe.
 * The functions must be called in mainloop.
 * Calling out of mainloop will result in race condition and unpredictable
 * result.
 * */
static void sock_read_cb(int fd, uint32_t events, void *user_data)
{
	struct server *server = gatt_server;
	ssize_t ret;
	uint8_t read_buf[8];
	uint8_t *data;
	int len;
	bool result;

	/* printf("mainloop sock read\n"); */

	if (events & (EPOLLRDHUP | EPOLLHUP | EPOLLERR)) {
		fprintf(stderr, "Socket error\n");
		return;
	}

	ret = read(fd, read_buf, sizeof(read_buf));
	if (ret <= 0) {
		fprintf(stderr, "Sock read error %ld", ret);
		return;
	}

	do {
		pthread_mutex_lock(&tx_q_mutex);
		data = queue_pop_head(tx_queue);
		pthread_mutex_unlock(&tx_q_mutex);

		if (!data)
			break;

		len = ((uint16_t)data[3] << 8 | data[2]);
		/* printf("Send data from queue, len %u\n", len); */

		switch (data[0]) {
		case ATT_NOTIF:
			result = bt_gatt_server_send_notification(server->gatt,
					server->value_handle,
					data + COMM_HEADER, len, false);
			if (!result)
				fprintf(stderr, "Transmit notification err\n");
			break;
		case ATT_INDIC:
			if (!bt_gatt_server_send_indication(server->gatt,
						server->value_handle,
						data + COMM_HEADER,
						len, conf_cb, NULL, NULL))
				fprintf(stderr, "Transmit ind error\n");
			break;

		}
		free(data);
	} while (--ret);
}

#ifdef THREAD_DEMO
static void *thread_func(void *user_data)
{
	uint8_t *data;
	uint8_t read_buf[8];
	struct pollfd p;
	int ret;
	uint16_t len;

	p.fd = sock_fds[1];
	p.events = POLLERR | POLLHUP | POLLIN;

	while (thread_running) {
		p.revents = 0;
		if (poll(&p, 1, -1) <= 0) {
			fprintf(stderr, "Poll call error, %s\n",
				strerror(errno));
			break;
		}

		if (p.revents & (POLLERR | POLLHUP)) {
			fprintf(stderr, "POLLERR or POLLUP happens, %s\n",
				strerror(errno));
			break;
		}

		ret = read(sock_fds[1], read_buf, sizeof(read_buf));
		if (ret <= 0) {
			fprintf(stderr, "Read socket error in thread, %s\n",
				strerror(errno));
			continue;
		}

		do {
			pthread_mutex_lock(&rx_q_mutex);
			data = queue_pop_head(rx_queue);
			pthread_mutex_unlock(&rx_q_mutex);

			if (!data)
				continue;

			len = __get_unaligned_le16(data + 2);
			/* printf("%s: process data\n", __func__); */
			rx_data_process(data + 4, len);

			free(data);
		} while (--ret);
	}

	printf("Thread exit\n");

	return NULL;
}
#endif

int main(int argc, char *argv[])
{
	int opt;
	bdaddr_t src_addr;
	int dev_id = -1;
	int fd;
	int sec = BT_SECURITY_LOW;
	uint8_t src_type = BDADDR_LE_PUBLIC;
	sigset_t mask;
	int ret;
#ifdef WPA_SOCK
	char str[64];
#endif

	while ((opt = getopt_long(argc, argv, "+hvrs:t:m:i:",
						main_options, NULL)) != -1) {
		switch (opt) {
		case 'h':
			usage();
			return EXIT_SUCCESS;
		case 'v':
			verbose = true;
			break;
		case 's':
			if (strcmp(optarg, "low") == 0)
				sec = BT_SECURITY_LOW;
			else if (strcmp(optarg, "medium") == 0)
				sec = BT_SECURITY_MEDIUM;
			else if (strcmp(optarg, "high") == 0)
				sec = BT_SECURITY_HIGH;
			else {
				fprintf(stderr, "Invalid security level\n");
				return EXIT_FAILURE;
			}
			break;
		case 't':
			if (strcmp(optarg, "random") == 0)
				src_type = BDADDR_LE_RANDOM;
			else if (strcmp(optarg, "public") == 0)
				src_type = BDADDR_LE_PUBLIC;
			else {
				fprintf(stderr,
					"Allowed types: random, public\n");
				return EXIT_FAILURE;
			}
			break;
		case 'm': {
			int arg;

			arg = atoi(optarg);
			if (arg <= 0) {
				fprintf(stderr, "Invalid MTU: %d\n", arg);
				return EXIT_FAILURE;
			}

			if (arg > UINT16_MAX) {
				fprintf(stderr, "MTU too large: %d\n", arg);
				return EXIT_FAILURE;
			}

			att_mtu = (uint16_t) arg;
			break;
		}
		case 'i':
			dev_id = hci_devid(optarg);
			if (dev_id < 0) {
				perror("Invalid adapter");
				return EXIT_FAILURE;
			}

			break;
		default:
			fprintf(stderr, "Invalid option: %c\n", opt);
			return EXIT_FAILURE;
		}
	}

	argc -= optind;
	argv -= optind;
	optind = 0;

	if (argc) {
		usage();
		return EXIT_SUCCESS;
	}

	if (dev_id == -1)
		bacpy(&src_addr, BDADDR_ANY);
	else if (hci_devba(dev_id, &src_addr) < 0) {
		perror("Adapter not available");
		return EXIT_FAILURE;
	}

	fd = l2cap_le_att_listen(&src_addr, sec, src_type);
	if (fd < 0) {
		fprintf(stderr, "Failed to listen L2CAP ATT connection\n");
		return EXIT_FAILURE;
	}

	mainloop_init();

	if (mainloop_add_fd(fd, EPOLLIN, server_listen_cb, NULL, NULL) < 0) {
		fprintf(stderr, "Failed to add listen socket\n");
		close(fd);
		return EXIT_FAILURE;
	}

	if (mainloop_add_fd(fileno(stdin),
				EPOLLIN | EPOLLRDHUP | EPOLLHUP | EPOLLERR,
				prompt_read_cb, NULL, NULL) < 0) {
		fprintf(stderr, "Failed to initialize console\n");
		if (gatt_server)
			server_destroy(gatt_server);

		return EXIT_FAILURE;
	}

	tx_queue = queue_new();
	if (!tx_queue) {
		fprintf(stderr, "Failed to initalize tx queue\n");
		goto err1;
	}

	ret = socketpair(PF_UNIX, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,
			 0, sock_fds);
	if (ret == -1) {
		fprintf(stderr, "socketpair call error, %s\n", strerror(errno));
		goto err2;
	}

	ret = mainloop_add_fd(sock_fds[0],
			      EPOLLIN | EPOLLRDHUP | EPOLLHUP | EPOLLERR,
			      sock_read_cb, NULL, NULL);
	if (ret < 0) {
		fprintf(stderr, "Failed to initialize sock read\n");
		goto err3;
	}

#ifdef THREAD_DEMO
	rx_queue = queue_new();
	if (!rx_queue) {
		fprintf(stderr, "Failed to initalize rx queue\n");
		goto err4;
	}
	thread_running = 1;
	ret = pthread_create(&thread_id, NULL, thread_func, NULL);
	if (ret < 0){
		fprintf(stderr, "Failed to create pthread\n");
		goto err5;
	}
#endif

	printf("Running GATT server\n");

	sigemptyset(&mask);
	sigaddset(&mask, SIGINT);
	sigaddset(&mask, SIGTERM);

	mainloop_set_signal(&mask, signal_cb, NULL, NULL);

	print_prompt();

#ifdef WPA_SOCK
	sprintf(str, "%s/%s", CTRL_PATH, CTRL_FILE);
	ctrl = wpa_ctrl_open2(str);
	if (!ctrl) {
		printf("Open wpa ctrl %s file error\n", str);
	}
#endif

	// start_advertising(0);

	mainloop_run();

	printf("\n\nShutting down...\n");
#ifdef WPA_SOCK
	wpa_ctrl_close(ctrl);
#endif
#ifdef THREAD_DEMO
	thread_running = 0;
	ret = write(sock_fds[0], &thread_running, 1);
	pthread_join(thread_id, NULL);
err5:
	queue_destroy(rx_queue, NULL);
err4:
	mainloop_remove_fd(sock_fds[0]);
#endif
err3:
	close(sock_fds[0]);
	close(sock_fds[1]);
err2:
	queue_destroy(tx_queue, NULL);
err1:
	close(fd);
	mainloop_remove_fd(fileno(stdin));
	if (gatt_server)
		server_destroy(gatt_server);

	return EXIT_SUCCESS;
}
