/*
 *  Copyright (C) 2013 Realtek Semiconductor Corp.
 */
#ifndef __WPA_IPC__
#define __WPA_IPC__

#include <stdint.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>

#define ARRAY_SIZE(a) (sizeof(a) / sizeof((a)[0]))

struct wpa_ctrl {
	int s;
	struct sockaddr_un local;
	struct sockaddr_un dest;
};

struct conn_req {
	uint8_t band;
	uint8_t encrypt;
	char ssid[32];
	uint8_t mac[6];
	char password[64];
} __attribute__((packed));

/* response to remote Bluetooth */
struct conn_status {
	/* 0x00: failed, 0x01:connecting, 0x06: password error,
	 * 0x04: success
	 */
	uint8_t status;
	uint8_t encrypt;
	uint8_t mac[6];
	char ssid[32];
	int8_t rssi;
} __attribute__((packed));

struct ap_info {
	/* 0 for none, 1 for wpa, 2 for wep */
	uint8_t encrypt;
	uint8_t mac[6];
	char    ssid[32];
	uint8_t channel;
	int8_t  rssi;
} __attribute__((packed));

struct wpa_ctrl *wpa_ctrl_open2(const char *ctrl_path);
void wpa_ctrl_close(struct wpa_ctrl *ctrl);
int wpa_ctrl_command2(struct wpa_ctrl *ctrl, char *cmd);
int scan_on(struct wpa_ctrl *ctrl);
int scan_results(struct wpa_ctrl *ctrl, struct ap_info *ap_info, int count);
int connect_to(struct wpa_ctrl *ctrl, struct conn_req *req);
int connection_status(struct wpa_ctrl *ctrl, struct conn_status *status);

#endif /* end of __WPA_IPC__ */
