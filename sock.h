/* See LICENSE file for copyright and license details. */
#ifndef SOCK_H
#define SOCK_H

#include <stddef.h>
#include <sys/socket.h>
#include <sys/types.h>

int sock_get_ips_arr(const char *, const char *, int *, size_t);
void sock_rem_uds(const char *);
int sock_get_uds_arr(const char *, uid_t, gid_t, int *, size_t);
int sock_set_timeout(int, int);
int sock_set_nonblocking(int);
int sock_get_inaddr_str(const struct sockaddr_storage *, char *, size_t);
int sock_same_addr(const struct sockaddr_storage *,
                   const struct sockaddr_storage *);

#endif /* SOCK_H */
