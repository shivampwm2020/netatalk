/*
  Copyright (c) 2026 Andy Lemin (andylemin)

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; either version 2 of the License, or
  (at your option) any later version.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.
*/

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif /* HAVE_CONFIG_H */

#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <atalk/adouble.h>
#include <atalk/afp.h>
#include <atalk/dsi.h>
#include <atalk/globals.h>
#include <atalk/util.h>
#include <atalk/volume.h>

#include "afpfunc_helpers.h"
#include "dircache.h"
#include "directory.h"
#include "faultinject.h"
#include "fork.h"
#include "subtests_sendfile.h"
#include "test.h"
#include "test_capabilities.h"
#include "virtual_icon.h"

#define SF_LEN   (1024 * 1024)
#define SF_SMALL (64 * 1024)
#define SF_HALF  (DSI_DATASIZ / 2)
#define SF_SHORT 64
#define SF_HUGE  (((uint64_t)1 << 31) + 4096)
#define SF_BUF   65536
#define SF_FRAME (DSI_BLOCKSIZ + 20)
#define SF_ODD   4097

/*! A DSI session, one fork, and a reader on the peer */
struct sf {
    AFPObj obj;
    struct vol *vol;
    DSI dsi;
    int peer;
    pthread_t reader;
    uint8_t *got;
    size_t cap;
    size_t gotlen;
    uint16_t refnum;
    const char *name;
    int exit_code;
    /* in_write as obj->exit() saw it: afpd's session ends there, the test's
     * goes on */
    int exit_in_write;
    bool eof;
};

/*! The session whose read path calls sf_exit() */
static struct sf *sf_session;

/*! The file the tests create, its name writable as dircache lookups take it */
static char sf_file[] = "sf_file";

/*!
 * @brief obj->exit() of the read path: records the code it ends with and
 *        in_write, which afp_dsi_die() reads before it writes an attention
 */
static void sf_exit(int code)
{
    sf_session->exit_code = code;
    sf_session->exit_in_write = sf_session->dsi.in_write;
}

/*! @brief Byte i of every fork the tests write */
static uint8_t sf_byte(size_t i)
{
    return (uint8_t)(i % 251);
}

/*!
 * @brief Reader thread: take up to cap bytes of the reply, then close the
 *        peer, which ends a reply longer than cap
 */
static void *sf_reader(void *arg)
{
    struct sf *sf = arg;

    while (sf->gotlen < sf->cap) {
        ssize_t n = read(sf->peer, sf->got + sf->gotlen, sf->cap - sf->gotlen);

        if (n == -1 && errno == EINTR) {
            continue;
        }

        if (n <= 0) {
            sf->eof = n == 0;
            break;
        }

        sf->gotlen += (size_t)n;
    }

    close(sf->peer);
    sf->peer = -1;
    return NULL;
}

/*! @brief Close the fork, delete its file and release the session */
static void sf_teardown(struct sf *sf)
{
    fault_inject_reset();

    if (sf->peer != -1) {
        close(sf->peer);
    }

    if (sf->refnum != 0) {
        closefork(&sf->obj, sf->refnum);
    }

    if (sf->name != NULL) {
        delete (&sf->obj, sf->vol->v_vid, DIRDID_ROOT, sf->name);
    }

    dir_free_invalid_q();
    free(sf->got);
    dsi_test_cleanup(&sf->dsi);
}

/*!
 * @brief Serve a copy of obj on one end of a socket pair with sendfile on
 *
 * @returns 0, or -1
 */
static int sf_pair_setup(struct sf *sf, AFPObj *obj, struct vol *vol)
{
    memset(sf, 0, sizeof(*sf));
    sf->peer = -1;
    sf->vol = vol;
    sf->obj = *obj;
    sf->obj.dsi = &sf->dsi;
    sf->obj.proto = AFPPROTO_DSI;
    sf->obj.exit = sf_exit;
    sf->obj.options.flags &= ~OPTION_NOSENDFILE;
    sf_session = sf;
    fault_inject_reset();

    if (dsi_test_open(&sf->dsi, DSI_DATASIZ, &sf->peer) != 0) {
        sf_teardown(sf);
        return -1;
    }

    return 0;
}

/*!
 * @brief sf_pair_setup() over loopback TCP with a small send buffer, so a
 *        reply takes many calls, for a test of the sendfile path; with fault
 *        set, for one that arms the fault interposer
 *
 * @returns 0, -1, or TEST_SKIP on a build without sendfile, in a sandbox
 *          without loopback, and with fault set where the interposer does not
 *          reach
 */
static int sf_sendfile_setup(struct sf *sf, AFPObj *obj, struct vol *vol,
                             bool fault)
{
#ifndef WITH_SENDFILE
    return TEST_SKIP;
#else
    int sndbuf = SF_BUF;
    int err;

    if (fault && !test_capability(TEST_CAP_FAULT_INJECT, vol)) {
        return TEST_SKIP;
    }

    if (sf_pair_setup(sf, obj, vol) != 0) {
        return -1;
    }

    if (dsi_test_connect_tcp(&sf->dsi, &sf->peer) != 0) {
        err = errno;
        sf_teardown(sf);
        return dsi_test_no_loopback(err) ? TEST_SKIP : -1;
    }

    if (setsockopt(sf->dsi.socket, SOL_SOCKET, SO_SNDBUF, &sndbuf,
                   sizeof(sndbuf)) != 0) {
        sf_teardown(sf);
        return -1;
    }

    return 0;
#endif
}

/*!
 * @brief sf_pair_setup() for a read through the request buffer, as under
 *        use sendfile = no
 *
 * @returns 0, or -1
 */
static int sf_buffered_setup(struct sf *sf, AFPObj *obj, struct vol *vol)
{
    if (sf_pair_setup(sf, obj, vol) != 0) {
        return -1;
    }

    sf->obj.options.flags |= OPTION_NOSENDFILE;
    return 0;
}

/*!
 * @brief FPCreateFile sf_file, FPOpenFork of its fork read-write, and len
 *        bytes of sf_byte() written into the fork
 *
 * @returns the fork's adouble, or NULL
 */
static struct adouble *sf_open(struct sf *sf, uint8_t fork, size_t len)
{
    static char buf[65536];
    int eid = fork == OPENFORK_DATA ? ADEID_DFORK : ADEID_RFORK;
    struct ofork *of;
    sf->name = sf_file;

    if (createfile(&sf->obj, sf->vol->v_vid, DIRDID_ROOT, sf->name) != AFP_OK
            || openfork(&sf->obj, sf->vol->v_vid, DIRDID_ROOT, sf->name, fork,
                        OPENACC_RD | OPENACC_WR, &sf->refnum) != AFP_OK
            || (of = of_find(sf->refnum)) == NULL) {
        return NULL;
    }

    for (size_t off = 0; off < len; off += sizeof(buf)) {
        size_t n = MIN(sizeof(buf), len - off);

        for (size_t i = 0; i < n; i++) {
            buf[i] = (char)sf_byte(off + i);
        }

        if (ad_write(of->of_ad, eid, (off_t)off, 0, buf, n) != (ssize_t)n) {
            return NULL;
        }
    }

    return of->of_ad;
}

/*!
 * @brief Close the fork sf_open() opened, have FPGetFileDirParms put the file
 *        in the dircache with rlen as its resource fork's length, and open
 *        that fork read-only, the open the rfork cache serves
 *
 * @returns the file's dircache entry, or NULL
 */
static struct dir *sf_reopen_cached(struct sf *sf, off_t rlen)
{
    struct dir *entry;

    if (closefork(&sf->obj, sf->refnum) != AFP_OK) {
        return NULL;
    }

    sf->refnum = 0;

    if (getfiledirparms(&sf->obj, sf->vol->v_vid, DIRDID_ROOT, sf_file)
            != AFP_OK
            || (entry = dircache_search_by_name(sf->vol,
                                                dirlookup(sf->vol, DIRDID_ROOT),
                                                sf_file,
                                                sizeof(sf_file) - 1)) == NULL
            || openfork(&sf->obj, sf->vol->v_vid, DIRDID_ROOT, sf_file,
                        OPENFORK_RSCS, OPENACC_RD, &sf->refnum) != AFP_OK) {
        return NULL;
    }

    entry->dcache_rlen = rlen;
    return entry;
}

/*! @brief Pack request 1, an FPReadExt for count bytes at offset, into frame */
static void sf_frame(const struct sf *sf, uint8_t *frame, uint64_t offset,
                     uint64_t count)
{
    uint8_t *p = frame + DSI_BLOCKSIZ;
    uint64_t offset_be = hton64(offset);
    uint64_t count_be = hton64(count);
    dsi_test_frame(frame, DSIFUNC_CMD, 1, 0, 20, AFP_READ_EXT);
    /* refnum travels raw, as afp_openfork() returns it */
    memcpy(p + 2, &sf->refnum, sizeof(sf->refnum));
    memcpy(p + 4, &offset_be, sizeof(offset_be));
    memcpy(p + 12, &count_be, sizeof(count_be));
}

/*!
 * @brief Send FPReadExt for count bytes at offset as a client does, and
 *        receive it on the session
 *
 * @returns 0, or -1 when the request goes astray
 */
static int sf_request(struct sf *sf, uint64_t offset, uint64_t count)
{
    uint8_t frame[SF_FRAME];
    sf_frame(sf, frame, offset, count);

    if (write(sf->peer, frame, sizeof(frame)) != sizeof(frame)
            || dsi_stream_receive(&sf->dsi) != DSIFUNC_CMD) {
        return -1;
    }

    return 0;
}

/*!
 * @brief Send FPReadExt for count bytes at offset as a client does, and run
 *        it as afp_over_dsi() does while the reader takes up to cap bytes of
 *        the reply; the stream ends after it
 *
 * @returns the AFP result, or -1 when the request goes astray
 */
static int sf_read(struct sf *sf, uint64_t offset, uint64_t count, size_t cap)
{
    size_t rbuflen = DSI_DATASIZ;
    int ret;

    if (sf_request(sf, offset, count) != 0
            || (sf->got = malloc(cap)) == NULL) {
        return -1;
    }

    sf->cap = cap;

    if (pthread_create(&sf->reader, NULL, sf_reader, sf) != 0) {
        return -1;
    }

    ret = afp_read_ext(&sf->obj, (char *)sf->dsi.commands, sf->dsi.cmdlen,
                       (char *)sf->dsi.data, &rbuflen);
    shutdown(sf->dsi.socket, SHUT_WR);
    pthread_join(sf->reader, NULL);
    return ret;
}

/*! @brief Does the reply open with request 1's header carrying code and len? */
static bool sf_header_is(const struct sf *sf, int code, uint32_t len)
{
    uint8_t want[DSI_BLOCKSIZ];
    dsi_test_header(want, DSIFUNC_CMD, 1, (uint32_t)code, len);
    want[0] = DSIFL_REPLY;
    return sf->gotlen >= DSI_BLOCKSIZ
           && memcmp(sf->got, want, sizeof(want)) == 0;
}

/*!
 * @brief Is everything after the header exactly len bytes of the fork from
 *        offset start?
 */
static bool sf_payload_from(const struct sf *sf, size_t start, size_t len)
{
    if (sf->gotlen != DSI_BLOCKSIZ + len) {
        return false;
    }

    for (size_t i = 0; i < len; i++) {
        if (sf->got[DSI_BLOCKSIZ + i] != sf_byte(start + i)) {
            return false;
        }
    }

    return true;
}

/*!
 * @brief Did the session send len bytes of the fork from start whole, under
 *        an AFP_OK header, as its only reply, and go on with no reply under
 *        way?
 */
static bool sf_reply_whole(const struct sf *sf, size_t start, size_t len)
{
    return sf_header_is(sf, AFP_OK, (uint32_t)len)
           && sf_payload_from(sf, start, len)
           && (sf->dsi.flags & DSI_NOREPLY)
           && sf->exit_code == 0 && sf->dsi.in_write == 0;
}

/*!
 * @brief A reply sent with sendfile through a small send buffer arrives
 *        whole and in step
 */
int utest_sendfile_read_lands_intact(AFPObj *obj, struct vol *vol)
{
    struct sf sf;
    int ret;

    if ((ret = sf_sendfile_setup(&sf, obj, vol, false)) != 0) {
        return ret;
    }

    ret = -1;

    if (sf_open(&sf, OPENFORK_DATA, SF_LEN) != NULL
            && sf_read(&sf, 0, SF_LEN, DSI_BLOCKSIZ + SF_LEN + 1) == AFP_OK
            && sf_reply_whole(&sf, 0, SF_LEN)) {
        ret = 0;
    }

    sf_teardown(&sf);
    return ret;
}

/*!
 * @brief A reply header that fails to send ends the session with nothing
 *        sent: no file data goes out without it
 */
int utest_sendfile_header_failure_sends_nothing(AFPObj *obj, struct vol *vol)
{
#ifndef SENDFILE_FLAVOR_LINUX
    /* elsewhere the header travels in the sendfile call itself */
    return TEST_SKIP;
#else
    struct sf sf;
    int ret;

    if ((ret = sf_sendfile_setup(&sf, obj, vol, true)) != 0) {
        return ret;
    }

    ret = -1;

    if (sf_open(&sf, OPENFORK_DATA, SF_SMALL) != NULL) {
        fi.send.armed = 1;
        fi.send.fail_after = 0;
        fi.send.errnum = ECONNRESET;
        sf_read(&sf, 0, SF_SMALL, DSI_BLOCKSIZ + SF_SMALL + 1);

        if (fi.send.fail_after == -1
                && sf.gotlen == 0
                && sf.exit_code == EXITERR_CLNT
                && sf.exit_in_write == 1) {
            ret = 0;
        }
    }

    sf_teardown(&sf);
    return ret;
#endif
}

/*!
 * @brief A reply header the socket takes a few bytes at a time goes out whole
 *        before the file data
 */
int utest_sendfile_partial_header_lands_intact(AFPObj *obj, struct vol *vol)
{
#ifndef SENDFILE_FLAVOR_LINUX
    /* elsewhere the header travels in the sendfile call itself */
    return TEST_SKIP;
#else
    struct sf sf;
    int ret;

    if ((ret = sf_sendfile_setup(&sf, obj, vol, true)) != 0) {
        return ret;
    }

    ret = -1;

    if (sf_open(&sf, OPENFORK_DATA, SF_SMALL) != NULL) {
        fi.send_max = 5;

        if (sf_read(&sf, 0, SF_SMALL, DSI_BLOCKSIZ + SF_SMALL + 1) == AFP_OK
                && sf_reply_whole(&sf, 0, SF_SMALL)) {
            ret = 0;
        }
    }

    sf_teardown(&sf);
    return ret;
#endif
}

/*!
 * @brief dsi_close() on a session over a socket pair whose in_write is
 *        in_write; got takes what the peer reads before the end of the stream
 *
 * @returns the bytes got takes, or -1
 */
static ssize_t sf_close(int in_write, uint8_t *got, size_t len)
{
    DSI *dsi;
    int fds[2];
    ssize_t n;

    if ((dsi = calloc(1, sizeof(*dsi))) == NULL) {
        return -1;
    }

    if (socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0) {
        free(dsi);
        return -1;
    }

    dsi->socket = fds[1];
    dsi->proto_close = dsi_test_close_socket;
    dsi->in_write = in_write;
    /* frees dsi */
    dsi_close(dsi);
    n = recv(fds[0], got, len, 0);
    close(fds[0]);
    return n;
}

/*!
 * @brief dsi_close() sends its CloseSession frame on a session with no reply
 *        under way, and closes one with a reply under way without it
 */
int utest_dsi_close_frame_unless_mid_reply(AFPObj *obj _U_,
                                           struct vol *vol _U_)
{
    uint8_t want[DSI_BLOCKSIZ];
    uint8_t got[DSI_BLOCKSIZ + 1];
    dsi_test_header(want, DSIFUNC_CLOSE, 0, 0, 0);
    return sf_close(0, got, sizeof(got)) == DSI_BLOCKSIZ
           && memcmp(got, want, sizeof(want)) == 0
           && sf_close(1, got, sizeof(got)) == 0 ? 0 : -1;
}

/*!
 * @brief A resource fork shorter than its recorded length sends what it has
 *        and ends the session with in_write raised, the header having
 *        promised the rest: through sendfile or through the request buffer
 *
 * @returns 0, -1, or TEST_SKIP
 */
static int sf_short_fork_test(AFPObj *obj, struct vol *vol, bool sendfile)
{
    struct adouble *ad;
    struct sf sf;
    int ret = sendfile ? sf_sendfile_setup(&sf, obj, vol, false)
              : sf_buffered_setup(&sf, obj, vol);

    if (ret != 0) {
        return ret;
    }

    ret = -1;

    if ((ad = sf_open(&sf, OPENFORK_RSCS, SF_SMALL)) != NULL) {
        /* a length left stale by another writer's shrink */
        ad->ad_rlen = SF_SMALL + SF_SHORT;
        sf_read(&sf, 0, SF_SMALL + SF_SHORT,
                DSI_BLOCKSIZ + SF_SMALL + SF_SHORT + 1);
        ad->ad_rlen = SF_SMALL;

        if (sf_header_is(&sf, AFP_OK, SF_SMALL + SF_SHORT)
                && sf_payload_from(&sf, 0, SF_SMALL)
                && sf.exit_code == EXITERR_CLNT
                && sf.exit_in_write == 1) {
            ret = 0;
        }
    }

    sf_teardown(&sf);
    return ret;
}

/*!
 * @brief A resource fork shorter than its recorded length sends what it has
 *        and ends the session, the header having promised the rest
 */
int utest_sendfile_short_fork_ends_session(AFPObj *obj, struct vol *vol)
{
    return sf_short_fork_test(obj, vol, true);
}

/*!
 * @brief A buffered reply the resource fork cuts short ends the session as
 *        the sendfile reply does, with no attention behind it
 */
int utest_buffered_short_fork_ends_session(AFPObj *obj, struct vol *vol)
{
    return sf_short_fork_test(obj, vol, false);
}

/*! @brief A reply of 2 GiB or more on a 32-bit build keeps the session */
int utest_sendfile_large_reply_keeps_session(AFPObj *obj, struct vol *vol)
{
#ifndef SENDFILE_FLAVOR_LINUX
    /* the fault interposer fakes the Linux call alone */
    return TEST_SKIP;
#else
    struct adouble *ad;
    struct sf sf;
    int ret;

    /* a 64-bit size_t carries the count whole */
    if (sizeof(size_t) > 4) {
        return TEST_SKIP;
    }

    if ((ret = sf_sendfile_setup(&sf, obj, vol, true)) != 0) {
        return ret;
    }

    ret = -1;

    if ((ad = sf_open(&sf, OPENFORK_RSCS, SF_SMALL)) != NULL) {
        /* a reply the interposer reports sent and moves none of: the reader
         * takes the header */
        ad->ad_rlen = (off_t)SF_HUGE;
        fi.sendfile_fake = 1;

        if (sf_read(&sf, 0, SF_HUGE, DSI_BLOCKSIZ) == AFP_OK
                && sf_header_is(&sf, AFP_OK, (uint32_t)SF_HUGE)
                && sf.exit_code == 0) {
            ret = 0;
        }

        fi.sendfile_fake = 0;
        ad->ad_rlen = SF_SMALL;
    }

    sf_teardown(&sf);
    return ret;
#endif
}

/*!
 * @brief An FPReadExt of 8 GiB on a fork of rlen bytes gets the largest reply
 *        DSI frames, with the code want: through sendfile or through the
 *        request buffer
 *
 * @returns 0, -1, or TEST_SKIP
 */
static int sf_read_ext_test(AFPObj *obj, struct vol *vol, bool sendfile,
                            off_t rlen, int want)
{
    struct adouble *ad;
    struct sf sf;
    int ret = sendfile ? sf_sendfile_setup(&sf, obj, vol, false)
              : sf_buffered_setup(&sf, obj, vol);

    if (ret != 0) {
        return ret;
    }

    ret = -1;

    if ((ad = sf_open(&sf, OPENFORK_RSCS, SF_SMALL)) != NULL) {
        /* a recorded length the file lacks: the reader takes the header */
        ad->ad_rlen = rlen;

        if (sf_read(&sf, 0, (uint64_t)1 << 33, DSI_BLOCKSIZ) == want
                && sf_header_is(&sf, want, UINT32_MAX)) {
            ret = 0;
        }

        ad->ad_rlen = SF_SMALL;
    }

    sf_teardown(&sf);
    return ret;
}

/*!
 * @brief FPReadExt of 8 GiB on a fork longer than DSI frames, then on one
 *        UINT32_MAX bytes long, through sendfile or the request buffer: the
 *        first reply stops short with AFP_OK, the second ends at the fork's
 *        end with AFPERR_EOF
 */
static int sf_read_ext_cases(AFPObj *obj, struct vol *vol, bool sendfile)
{
    int ret = sf_read_ext_test(obj, vol, sendfile, ((off_t)1 << 32) + 4096,
                               AFP_OK);
    return ret != 0 ? ret
           : sf_read_ext_test(obj, vol, sendfile, UINT32_MAX, AFPERR_EOF);
}

/*! @brief An FPReadExt of 8 GiB through sendfile gets the largest DSI reply */
int utest_sendfile_read_ext_fits_dsi_length(AFPObj *obj, struct vol *vol)
{
    return sf_read_ext_cases(obj, vol, true);
}

/*!
 * @brief An FPReadExt of 8 GiB through the request buffer gets the largest
 *        DSI reply
 */
int utest_buffered_read_ext_fits_dsi_length(AFPObj *obj, struct vol *vol)
{
    return sf_read_ext_cases(obj, vol, false);
}

/*!
 * @brief A file the kernel will not send from goes out through the request
 *        buffer after one refused call, from the offset asked for, whole and
 *        in step
 */
int utest_sendfile_refused_file_sent_from_buffer(AFPObj *obj, struct vol *vol)
{
#if !defined(SENDFILE_FLAVOR_LINUX) && !defined(SENDFILE_FLAVOR_FREEBSD)
    /* the fault interposer covers these two sendfile() calls */
    return TEST_SKIP;
#else
    struct sf sf;
    int ret;

    if ((ret = sf_sendfile_setup(&sf, obj, vol, true)) != 0) {
        return ret;
    }

    ret = -1;

    if (sf_open(&sf, OPENFORK_DATA, SF_LEN) != NULL) {
        fi.sendfile.armed = 1;
        fi.sendfile.fail_after = 0;
        fi.sendfile.errnum = EINVAL;

        if (sf_read(&sf, SF_ODD, SF_LEN - SF_ODD,
                    DSI_BLOCKSIZ + SF_LEN - SF_ODD + 1) == AFP_OK
                && fi.sendfile.calls == 1
                && sf_reply_whole(&sf, SF_ODD, SF_LEN - SF_ODD)) {
            ret = 0;
        }
    }

    sf_teardown(&sf);
    return ret;
#endif
}

/*! The volume's own icon settings while a test lends it a built-in icon */
struct vi_lent {
    char *legacy;
    unsigned char *icon;
    size_t icon_len;
};

/*!
 * @brief Serve a built-in icon as the virtual icon file at the volume root
 *
 * @returns 0, or -1 when the volume builds no icon
 */
static int vi_lend(struct vol *vol, struct vi_lent *lent)
{
    lent->legacy = vol->v_legacyicon;
    lent->icon = vol->v_icon_rfork;
    lent->icon_len = vol->v_icon_rfork_len;
    vol->v_icon_rfork = NULL;
    vol->v_legacyicon = "globe";
    virtual_icon_init(vol);
    return vol->v_icon_rfork != NULL ? 0 : -1;
}

/*! @brief Free the lent icon and give the volume its own settings back */
static void vi_return(struct vol *vol, const struct vi_lent *lent)
{
    free(vol->v_icon_rfork);
    vol->v_icon_rfork = lent->icon;
    vol->v_icon_rfork_len = lent->icon_len;
    vol->v_legacyicon = lent->legacy;
}

/*!
 * @brief A virtual icon fork holds the one reference to its adouble, which
 *        closing the fork releases
 */
int utest_virtual_icon_fork_frees_adouble(AFPObj *obj, struct vol *vol)
{
    const struct ofork *of;
    struct vi_lent lent;
    uint16_t refnum = 0;
    int ret = -1;

    if (vi_lend(vol, &lent) == 0
            && openfork(obj, vol->v_vid, DIRDID_ROOT, VIRTUAL_ICON_NAME,
                        OPENFORK_RSCS, OPENACC_RD, &refnum) == AFP_OK
            && (of = of_find(refnum)) != NULL
            && of->of_ad->ad_refcount == 1) {
        ret = 0;
    }

    if (refnum != 0) {
        closefork(obj, refnum);
    }

    vi_return(vol, &lent);
    return ret;
}

/*!
 * @brief A virtual icon fork's read over DSI is answered from the reply
 *        buffer, with nothing streamed and no reply under way
 */
int utest_virtual_icon_read_from_reply_buffer(AFPObj *obj, struct vol *vol)
{
    size_t rbuflen = DSI_DATASIZ;
    struct vi_lent lent;
    struct sf sf;
    int ret = -1;

    if (sf_pair_setup(&sf, obj, vol) != 0) {
        return -1;
    }

    if (vi_lend(vol, &lent) == 0
            && openfork(&sf.obj, vol->v_vid, DIRDID_ROOT, VIRTUAL_ICON_NAME,
                        OPENFORK_RSCS, OPENACC_RD, &sf.refnum) == AFP_OK
            && sf_request(&sf, 0, SF_SHORT) == 0
            && afp_read_ext(&sf.obj, (char *)sf.dsi.commands, sf.dsi.cmdlen,
                            (char *)sf.dsi.data, &rbuflen) == AFP_OK
            && rbuflen == SF_SHORT
            && memcmp(sf.dsi.data, vol->v_icon_rfork, SF_SHORT) == 0
            && sf.dsi.in_write == 0
            && !(sf.dsi.flags & DSI_NOREPLY)
            && sf.exit_code == 0) {
        ret = 0;
    }

    sf_teardown(&sf);
    vi_return(vol, &lent);
    return ret;
}

/*!
 * @brief A session that a failed primary reconnect left serving ends when a
 *        read reply fails: the data the file had, then the end of the stream
 */
int utest_sendfile_reconnect_session_ends(AFPObj *obj, struct vol *vol)
{
#ifndef WITH_SENDFILE
    return TEST_SKIP;
#else
    struct timeval wait = { .tv_sec = 10 };
    uint8_t frame[SF_FRAME + DSI_BLOCKSIZ + 1];
    struct adouble *ad;
    struct sf sf;
    pid_t pid = -1;
    int peer = -1;
    int ret = -1;

    if (sf_pair_setup(&sf, obj, vol) != 0) {
        return -1;
    }

    sf.cap = DSI_BLOCKSIZ + SF_SMALL + DSI_BLOCKSIZ;

    if ((ad = sf_open(&sf, OPENFORK_RSCS, SF_SMALL)) != NULL
            && (sf.got = malloc(sf.cap)) != NULL) {
        /* the session inherits a length another writer's shrink left stale */
        ad->ad_rlen = SF_SMALL + SF_SHORT;
        pid = dsi_test_serve(&sf.obj, DSI_RECONINPROG, &peer);

        if (pid == -1 && dsi_test_no_loopback(errno)) {
            ret = TEST_SKIP;
        }

        ad->ad_rlen = SF_SMALL;
    }

    if (pid > 0) {
        /* the read, then a request only a session that goes on answers */
        sf_frame(&sf, frame, 0, SF_SMALL + SF_SHORT);
        dsi_test_frame(frame + SF_FRAME, DSIFUNC_CMD, 2, 0, 1, 0xff);
        close(sf.peer);
        sf.peer = peer;

        if (setsockopt(sf.peer, SOL_SOCKET, SO_RCVTIMEO, &wait,
                       sizeof(wait)) == 0
                && write(sf.peer, frame, sizeof(frame)) == sizeof(frame)) {
            sf_reader(&sf);
        }

        if (dsi_test_reap(pid, sf.peer) == 0
                && sf.eof
                && sf_header_is(&sf, AFP_OK, SF_SMALL + SF_SHORT)
                && sf_payload_from(&sf, 0, SF_SMALL)) {
            ret = 0;
        }

        sf.peer = -1;
    }

    sf_teardown(&sf);
    return ret;
#endif
}

/*!
 * @brief A session that a failed primary reconnect left serving ends at
 *        SIGTERM as any other: its shutdown attention, then its CloseSession
 *        frame
 */
int utest_reconnect_sigterm_sends_attention(AFPObj *obj, struct vol *vol _U_)
{
    uint8_t frame[DSI_BLOCKSIZ + 1];
    uint8_t got[2 * DSI_BLOCKSIZ + 2];
    uint16_t shutdown_be = htons(AFPATTN_SHUTDOWN);
    int peer = -1;
    pid_t pid = dsi_test_serve(obj, DSI_RECONINPROG, &peer);
    int ret = -1;

    if (pid == -1) {
        return dsi_test_no_loopback(errno) ? TEST_SKIP : -1;
    }

    /* a request only a running session answers, its handlers set */
    dsi_test_frame(frame, DSIFUNC_CMD, 1, 0, 1, 0xff);

    if (write(peer, frame, sizeof(frame)) == sizeof(frame)
            && readt(peer, got, DSI_BLOCKSIZ, 1, 10) == DSI_BLOCKSIZ
            && kill(pid, SIGTERM) == 0
            && readt(peer, got, sizeof(got), 1, 10) == sizeof(got)
            && got[0] == DSIFL_REQUEST && got[1] == DSIFUNC_ATTN
            && memcmp(got + DSI_BLOCKSIZ, &shutdown_be,
                      sizeof(shutdown_be)) == 0
            && got[DSI_BLOCKSIZ + 2] == DSIFL_REQUEST
            && got[DSI_BLOCKSIZ + 3] == DSIFUNC_CLOSE) {
        ret = 0;
    }

    return dsi_test_reap(pid, peer) == 0 ? ret : -1;
}

/*!
 * @brief A resource fork the dircache records at a length shorter than the
 *        file's, as another session's write leaves it, is read whole at the
 *        file's length
 */
int utest_rfork_cache_stale_length_read_whole(AFPObj *obj, struct vol *vol)
{
    struct sf sf;
    int ret = -1;

    if (sf_buffered_setup(&sf, obj, vol) != 0) {
        return -1;
    }

    /* the length recorded before another session grew the fork */
    if (sf_open(&sf, OPENFORK_RSCS, SF_SMALL) != NULL
            && sf_reopen_cached(&sf, SF_SHORT) != NULL
            && sf_read(&sf, 0, SF_SMALL, DSI_BLOCKSIZ + SF_SMALL + 1) == AFP_OK
            && sf_reply_whole(&sf, 0, SF_SMALL)) {
        ret = 0;
    }

    sf_teardown(&sf);
    return ret;
}

/*!
 * @brief Read the whole fork with a session quantum of half the request
 *        buffer: does the reply arrive whole, and the buffer past the quantum
 *        keep its fill?
 */
static bool sf_read_within_quantum(struct sf *sf)
{
    uint8_t *past = sf->dsi.commands + SF_HALF;
    sf->dsi.server_quantum = SF_HALF;
    memset(past, 0xee, DSI_DATASIZ - SF_HALF);

    if (sf_read(sf, 0, SF_SMALL, DSI_BLOCKSIZ + SF_SMALL + 1) != AFP_OK
            || !sf_reply_whole(sf, 0, SF_SMALL)) {
        return false;
    }

    for (size_t i = 0; i < DSI_DATASIZ - SF_HALF; i++) {
        if (past[i] != 0xee) {
            return false;
        }
    }

    return true;
}

/*!
 * @brief A buffered read stages its pieces inside the session's quantum of
 *        the request buffer
 */
int utest_buffered_read_stays_in_quantum(AFPObj *obj, struct vol *vol)
{
    struct sf sf;
    int ret = -1;

    if (sf_buffered_setup(&sf, obj, vol) != 0) {
        return -1;
    }

    if (sf_open(&sf, OPENFORK_DATA, SF_SMALL) != NULL
            && sf_read_within_quantum(&sf)) {
        ret = 0;
    }

    sf_teardown(&sf);
    return ret;
}

/*!
 * @brief A read the rfork cache serves stages its pieces inside the session's
 *        quantum of the request buffer
 */
int utest_rfork_cache_read_stays_in_quantum(AFPObj *obj, struct vol *vol)
{
    const struct dir *entry;
    struct sf sf;
    int ret = -1;

    if (sf_buffered_setup(&sf, obj, vol) != 0) {
        return -1;
    }

    if (sf_open(&sf, OPENFORK_RSCS, SF_SMALL) != NULL
            && (entry = sf_reopen_cached(&sf, SF_SMALL)) != NULL
            && sf_read_within_quantum(&sf)
            && entry->dcache_rfork_buf != NULL) {
        ret = 0;
    }

    sf_teardown(&sf);
    return ret;
}

/*!
 * @brief A buffered read whose first piece the fork cuts short replies with
 *        the bytes the fork holds and AFPERR_EOF, and the session goes on
 */
int utest_buffered_short_first_piece_replies(AFPObj *obj, struct vol *vol)
{
    struct adouble *ad;
    struct sf sf;
    int ret = -1;

    if (sf_buffered_setup(&sf, obj, vol) != 0) {
        return -1;
    }

    if ((ad = sf_open(&sf, OPENFORK_RSCS, SF_HALF)) != NULL) {
        /* a length left stale by another writer's shrink, within a quantum */
        ad->ad_rlen = SF_HALF + SF_SHORT;

        if (sf_read(&sf, 0, SF_HALF + SF_SHORT,
                    DSI_BLOCKSIZ + SF_HALF + SF_SHORT + 1) == AFPERR_EOF
                && sf_header_is(&sf, AFPERR_EOF, SF_HALF)
                && sf_payload_from(&sf, 0, SF_HALF)
                && (sf.dsi.flags & DSI_NOREPLY)
                && sf.exit_code == 0 && sf.dsi.in_write == 0) {
            ret = 0;
        }

        ad->ad_rlen = SF_HALF;
    }

    sf_teardown(&sf);
    return ret;
}

/*!
 * @brief A buffered FPRead whose read of the fork fails answers AFPERR_MISC,
 *        with nothing sent
 */
int utest_buffered_read_error_replies_misc(AFPObj *obj, struct vol *vol)
{
    size_t rbuflen = DSI_DATASIZ;
    struct sf sf;
    int ret = -1;

    if (!test_capability(TEST_CAP_FAULT_INJECT, vol)) {
        return TEST_SKIP;
    }

    if (sf_buffered_setup(&sf, obj, vol) != 0) {
        return -1;
    }

    if (sf_open(&sf, OPENFORK_DATA, SF_SMALL) != NULL
            && sf_request(&sf, 0, SF_SHORT) == 0) {
        fi.read.armed = 1;
        fi.read.fail_after = 0;
        fi.read.errnum = EIO;

        if (afp_read_ext(&sf.obj, (char *)sf.dsi.commands, sf.dsi.cmdlen,
                         (char *)sf.dsi.data, &rbuflen) == AFPERR_MISC
                && rbuflen == 0 && fi.read.calls == 1
                && !(sf.dsi.flags & DSI_NOREPLY)
                && sf.dsi.in_write == 0 && sf.exit_code == 0) {
            ret = 0;
        }
    }

    sf_teardown(&sf);
    return ret;
}

/*!
 * @brief An AppleTalk FPRead under strict locking clears the range it
 *        locked, and that range alone
 */
int utest_asp_read_unlocks_its_range(AFPObj *obj, struct vol *vol)
{
#ifdef NO_DDP
    return TEST_SKIP;
#else
    uint8_t req[12] = { AFP_READ };
    uint32_t offset_be = htonl(SF_ODD);
    uint32_t count_be = htonl(SF_SHORT);
    size_t rbuflen = DSI_DATASIZ;
    struct sf sf;
    int ret = -1;

    if (!test_capability(TEST_CAP_FAULT_INJECT, vol)) {
        return TEST_SKIP;
    }

    if (sf_pair_setup(&sf, obj, vol) != 0) {
        return -1;
    }

    if (sf_open(&sf, OPENFORK_DATA, SF_SMALL) != NULL) {
        /* refnum travels raw, as afp_openfork() returns it */
        memcpy(req + 2, &sf.refnum, sizeof(sf.refnum));
        memcpy(req + 4, &offset_be, sizeof(offset_be));
        memcpy(req + 8, &count_be, sizeof(count_be));
        sf.obj.proto = AFPPROTO_ASP;
        sf.obj.options.flags |= OPTION_STRICT_LOCKING;
        fault_inject_reset();
        fi.fcntl_watch = 1;

        if (afp_read(&sf.obj, (char *)req, sizeof(req), (char *)sf.dsi.data,
                     &rbuflen) == AFP_OK
                && rbuflen == SF_SHORT && fi.setlk_calls == 2
                && fi.lock_last_type == F_UNLCK
                && fi.lock_last_start == SF_ODD
                && fi.lock_last_len == SF_SHORT) {
            ret = 0;
        }

        sf.obj.proto = AFPPROTO_DSI;
        sf.obj.options.flags &= ~OPTION_STRICT_LOCKING;
    }

    sf_teardown(&sf);
    return ret;
#endif
}
