/*
 * Drives the Shearwater Petrel download loop (shearwater_petrel.c) against a
 * scripted fake computer, with no Bluetooth and no real Perdix.
 *
 * shearwater_petrel.c reaches the device only through the shearwater_common_*
 * functions, so this file supplies its own versions of those: a fake that
 * serves manifest pages and dive blobs from a script, and can be told to
 * refuse (NAK) a request or to time out. Everything else — the manifest walk,
 * the fingerprint stop, deleted records, error handling — is the real driver.
 *
 * run.sh builds it twice: against DeepLog's fixed driver, which must pass
 * everything, and against the untouched upstream driver, which must fail the
 * scenarios the fixes are for. That second build is what shows each test
 * really detects the bug it names, rather than passing by construction.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <libdivecomputer/context.h>
#include <libdivecomputer/device.h>
#include <libdivecomputer/buffer.h>

#include "shearwater_common.h"
#include "shearwater_petrel.h"

#define MANIFEST_ADDR 0xE0000000u
#define MANIFEST_SIZE 0x600
#define RECORD_SIZE   0x20
#define RECORD_COUNT  (MANIFEST_SIZE / RECORD_SIZE)
#define BASE_ADDR     0x80000000u      /* Petrel Native Format */

/* ── The fake computer ──────────────────────────────────────────────────── */

enum { OK = 0, NAK_IT, TIMEOUT_IT };

typedef struct {
	int deleted;            /* a record the computer marked deleted          */
	int fate;               /* what happens when this dive is downloaded     */
} fake_dive_t;

static fake_dive_t g_dives[256];
static int g_ndives;                   /* newest first, as the manifest lists */
static int g_page_served;              /* manifest pages handed out so far    */
static int g_nak_extra_page;           /* refuse the page after the last one  */
static int g_downloaded[256];          /* dive index -> times served          */

/* The 4-byte fingerprint of dive i: its position, so it is easy to read. */
static void fingerprint_of (int i, unsigned char out[4])
{
	out[0] = 0xD1; out[1] = 0x7E; out[2] = (unsigned char) (i >> 8); out[3] = (unsigned char) i;
}

static void reset (void)
{
	memset (g_dives, 0, sizeof g_dives);
	memset (g_downloaded, 0, sizeof g_downloaded);
	g_ndives = 0; g_page_served = 0; g_nak_extra_page = 0;
}

static void put_be32 (unsigned char *p, unsigned int v)
{
	p[0] = (unsigned char) (v >> 24); p[1] = (unsigned char) (v >> 16);
	p[2] = (unsigned char) (v >> 8);  p[3] = (unsigned char) v;
}

/* One manifest page: up to RECORD_COUNT records, then a terminator. */
static dc_status_t serve_manifest (dc_buffer_t *buffer)
{
	int first = g_page_served * RECORD_COUNT;
	if (first >= g_ndives) {
		if (g_nak_extra_page) return DC_STATUS_UNSUPPORTED;   /* the NAK */
		/* Otherwise an empty page: no valid header anywhere. */
	}

	unsigned char page[MANIFEST_SIZE];
	memset (page, 0xFF, sizeof page);
	for (int r = 0; r < RECORD_COUNT && first + r < g_ndives; ++r) {
		int i = first + r;
		unsigned char *rec = page + r * RECORD_SIZE;
		memset (rec, 0, RECORD_SIZE);
		rec[0] = g_dives[i].deleted ? 0x5A : 0xA5;
		rec[1] = g_dives[i].deleted ? 0x23 : 0xC4;
		fingerprint_of (i, rec + 4);
		put_be32 (rec + 20, (unsigned int) (0x1000 * (i + 1)));  /* dive address */
	}
	g_page_served++;
	dc_buffer_clear (buffer);
	dc_buffer_append (buffer, page, sizeof page);
	return DC_STATUS_SUCCESS;
}

static dc_status_t serve_dive (dc_buffer_t *buffer, unsigned int address)
{
	int i = (int) ((address - BASE_ADDR) / 0x1000) - 1;
	if (i < 0 || i >= g_ndives) return DC_STATUS_PROTOCOL;

	if (g_dives[i].fate == NAK_IT) return DC_STATUS_UNSUPPORTED;
	if (g_dives[i].fate == TIMEOUT_IT) return DC_STATUS_TIMEOUT;

	/* A dive blob carries its fingerprint at offset 12. */
	unsigned char blob[64];
	memset (blob, 0, sizeof blob);
	fingerprint_of (i, blob + 12);
	g_downloaded[i]++;
	dc_buffer_clear (buffer);
	dc_buffer_append (buffer, blob, sizeof blob);
	return DC_STATUS_SUCCESS;
}

/* ── The shearwater_common_* API the driver expects ─────────────────────── */

dc_status_t
shearwater_common_setup (shearwater_common_device_t *device, dc_context_t *context, dc_iostream_t *iostream)
{
	(void) context;
	device->iostream = iostream;
	return DC_STATUS_SUCCESS;
}

dc_status_t
shearwater_common_transfer (shearwater_common_device_t *device, const unsigned char input[],
                            unsigned int isize, unsigned char output[], unsigned int osize,
                            unsigned int *actual)
{
	(void) device; (void) input; (void) isize; (void) output; (void) osize;
	if (actual) *actual = 0;
	return DC_STATUS_SUCCESS;
}

dc_status_t
shearwater_common_download (shearwater_common_device_t *device, dc_buffer_t *buffer,
                            unsigned int address, unsigned int size, unsigned int compression,
                            dc_event_progress_t *progress)
{
	(void) device; (void) size; (void) compression; (void) progress;
	if (address == MANIFEST_ADDR) return serve_manifest (buffer);
	return serve_dive (buffer, address);
}

dc_status_t
shearwater_common_rdbi (shearwater_common_device_t *device, unsigned int id,
                        unsigned char data[], unsigned int size, unsigned int *actual)
{
	(void) device;
	memset (data, 0, size);
	switch (id) {
	case ID_SERIAL:    memcpy (data, "12AB34CD", size < 8 ? size : 8); break;
	case ID_FIRMWARE:  memcpy (data, "F99", size < 3 ? size : 3); if (actual) *actual = 3; break;
	case ID_HARDWARE:  data[0] = 0x07; data[1] = 0x06; break;
	case ID_LOGUPLOAD: put_be32 (data + 1, BASE_ADDR); break;
	}
	return DC_STATUS_SUCCESS;
}

dc_status_t
shearwater_common_wdbi (shearwater_common_device_t *device, unsigned int id,
                        const unsigned char data[], unsigned int size)
{ (void) device; (void) id; (void) data; (void) size; return DC_STATUS_SUCCESS; }

dc_status_t
shearwater_common_timesync_local (shearwater_common_device_t *device, const dc_datetime_t *datetime)
{ (void) device; (void) datetime; return DC_STATUS_SUCCESS; }

dc_status_t
shearwater_common_timesync_utc (shearwater_common_device_t *device, const dc_datetime_t *datetime)
{ (void) device; (void) datetime; return DC_STATUS_SUCCESS; }

unsigned int
shearwater_common_get_model (shearwater_common_device_t *device, unsigned int hardware)
{ (void) device; (void) hardware; return PERDIX; }

/* ── Running a download ─────────────────────────────────────────────────── */

typedef struct { int got[256]; int n; } delivered_t;

static int on_dive (const unsigned char *data, unsigned int size,
                    const unsigned char *fp, unsigned int fsize, void *ud)
{
	(void) data; (void) size; (void) fsize;
	delivered_t *d = ud;
	d->got[d->n++] = (fp[2] << 8) | fp[3];
	return 1;
}

static dc_status_t run (delivered_t *out, const int *fingerprint_of_dive)
{
	dc_context_t *ctx = NULL;
	dc_context_new (&ctx);
	dc_context_set_loglevel (ctx, DC_LOGLEVEL_NONE);

	dc_device_t *dev = NULL;
	dc_status_t rc = shearwater_petrel_device_open (&dev, ctx, NULL);
	if (rc != DC_STATUS_SUCCESS) { dc_context_free (ctx); return rc; }

	if (fingerprint_of_dive) {
		unsigned char fp[4];
		fingerprint_of (*fingerprint_of_dive, fp);
		dc_device_set_fingerprint (dev, fp, sizeof fp);
	}

	memset (out, 0, sizeof *out);
	rc = dc_device_foreach (dev, on_dive, out);
	dc_device_close (dev);
	dc_context_free (ctx);
	return rc;
}

/* ── Scenarios ──────────────────────────────────────────────────────────── */

static int failures = 0;

static void check (int ok, const char *name, const char *detail)
{
	printf ("%s  %s%s%s\n", ok ? "ok  " : "FAIL", name, ok ? "" : "   << ", ok ? "" : detail);
	if (!ok) failures++;
}

/* True when exactly the listed dives came back, in manifest order. */
static int got_exactly (const delivered_t *d, const int *want, int n, char *why, size_t len)
{
	int ok = d->n == n;
	for (int k = 0; ok && k < n; ++k) ok = d->got[k] == want[k];
	int w = snprintf (why, len, "got [");
	for (int k = 0; k < d->n && w < (int) len; ++k)
		w += snprintf (why + w, len - (size_t) w, "%s%d", k ? " " : "", d->got[k]);
	snprintf (why + w, len - (size_t) w, "], %d dive(s)", d->n);
	return ok;
}

int main (void)
{
	delivered_t d;
	char why[512];
	dc_status_t rc;

	/* 1. Nothing unusual: every dive comes back. */
	reset (); g_ndives = 5;
	rc = run (&d, NULL);
	{ int want[] = {0,1,2,3,4};
	  int same = got_exactly (&d, want, 5, why, sizeof why);
	  check (rc == DC_STATUS_SUCCESS && same,
	         "plain log: all 5 dives", why); }

	/* 2. A dive deleted on the computer must not cost a real one. */
	reset (); g_ndives = 5; g_dives[1].deleted = 1;
	rc = run (&d, NULL);
	{ int want[] = {0,2,3,4};
	  int same = got_exactly (&d, want, 4, why, sizeof why);
	  check (rc == DC_STATUS_SUCCESS && same,
	         "one deleted record: all 4 real dives, none dropped", why); }

	/* 3. Several deleted records: each used to drop one more real dive. */
	reset (); g_ndives = 10;
	g_dives[0].deleted = g_dives[3].deleted = g_dives[6].deleted = 1;
	rc = run (&d, NULL);
	{ int want[] = {1,2,4,5,7,8,9};
	  int same = got_exactly (&d, want, 7, why, sizeof why);
	  check (rc == DC_STATUS_SUCCESS && same,
	         "three deleted records: all 7 real dives", why); }

	/* 4. A log that ends exactly on a page boundary. */
	reset (); g_ndives = RECORD_COUNT; g_nak_extra_page = 1;
	rc = run (&d, NULL);
	snprintf (why, sizeof why, "rc=%d, %d dive(s)", rc, d.n);
	check (rc == DC_STATUS_SUCCESS && d.n == RECORD_COUNT,
	       "log ends on a page boundary: all 48 dives, not an error", why);

	/* 5. Two full pages, refusal after the second. */
	reset (); g_ndives = 2 * RECORD_COUNT; g_nak_extra_page = 1;
	rc = run (&d, NULL);
	snprintf (why, sizeof why, "rc=%d, %d dive(s)", rc, d.n);
	check (rc == DC_STATUS_SUCCESS && d.n == 2 * RECORD_COUNT,
	       "log ends on the second page boundary: all 96 dives", why);

	/* 6. One dive the computer refuses must not hide the older ones. */
	reset (); g_ndives = 6; g_dives[2].fate = NAK_IT;
	rc = run (&d, NULL);
	{ int want[] = {0,1,3,4,5};
	  int same = got_exactly (&d, want, 5, why, sizeof why);
	  check (rc == DC_STATUS_SUCCESS && same,
	         "one refused dive: skipped, the 3 older dives still come", why); }

	/* 7. A link failure still stops the download, and reports it. */
	reset (); g_ndives = 6; g_dives[2].fate = TIMEOUT_IT;
	rc = run (&d, NULL);
	snprintf (why, sizeof why, "rc=%d, %d dive(s)", rc, d.n);
	check (rc == DC_STATUS_TIMEOUT && d.n == 2,
	       "timeout mid-download: stops and reports failure (not success)", why);

	/* 8. Everything refused is systemic, not a clean empty download. */
	reset (); g_ndives = 3;
	g_dives[0].fate = g_dives[1].fate = g_dives[2].fate = NAK_IT;
	rc = run (&d, NULL);
	snprintf (why, sizeof why, "rc=%d, %d dive(s)", rc, d.n);
	check (rc != DC_STATUS_SUCCESS && d.n == 0,
	       "every dive refused: reported as a failure, not success", why);

	/* 9. The fingerprint still stops the walk at the last-downloaded dive. */
	reset (); g_ndives = 8;
	{ int fp = 3; rc = run (&d, &fp); }
	{ int want[] = {0,1,2};
	  int same = got_exactly (&d, want, 3, why, sizeof why);
	  check (rc == DC_STATUS_SUCCESS && same,
	         "fingerprint: only dives newer than the last download", why); }

	/* 10. Fingerprint and deleted records together. */
	reset (); g_ndives = 8; g_dives[1].deleted = 1;
	{ int fp = 4; rc = run (&d, &fp); }
	{ int want[] = {0,2,3};
	  int same = got_exactly (&d, want, 3, why, sizeof why);
	  check (rc == DC_STATUS_SUCCESS && same,
	         "fingerprint with a deleted record ahead of it", why); }

	/* 11. A full page made full by a deleted record, then a second page. */
	reset (); g_ndives = RECORD_COUNT + 3; g_dives[5].deleted = 1;
	rc = run (&d, NULL);
	snprintf (why, sizeof why, "rc=%d, %d dive(s)", rc, d.n);
	check (rc == DC_STATUS_SUCCESS && d.n == RECORD_COUNT + 2,
	       "deleted record on a full first page: the second page is still read", why);

	/* No dive may be downloaded twice in one pass. */
	int twice = 0;
	for (int i = 0; i < 256; ++i) if (g_downloaded[i] > 1) twice++;
	check (twice == 0, "no dive fetched twice in a pass", "a dive was downloaded twice");

	printf ("\n%s\n", failures ? "FAILED" : "all passed");
	return failures ? 1 : 0;
}
