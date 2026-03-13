// bridge.cpp
//
// JNI bridge between Kotlin (Android BLE) and libdivecomputer.
//
// Android BLE is async; libdivecomputer expects synchronous I/O.
// Bridge: incoming BLE packets are pushed into a thread-safe queue;
// the custom dc_iostream read() blocks on that queue until data arrives
// or the timeout expires.

#include <jni.h>
#include <android/log.h>

#include <libdivecomputer/context.h>
#include <libdivecomputer/descriptor.h>
#include <libdivecomputer/device.h>
#include <libdivecomputer/parser.h>
#include <libdivecomputer/custom.h>
#include <libdivecomputer/ble.h>

#include <string>
#include <vector>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <functional>
#include <atomic>

#define LOG_TAG "DeepLogBridge"
#define LOGI(...) __android_log_print(ANDROID_LOG_INFO,  LOG_TAG, __VA_ARGS__)
#define LOGE(...) __android_log_print(ANDROID_LOG_ERROR, LOG_TAG, __VA_ARGS__)

// ── Async BLE → sync I/O bridge ──────────────────────────────────────────────

struct BleIo {
    std::mutex              mtx;
    std::condition_variable cv;
    std::queue<std::vector<uint8_t>> rxq;
    std::vector<uint8_t>    leftover;   // partial packet carry
    int                     timeout_ms = 5000;
    std::atomic<bool>       closed{false};

    // Called from JNI when Android BLE delivers a characteristic notification.
    void push(const uint8_t* data, size_t len) {
        std::lock_guard<std::mutex> lock(mtx);
        rxq.push(std::vector<uint8_t>(data, data + len));
        cv.notify_one();
    }

    // Called by libdivecomputer to write bytes (forwarded to Kotlin via callback).
    std::function<bool(const uint8_t*, size_t)> writeFn;

    // Called by libdivecomputer to send a BLE ioctl (characteristic write).
    std::function<bool(uint32_t, const uint8_t*, size_t)> ioctlFn;
};

// One global instance per active download (single-threaded download).
static BleIo* g_bio = nullptr;

// ── dc_custom_cbs_t implementations ──────────────────────────────────────────

static dc_status_t cb_set_timeout(void* ud, int ms) {
    auto* bio = (BleIo*)ud;
    bio->timeout_ms = (ms < 0) ? -1 : ms;
    return DC_STATUS_SUCCESS;
}

static dc_status_t cb_read(void* ud, void* buf, size_t size, size_t* actual) {
    auto* bio = (BleIo*)ud;
    uint8_t* out = (uint8_t*)buf;
    size_t copied = 0;

    while (copied < size) {
        // Drain leftover from previous packet first.
        if (!bio->leftover.empty()) {
            size_t n = std::min(size - copied, bio->leftover.size());
            memcpy(out + copied, bio->leftover.data(), n);
            copied += n;
            bio->leftover.erase(bio->leftover.begin(), bio->leftover.begin() + n);
            continue;
        }

        // Wait for next packet.
        std::unique_lock<std::mutex> lock(bio->mtx);
        bool ok;
        if (bio->timeout_ms < 0) {
            bio->cv.wait(lock, [bio]{ return !bio->rxq.empty() || bio->closed.load(); });
            ok = !bio->rxq.empty();
        } else {
            ok = bio->cv.wait_for(lock,
                std::chrono::milliseconds(bio->timeout_ms),
                [bio]{ return !bio->rxq.empty() || bio->closed.load(); });
        }
        if (!ok || bio->rxq.empty()) {
            if (copied == 0) return DC_STATUS_TIMEOUT;
            break;
        }
        bio->leftover = std::move(bio->rxq.front());
        bio->rxq.pop();
    }

    if (actual) *actual = copied;
    return (copied > 0) ? DC_STATUS_SUCCESS : DC_STATUS_IO;
}

static dc_status_t cb_write(void* ud, const void* buf, size_t size, size_t* actual) {
    auto* bio = (BleIo*)ud;
    bool ok = bio->writeFn((const uint8_t*)buf, size);
    if (actual) *actual = ok ? size : 0;
    return ok ? DC_STATUS_SUCCESS : DC_STATUS_IO;
}

static dc_status_t cb_ioctl(void* ud, unsigned int req, void* data, size_t size) {
    auto* bio = (BleIo*)ud;
    if (bio->ioctlFn) {
        bio->ioctlFn(req, (const uint8_t*)data, size);
    }
    return DC_STATUS_SUCCESS;
}

static dc_status_t cb_flush(void* ud) { return DC_STATUS_SUCCESS; }
static dc_status_t cb_purge(void* ud, dc_direction_t dir) { return DC_STATUS_SUCCESS; }
static dc_status_t cb_sleep(void* ud, unsigned int ms) {
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
    return DC_STATUS_SUCCESS;
}
static dc_status_t cb_close(void* ud) {
    auto* bio = (BleIo*)ud;
    bio->closed = true;
    bio->cv.notify_all();
    return DC_STATUS_SUCCESS;
}

static const dc_custom_cbs_t k_cbs = {
    .set_timeout  = cb_set_timeout,
    .set_break    = nullptr,
    .set_dtr      = nullptr,
    .set_rts      = nullptr,
    .get_lines    = nullptr,
    .get_available = nullptr,
    .configure    = nullptr,
    .poll         = nullptr,
    .read         = cb_read,
    .write        = cb_write,
    .ioctl        = cb_ioctl,
    .flush        = cb_flush,
    .purge        = cb_purge,
    .sleep        = cb_sleep,
    .close        = cb_close,
};

// ── Dive data collection ──────────────────────────────────────────────────────

struct Sample {
    uint32_t time_ms = 0;
    double   depth   = 0.0;
    double   temp    = 0.0;
};

struct Dive {
    dc_datetime_t   when{};
    double          maxdepth  = 0.0;
    uint32_t        duration  = 0;   // seconds
    std::vector<Sample> samples;
};

static void sample_cb(dc_sample_type_t type, const dc_sample_value_t* val, void* ud) {
    auto* dive = (Dive*)ud;
    Sample& last = dive->samples.empty()
        ? dive->samples.emplace_back()
        : dive->samples.back();
    switch (type) {
        case DC_SAMPLE_TIME:
            if (!dive->samples.empty() && last.time_ms > 0)
                dive->samples.emplace_back();
            dive->samples.back().time_ms = val->time;
            break;
        case DC_SAMPLE_DEPTH:
            dive->samples.back().depth = val->depth;
            if (val->depth > dive->maxdepth) dive->maxdepth = val->depth;
            break;
        case DC_SAMPLE_TEMPERATURE:
            dive->samples.back().temp = val->temperature;
            break;
        default:
            break;
    }
}

struct DownloadCtx {
    dc_context_t*    ctx        = nullptr;
    dc_descriptor_t* descriptor = nullptr;
    std::vector<Dive> dives;
    // Progress callback → Kotlin
    std::function<void(int current, int total)> progressFn;
    // Status text → Kotlin
    std::function<void(const std::string&)>     statusFn;
};

static int dive_cb(const unsigned char* data, unsigned int size,
                   const unsigned char* fprint, unsigned int fsize, void* ud)
{
    auto* dc = (DownloadCtx*)ud;
    Dive dive;

    dc_parser_t* parser = nullptr;
    dc_status_t rc = dc_parser_new(&parser, nullptr, data, size);
    if (rc != DC_STATUS_SUCCESS) {
        // Try with context + descriptor
        rc = dc_parser_new2(&parser, dc->ctx, dc->descriptor, data, size);
    }
    if (rc != DC_STATUS_SUCCESS || !parser) {
        LOGE("dc_parser_new failed: %d", rc);
        dc->dives.push_back(std::move(dive));
        return 1;
    }

    dc_parser_get_datetime(parser, &dive.when);

    double val = 0;
    if (dc_parser_get_field(parser, DC_FIELD_MAXDEPTH, 0, &val) == DC_STATUS_SUCCESS)
        dive.maxdepth = val;

    uint32_t dur = 0;
    if (dc_parser_get_field(parser, DC_FIELD_DIVETIME, 0, &dur) == DC_STATUS_SUCCESS)
        dive.duration = dur;

    dc_parser_samples_foreach(parser, sample_cb, &dive);
    dc_parser_destroy(parser);

    char buf[128];
    snprintf(buf, sizeof(buf),
        "Dive %02d-%02d-%04d  %.1fm  %um  %zu samples",
        dive.when.day, dive.when.month, dive.when.year,
        dive.maxdepth, dive.duration,
        dive.samples.size());
    if (dc->statusFn) dc->statusFn(buf);

    dc->dives.push_back(std::move(dive));
    return 1; // continue
}

static void event_cb(dc_device_t*, dc_event_type_t ev,
                     const void* data, void* ud)
{
    auto* dc = (DownloadCtx*)ud;
    if (ev == DC_EVENT_PROGRESS) {
        auto* p = (const dc_event_progress_t*)data;
        if (dc->progressFn && p->maximum > 0)
            dc->progressFn(p->current, p->maximum);
    } else if (ev == DC_EVENT_DEVINFO) {
        auto* d = (const dc_event_devinfo_t*)data;
        char buf[64];
        snprintf(buf, sizeof(buf), "Device model=%u fw=%u serial=%u",
            d->model, d->firmware, d->serial);
        if (dc->statusFn) dc->statusFn(buf);
    }
}

// ── JNI interface ─────────────────────────────────────────────────────────────
//
// Called from Kotlin:
//   DcBridge.onBleData(bytes)          — deliver incoming BLE packet
//   DcBridge.download(deviceName, transport) — blocking; returns JSON string

extern "C" {

JNIEXPORT void JNICALL
Java_fi_deeplog_bridge_DcBridge_onBleData(JNIEnv* env, jclass, jbyteArray arr)
{
    if (!g_bio) return;
    jsize len = env->GetArrayLength(arr);
    jbyte* raw = env->GetByteArrayElements(arr, nullptr);
    g_bio->push((const uint8_t*)raw, (size_t)len);
    env->ReleaseByteArrayElements(arr, raw, JNI_ABORT);
}

// writeCb and ioctlCb are Java method IDs set up during download().
static JavaVM*     g_jvm       = nullptr;
static jobject     g_bleObj    = nullptr; // BleTransport instance
static jmethodID   g_writeId   = nullptr;
static jmethodID   g_ioctlId   = nullptr;

JNIEXPORT jstring JNICALL
Java_fi_deeplog_bridge_DcBridge_download(
    JNIEnv* env, jclass,
    jstring jDeviceName,
    jint    jtransport,   // DC_TRANSPORT_BLE = 5
    jobject bleObj)       // BleTransport instance (write/ioctl callbacks)
{
    env->GetJavaVM(&g_jvm);
    g_bleObj  = env->NewGlobalRef(bleObj);
    jclass bleCls = env->GetObjectClass(bleObj);
    g_writeId = env->GetMethodID(bleCls, "write", "([B)Z");
    g_ioctlId = env->GetMethodID(bleCls, "ioctl", "(I[B)V");

    const char* devName = env->GetStringUTFChars(jDeviceName, nullptr);

    // Find descriptor matching device name.
    dc_context_t* ctx = nullptr;
    dc_context_new(&ctx);

    dc_iterator_t* iter = nullptr;
    dc_descriptor_iterator_new(&iter, ctx);

    dc_descriptor_t* descriptor = nullptr;
    dc_descriptor_t* cur = nullptr;
    while (dc_iterator_next(iter, &cur) == DC_STATUS_SUCCESS) {
        const char* vendor  = dc_descriptor_get_vendor(cur);
        const char* product = dc_descriptor_get_product(cur);
        char fullname[128];
        snprintf(fullname, sizeof(fullname), "%s %s", vendor ? vendor : "", product ? product : "");
        // Match: device name contains product name or full name
        if ((product && strstr(devName, product)) ||
            strstr(devName, fullname))
        {
            descriptor = cur;
            break;
        }
        dc_descriptor_free(cur);
    }
    dc_iterator_free(iter);

    if (!descriptor) {
        // No exact match — try family from transport type; just pick first BLE-capable
        dc_iterator_t* iter2 = nullptr;
        dc_descriptor_iterator_new(&iter2, ctx);
        while (dc_iterator_next(iter2, &cur) == DC_STATUS_SUCCESS) {
            unsigned int transports = dc_descriptor_get_transports(cur);
            if (transports & (1u << jtransport)) {
                descriptor = cur;
                break;
            }
            dc_descriptor_free(cur);
        }
        dc_iterator_free(iter2);
    }

    env->ReleaseStringUTFChars(jDeviceName, devName);

    if (!descriptor) {
        dc_context_free(ctx);
        env->DeleteGlobalRef(g_bleObj);
        return env->NewStringUTF("{\"error\":\"Device not found in libdivecomputer descriptors\"}");
    }

    // Set up BleIo.
    BleIo bio;
    g_bio = &bio;

    bio.writeFn = [](const uint8_t* data, size_t len) -> bool {
        JNIEnv* e = nullptr;
        g_jvm->AttachCurrentThread(&e, nullptr);
        jbyteArray arr = e->NewByteArray((jsize)len);
        e->SetByteArrayRegion(arr, 0, (jsize)len, (const jbyte*)data);
        jboolean ok = e->CallBooleanMethod(g_bleObj, g_writeId, arr);
        e->DeleteLocalRef(arr);
        return (bool)ok;
    };

    bio.ioctlFn = [](uint32_t req, const uint8_t* data, size_t len) -> bool {
        JNIEnv* e = nullptr;
        g_jvm->AttachCurrentThread(&e, nullptr);
        jbyteArray arr = e->NewByteArray((jsize)len);
        e->SetByteArrayRegion(arr, 0, (jsize)len, (const jbyte*)data);
        e->CallVoidMethod(g_bleObj, g_ioctlId, (jint)req, arr);
        e->DeleteLocalRef(arr);
        return true;
    };

    // Open custom iostream.
    dc_iostream_t* stream = nullptr;
    dc_status_t rc = dc_custom_open(&stream, ctx,
        (dc_transport_t)jtransport, &k_cbs, &bio);

    if (rc != DC_STATUS_SUCCESS) {
        dc_descriptor_free(descriptor);
        dc_context_free(ctx);
        g_bio = nullptr;
        env->DeleteGlobalRef(g_bleObj);
        return env->NewStringUTF("{\"error\":\"dc_custom_open failed\"}");
    }

    // Open device.
    dc_device_t* device = nullptr;
    rc = dc_device_open(&device, ctx, descriptor, stream);
    if (rc != DC_STATUS_SUCCESS) {
        LOGE("dc_device_open failed: %d", rc);
        dc_iostream_close(stream);
        dc_descriptor_free(descriptor);
        dc_context_free(ctx);
        g_bio = nullptr;
        env->DeleteGlobalRef(g_bleObj);
        char err[64];
        snprintf(err, sizeof(err), "{\"error\":\"dc_device_open failed: %d\"}", rc);
        return env->NewStringUTF(err);
    }

    // Set up download context with callbacks into Kotlin.
    DownloadCtx dc;
    dc.ctx        = ctx;
    dc.descriptor = descriptor;

    jclass    mainCls    = env->FindClass("fi/deeplog/bridge/MainActivity");
    jmethodID progressId = env->GetStaticMethodID(mainCls, "onProgress", "(II)V");
    jmethodID statusId   = env->GetStaticMethodID(mainCls, "onStatus", "(Ljava/lang/String;)V");

    dc.progressFn = [&](int cur, int tot) {
        JNIEnv* e = nullptr;
        g_jvm->AttachCurrentThread(&e, nullptr);
        e->CallStaticVoidMethod(mainCls, progressId, cur, tot);
    };
    dc.statusFn = [&](const std::string& msg) {
        JNIEnv* e = nullptr;
        g_jvm->AttachCurrentThread(&e, nullptr);
        jstring s = e->NewStringUTF(msg.c_str());
        e->CallStaticVoidMethod(mainCls, statusId, s);
        e->DeleteLocalRef(s);
    };

    dc_device_set_events(device,
        DC_EVENT_PROGRESS | DC_EVENT_DEVINFO | DC_EVENT_CLOCK,
        event_cb, &dc);

    rc = dc_device_foreach(device, dive_cb, &dc);
    LOGI("dc_device_foreach returned %d, %zu dives", rc, dc.dives.size());

    dc_device_close(device);
    dc_iostream_close(stream);
    dc_descriptor_free(descriptor);
    dc_context_free(ctx);
    g_bio = nullptr;
    env->DeleteGlobalRef(g_bleObj);

    // Build JSON result.
    std::string json = "[";
    for (size_t i = 0; i < dc.dives.size(); i++) {
        const Dive& d = dc.dives[i];
        char buf[512];
        snprintf(buf, sizeof(buf),
            "%s{\"date\":\"%04d-%02d-%02d\","
            "\"time\":\"%02d:%02d\","
            "\"maxdepth\":%.1f,"
            "\"duration\":%u,"
            "\"samples\":%zu}",
            i ? "," : "",
            d.when.year, d.when.month, d.when.day,
            d.when.hour, d.when.minute,
            d.maxdepth, d.duration,
            d.samples.size());
        json += buf;
    }
    json += "]";

    return env->NewStringUTF(json.c_str());
}

} // extern "C"
