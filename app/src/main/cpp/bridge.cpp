// bridge.cpp — JNI bridge: libdivecomputer ↔ Android BLE

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
#include <thread>
#include <functional>
#include <atomic>
#include <sstream>
#include <iomanip>
#include <cctype>
#include <cstring>
#include <strings.h>

#define LOG_TAG "DeepLogBridge"
#define LOGI(...) __android_log_print(ANDROID_LOG_INFO,  LOG_TAG, __VA_ARGS__)
#define LOGE(...) __android_log_print(ANDROID_LOG_ERROR, LOG_TAG, __VA_ARGS__)

// ── Async BLE → sync I/O queue ────────────────────────────────────────────────

struct BleIo {
    std::mutex              mtx;
    std::condition_variable cv;
    std::queue<std::vector<uint8_t>> rxq;
    std::vector<uint8_t>    leftover;
    int                     timeout_ms = 5000;
    std::atomic<bool>       closed{false};
    std::function<bool(const uint8_t*, size_t)> writeFn;

    void push(const uint8_t* data, size_t len) {
        char hex[64] = {};
        for (size_t i = 0; i < len && i < 8; i++)
            snprintf(hex + i*3, 4, "%02x ", data[i]);
        __android_log_print(ANDROID_LOG_VERBOSE, LOG_TAG, "BLE rx %zuB: %s%s",
            len, hex, len > 8 ? "..." : "");
        std::lock_guard<std::mutex> lock(mtx);
        rxq.push(std::vector<uint8_t>(data, data + len));
        cv.notify_one();
    }
};

static BleIo* g_bio = nullptr;

// ── dc_custom_cbs_t callbacks ─────────────────────────────────────────────────

static dc_status_t cb_set_timeout(void* ud, int ms) {
    ((BleIo*)ud)->timeout_ms = (ms < 0) ? -1 : ms;
    return DC_STATUS_SUCCESS;
}

static dc_status_t cb_read(void* ud, void* buf, size_t size, size_t* actual) {
    auto* bio = (BleIo*)ud;
    uint8_t* out = (uint8_t*)buf;

    // For BLE, libdivecomputer calls read(buf, 256) expecting ONE complete
    // BLE notification per call. It strips a 2-byte packet header then
    // SLIP-decodes the rest. Accumulating across notifications corrupts
    // framing because each notification has its own header. So we return
    // exactly one notification per call, storing any overflow in leftover.
    //
    // For serial (size==1 calls), the leftover path handles byte-at-a-time.

    // Drain leftover from the previous notification first.
    if (!bio->leftover.empty()) {
        size_t n = std::min(size, bio->leftover.size());
        memcpy(out, bio->leftover.data(), n);
        bio->leftover.erase(bio->leftover.begin(), bio->leftover.begin() + n);
        if (actual) *actual = n;
        return DC_STATUS_SUCCESS;
    }

    // Wait for the next BLE notification.
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
    if (!ok || bio->rxq.empty()) return DC_STATUS_TIMEOUT;

    auto pkt = std::move(bio->rxq.front());
    bio->rxq.pop();
    lock.unlock();

    size_t n = std::min(size, pkt.size());
    memcpy(out, pkt.data(), n);
    if (pkt.size() > n)
        bio->leftover.assign(pkt.begin() + n, pkt.end());

    if (actual) *actual = n;
    return DC_STATUS_SUCCESS;
}

static dc_status_t cb_write(void* ud, const void* buf, size_t size, size_t* actual) {
    auto* bio = (BleIo*)ud;
    bool ok = bio->writeFn((const uint8_t*)buf, size);
    if (actual) *actual = ok ? size : 0;
    return ok ? DC_STATUS_SUCCESS : DC_STATUS_IO;
}

static dc_status_t cb_flush(void* ud) { return DC_STATUS_SUCCESS; }
static dc_status_t cb_purge(void* ud, dc_direction_t) { return DC_STATUS_SUCCESS; }
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
    .read         = cb_read,
    .write        = cb_write,
    .flush        = cb_flush,
    .purge        = cb_purge,
    .sleep        = cb_sleep,
    .close        = cb_close,
};

// ── Dive / sample data ────────────────────────────────────────────────────────

struct GasMix {
    double oxygen  = 0.0;
    double helium  = 0.0;
    double nitrogen= 0.0;
    int    usage   = 0;
};

struct Tank {
    int    gasmix        = -1;
    double volume        = 0.0;  // L
    double workpressure  = 0.0;  // bar
    double beginpressure = 0.0;  // bar
    double endpressure   = 0.0;  // bar
    int    usage         = 0;
};

struct Ppo2Reading {
    int    sensor = -1;
    double value  = 0.0;
};

struct Sample {
    uint32_t time_ms   = 0;
    double   depth     = 0.0;
    double   temp      = 0.0;
    // pressure per tank
    int      pressure_tank  = -1;
    double   pressure_value = 0.0;
    // CNS %
    double   cns       = 0.0;
    // deco
    int      deco_type  = -1;  // 0=ndl 1=safetystop 2=decostop 3=deepstop
    double   deco_depth = 0.0; // m
    uint32_t deco_time  = 0;   // s
    // misc
    uint32_t rbt       = 0;    // remaining bottom time (s)
    uint32_t heartbeat = 0;    // bpm
    uint32_t bearing   = 0;    // degrees
    double   setpoint  = 0.0;  // bar
    std::vector<Ppo2Reading> ppo2;
    int      gasmix    = -1;   // active gasmix index
};

struct Dive {
    dc_datetime_t       when{};
    double              maxdepth = 0.0;
    double              avgdepth = 0.0;
    uint32_t            duration = 0;
    double              temp_surface = -999.0;
    double              temp_min     = -999.0;
    double              temp_max     = -999.0;
    double              atmospheric  = 0.0;  // bar
    int                 salinity_type = -1;  // 0=fresh 1=salt
    double              salinity_density = 0.0;
    int                 divemode = -1; // 0=OC 1=CCR 2=SCR 3=freedive 4=gauge
    std::vector<GasMix> gasmixes;
    std::vector<Tank>   tanks;
    std::vector<Sample> samples;
};

static const char* divemodeStr(int m) {
    switch (m) {
        case 0: return "OC";
        case 1: return "CCR";
        case 2: return "SCR";
        case 3: return "Freedive";
        case 4: return "Gauge";
        default: return "Unknown";
    }
}

static void sample_cb(dc_sample_type_t type, const dc_sample_value_t* val, void* ud) {
    auto* dive = (Dive*)ud;
    if (dive->samples.empty() || (type == DC_SAMPLE_TIME && dive->samples.back().time_ms > 0))
        dive->samples.emplace_back();
    Sample& s = dive->samples.back();

    switch (type) {
        case DC_SAMPLE_TIME:
            s.time_ms = val->time;
            break;
        case DC_SAMPLE_DEPTH:
            s.depth = val->depth;
            if (val->depth > dive->maxdepth) dive->maxdepth = val->depth;
            break;
        case DC_SAMPLE_TEMPERATURE:
            s.temp = val->temperature;
            break;
        case DC_SAMPLE_PRESSURE:
            s.pressure_tank  = (int)val->pressure.tank;
            s.pressure_value = val->pressure.value;
            break;
        case DC_SAMPLE_CNS:
            s.cns = val->cns * 100.0; // fraction → percent
            break;
        case DC_SAMPLE_DECO:
            s.deco_type  = (int)val->deco.type;
            s.deco_depth = val->deco.depth;
            s.deco_time  = val->deco.time;
            break;
        case DC_SAMPLE_RBT:
            s.rbt = val->rbt;
            break;
        case DC_SAMPLE_HEARTBEAT:
            s.heartbeat = val->heartbeat;
            break;
        case DC_SAMPLE_BEARING:
            s.bearing = val->bearing;
            break;
        case DC_SAMPLE_SETPOINT:
            s.setpoint = val->setpoint;
            break;
        case DC_SAMPLE_PPO2:
            s.ppo2.push_back({(int)val->ppo2.sensor, val->ppo2.value});
            break;
        case DC_SAMPLE_GASMIX:
            s.gasmix = (int)val->gasmix;
            break;
        default:
            break;
    }
}

// ── JSON helpers ──────────────────────────────────────────────────────────────

static std::string jd(double v, int prec = 2) {
    std::ostringstream o;
    o << std::fixed << std::setprecision(prec) << v;
    return o.str();
}

static std::string diveToJson(const Dive& d) {
    std::ostringstream o;
    o << "{"
      << "\"date\":\"" << d.when.year << '-'
      << (d.when.month  < 10 ? "0" : "") << d.when.month  << '-'
      << (d.when.day    < 10 ? "0" : "") << d.when.day    << "\","
      << "\"time\":\""
      << (d.when.hour   < 10 ? "0" : "") << d.when.hour   << ':'
      << (d.when.minute < 10 ? "0" : "") << d.when.minute << "\","
      << "\"maxdepth\":"  << jd(d.maxdepth)  << ","
      << "\"avgdepth\":"  << jd(d.avgdepth)  << ","
      << "\"duration\":"  << d.duration      << ","
      << "\"divemode\":\""<< divemodeStr(d.divemode) << "\",";

    if (d.temp_surface > -900)
        o << "\"temp_surface\":" << jd(d.temp_surface) << ",";
    if (d.temp_min > -900)
        o << "\"temp_min\":"     << jd(d.temp_min)     << ",";
    if (d.temp_max > -900)
        o << "\"temp_max\":"     << jd(d.temp_max)     << ",";
    if (d.atmospheric > 0)
        o << "\"atmospheric\":"  << jd(d.atmospheric, 4) << ",";
    if (d.salinity_type >= 0)
        o << "\"salinity_type\":" << d.salinity_type << ","
          << "\"salinity_density\":" << jd(d.salinity_density) << ",";

    // gasmixes
    o << "\"gasmixes\":[";
    for (size_t i = 0; i < d.gasmixes.size(); i++) {
        const auto& g = d.gasmixes[i];
        o << (i ? "," : "")
          << "{\"o2\":"  << jd(g.oxygen   * 100, 1)
          << ",\"he\":"  << jd(g.helium   * 100, 1)
          << ",\"n2\":"  << jd(g.nitrogen * 100, 1)
          << ",\"usage\":" << g.usage << "}";
    }
    o << "],";

    // tanks
    o << "\"tanks\":[";
    for (size_t i = 0; i < d.tanks.size(); i++) {
        const auto& t = d.tanks[i];
        o << (i ? "," : "")
          << "{\"gasmix\":"       << t.gasmix
          << ",\"volume\":"       << jd(t.volume, 1)
          << ",\"workpressure\":" << jd(t.workpressure, 0)
          << ",\"start\":"        << jd(t.beginpressure, 0)
          << ",\"end\":"          << jd(t.endpressure, 0)
          << ",\"usage\":"        << t.usage << "}";
    }
    o << "],";

    // samples: [time_ms, depth, temp, pressure_tank, pressure_value,
    //           cns, deco_type, deco_depth, deco_time, rbt, heartbeat,
    //           bearing, setpoint, gasmix, ppo2_count, [sensor,value]...]
    o << "\"samples\":[";
    for (size_t i = 0; i < d.samples.size(); i++) {
        const Sample& s = d.samples[i];
        o << (i ? "," : "")
          << "[" << s.time_ms
          << "," << jd(s.depth)
          << "," << jd(s.temp)
          << "," << s.pressure_tank
          << "," << jd(s.pressure_value, 1)
          << "," << jd(s.cns, 1)
          << "," << s.deco_type
          << "," << jd(s.deco_depth)
          << "," << s.deco_time
          << "," << s.rbt
          << "," << s.heartbeat
          << "," << s.bearing
          << "," << jd(s.setpoint, 3)
          << "," << s.gasmix
          << "," << s.ppo2.size();
        for (const auto& p : s.ppo2)
            o << "," << p.sensor << "," << jd(p.value, 3);
        o << "]";
    }
    o << "]}";
    return o.str();
}

// ── Descriptor resolution ─────────────────────────────────────────────────────
//
// libdivecomputer knows, per descriptor, which advertised BLE names belong to
// it: dc_descriptor_filter() answers that question. Asking it beats guessing
// with substring matches, and it never picks an unrelated driver.
//
// Filtering alone is not always enough, though: several descriptors can accept
// the same name (Shearwater's family filter takes every "Perdix*", the OSTC3
// family shares one model code). So collect every candidate, then rank.

// Case-insensitive compare that ignores spaces, hyphens and underscores, so
// "Perdix 2" and "PERDIX2" compare equal.
static bool name_equal_loose(const char* a, const char* b) {
    if (!a || !b) return false;
    auto skip = [](const char* p) {
        while (*p == ' ' || *p == '-' || *p == '_') p++;
        return p;
    };
    for (;;) {
        a = skip(a); b = skip(b);
        if (!*a || !*b) return !*a && !*b;
        if (tolower((unsigned char)*a) != tolower((unsigned char)*b)) return false;
        a++; b++;
    }
}

// Pelagic-family computers (Oceanic, Aqualung, Sherwood, Apeks) advertise as
// two letters naming the model followed by a serial number, e.g. "FQ001234".
// The two letters, uppercased, are the descriptor's model code.
static bool model_code_from_name(const char* name, unsigned int* out) {
    if (!name || !name[0] || !name[1]) return false;
    if (!isalpha((unsigned char)name[0]) || !isalpha((unsigned char)name[1])) return false;

    unsigned digits = 0;
    for (const char* p = name + 2; *p; p++) {
        if (isdigit((unsigned char)*p)) { digits++; continue; }
        if (*p == ' ' || *p == '-' || *p == '_') continue;
        return false;
    }
    if (digits < 6) return false;

    *out = ((unsigned int)toupper((unsigned char)name[0]) << 8) |
            (unsigned int)toupper((unsigned char)name[1]);
    return true;
}

// Length of the descriptor product name as a prefix of the advertised name,
// comparing loosely; 0 when it is not a prefix. Longer prefix = better match,
// which separates "Perdix 2" from "Perdix" when the device advertises the
// former.
static size_t product_prefix_len(const char* name, const char* product) {
    if (!name || !product) return 0;
    auto skip = [](const char* p) {
        while (*p == ' ' || *p == '-' || *p == '_') p++;
        return p;
    };
    const char* n = name;
    const char* p = product;
    size_t matched = 0;
    for (;;) {
        n = skip(n); p = skip(p);
        if (!*p) return matched;                 // product exhausted → prefix
        if (!*n) return 0;                       // name ran out first
        if (tolower((unsigned char)*n) != tolower((unsigned char)*p)) return 0;
        n++; p++; matched++;
    }
}

// Returns a descriptor the caller must dc_descriptor_free(), or NULL when the
// advertised name matches no supported dive computer.
static dc_descriptor_t* resolve_descriptor(dc_context_t* ctx,
                                           const char* name,
                                           dc_transport_t transport)
{
    dc_iterator_t* iter = nullptr;
    if (dc_descriptor_iterator_new(&iter, ctx) != DC_STATUS_SUCCESS) return nullptr;

    unsigned int wanted_model = 0;
    const bool has_wanted_model = model_code_from_name(name, &wanted_model);

    dc_descriptor_t* best  = nullptr;
    int              best_rank   = -1;   // higher is better
    size_t           best_prefix = 0;

    dc_descriptor_t* desc = nullptr;
    while (dc_iterator_next(iter, &desc) == DC_STATUS_SUCCESS) {
        if (!dc_descriptor_filter(desc, transport, name)) {
            dc_descriptor_free(desc);
            continue;
        }

        const char* product = dc_descriptor_get_product(desc);
        const size_t prefix = product_prefix_len(name, product);

        // Rank the candidate. 3: the name is exactly this product. 2: the name
        // carries this descriptor's model code. 1: the product is a prefix of
        // the name. 0: the descriptor's own filter accepted it, nothing more.
        int rank;
        if (name_equal_loose(name, product))                          rank = 3;
        else if (has_wanted_model &&
                 dc_descriptor_get_model(desc) == wanted_model)       rank = 2;
        else if (prefix > 0)                                          rank = 1;
        else                                                          rank = 0;

        const bool better = rank > best_rank ||
                            (rank == best_rank && prefix > best_prefix);
        if (better) {
            if (best) dc_descriptor_free(best);
            best = desc; best_rank = rank; best_prefix = prefix;
        } else {
            dc_descriptor_free(desc);
        }
    }
    dc_iterator_free(iter);

    if (best) {
        LOGI("Resolved '%s' → %s %s (model=%u, rank=%d)", name,
             dc_descriptor_get_vendor(best), dc_descriptor_get_product(best),
             dc_descriptor_get_model(best), best_rank);
    } else {
        LOGE("No descriptor matches advertised name '%s'", name);
    }
    return best;
}

// ── JNI globals ───────────────────────────────────────────────────────────────

static JavaVM*     g_jvm     = nullptr;
static jobject     g_bleObj  = nullptr;
static jmethodID   g_writeId = nullptr;

struct DownloadCtx {
    dc_context_t*    ctx        = nullptr;
    dc_descriptor_t* descriptor = nullptr;
    std::vector<Dive> dives;
    jclass    mainCls   = nullptr;
    jmethodID progressId = nullptr;
    jmethodID statusId   = nullptr;
    jmethodID diveFoundId = nullptr;
    jmethodID fingerprintId = nullptr;
    std::vector<uint8_t> newestFingerprint;
    bool fingerprintCaptured = false;
};

// Runs libdivecomputer's parser over one raw dive buffer. Shared by the live
// download path and by database imports, so both produce identical dives.
static bool parse_dive_buffer(dc_context_t* ctx, dc_descriptor_t* descriptor,
                              const unsigned char* data, unsigned int size,
                              Dive& dive)
{

    dc_parser_t* parser = nullptr;
    dc_status_t rc = dc_parser_new2(&parser, ctx, descriptor, data, size);
    if (rc != DC_STATUS_SUCCESS || !parser) {
        LOGE("dc_parser_new2 failed: %d", rc);
        return false;
    }

    dc_parser_get_datetime(parser, &dive.when);

    double dval = 0;
    if (dc_parser_get_field(parser, DC_FIELD_MAXDEPTH, 0, &dval) == DC_STATUS_SUCCESS)
        dive.maxdepth = dval;
    if (dc_parser_get_field(parser, DC_FIELD_AVGDEPTH, 0, &dval) == DC_STATUS_SUCCESS)
        dive.avgdepth = dval;

    uint32_t dur = 0;
    if (dc_parser_get_field(parser, DC_FIELD_DIVETIME, 0, &dur) == DC_STATUS_SUCCESS)
        dive.duration = dur;

    // Temperatures
    if (dc_parser_get_field(parser, DC_FIELD_TEMPERATURE_SURFACE, 0, &dval) == DC_STATUS_SUCCESS)
        dive.temp_surface = dval;
    if (dc_parser_get_field(parser, DC_FIELD_TEMPERATURE_MINIMUM, 0, &dval) == DC_STATUS_SUCCESS)
        dive.temp_min = dval;
    if (dc_parser_get_field(parser, DC_FIELD_TEMPERATURE_MAXIMUM, 0, &dval) == DC_STATUS_SUCCESS)
        dive.temp_max = dval;

    // Atmospheric pressure
    if (dc_parser_get_field(parser, DC_FIELD_ATMOSPHERIC, 0, &dval) == DC_STATUS_SUCCESS)
        dive.atmospheric = dval;

    // Salinity
    dc_salinity_t sal = {};
    if (dc_parser_get_field(parser, DC_FIELD_SALINITY, 0, &sal) == DC_STATUS_SUCCESS) {
        dive.salinity_type    = (int)sal.type;
        dive.salinity_density = sal.density;
    }

    // Dive mode
    dc_divemode_t mode = DC_DIVEMODE_OC;
    if (dc_parser_get_field(parser, DC_FIELD_DIVEMODE, 0, &mode) == DC_STATUS_SUCCESS)
        dive.divemode = (int)mode;

    // Gas mixes
    unsigned int ngases = 0;
    if (dc_parser_get_field(parser, DC_FIELD_GASMIX_COUNT, 0, &ngases) == DC_STATUS_SUCCESS) {
        for (unsigned int i = 0; i < ngases; i++) {
            dc_gasmix_t gm = {};
            if (dc_parser_get_field(parser, DC_FIELD_GASMIX, i, &gm) == DC_STATUS_SUCCESS) {
                GasMix g;
                g.oxygen   = gm.oxygen;
                g.helium   = gm.helium;
                g.nitrogen = gm.nitrogen;
                g.usage    = (int)gm.usage;
                dive.gasmixes.push_back(g);
            }
        }
    }

    // Tanks
    unsigned int ntanks = 0;
    if (dc_parser_get_field(parser, DC_FIELD_TANK_COUNT, 0, &ntanks) == DC_STATUS_SUCCESS) {
        for (unsigned int i = 0; i < ntanks; i++) {
            dc_tank_t tk = {};
            if (dc_parser_get_field(parser, DC_FIELD_TANK, i, &tk) == DC_STATUS_SUCCESS) {
                Tank t;
                t.gasmix        = (int)tk.gasmix;
                t.volume        = tk.volume;
                t.workpressure  = tk.workpressure;
                t.beginpressure = tk.beginpressure;
                t.endpressure   = tk.endpressure;
                t.usage         = (int)tk.usage;
                dive.tanks.push_back(t);
            }
        }
    }

    dc_parser_samples_foreach(parser, sample_cb, &dive);
    dc_parser_destroy(parser);
    return true;
}

static int dive_cb(const unsigned char* data, unsigned int size,
                   const unsigned char* fingerprint, unsigned int fsize, void* ud)
{
    auto* dc = (DownloadCtx*)ud;

    // Capture fingerprint of first (newest) dive to use as stop-marker next time.
    if (!dc->fingerprintCaptured && fingerprint && fsize > 0) {
        dc->newestFingerprint.assign(fingerprint, fingerprint + fsize);
        dc->fingerprintCaptured = true;
        LOGI("Captured fingerprint: %u bytes", fsize);
    }

    Dive dive;
    if (!parse_dive_buffer(dc->ctx, dc->descriptor, data, size, dive))
        return 1;

    LOGI("Dive %04d-%02d-%02d %.1fm %us %zu samples %zu gases %zu tanks",
        dive.when.year, dive.when.month, dive.when.day,
        dive.maxdepth, dive.duration, dive.samples.size(),
        dive.gasmixes.size(), dive.tanks.size());

    // Notify Kotlin immediately so dive appears in the list.
    if (dc->diveFoundId) {
        JNIEnv* e = nullptr;
        g_jvm->AttachCurrentThread(&e, nullptr);
        std::string json = diveToJson(dive);
        jstring s = e->NewStringUTF(json.c_str());
        e->CallStaticVoidMethod(dc->mainCls, dc->diveFoundId, s);
        e->DeleteLocalRef(s);
    }

    dc->dives.push_back(std::move(dive));
    return 1;
}

static void event_cb(dc_device_t*, dc_event_type_t ev, const void* data, void* ud) {
    auto* dc = (DownloadCtx*)ud;
    JNIEnv* e = nullptr;
    g_jvm->AttachCurrentThread(&e, nullptr);

    if (ev == DC_EVENT_PROGRESS && dc->progressId) {
        auto* p = (const dc_event_progress_t*)data;
        if (p->maximum > 0)
            e->CallStaticVoidMethod(dc->mainCls, dc->progressId, (jint)p->current, (jint)p->maximum);
    } else if (ev == DC_EVENT_DEVINFO && dc->statusId) {
        auto* d = (const dc_event_devinfo_t*)data;
        char buf[64];
        snprintf(buf, sizeof(buf), "model=%u fw=%u serial=%u", d->model, d->firmware, d->serial);
        jstring s = e->NewStringUTF(buf);
        e->CallStaticVoidMethod(dc->mainCls, dc->statusId, s);
        e->DeleteLocalRef(s);
    }
}

// ── Raw log parsing (Shearwater Cloud database imports) ───────────────────────
//
// Shearwater's desktop app stores each dive as the same bytes the computer
// sends over the wire, so libdivecomputer's own parser can read them once the
// two compression layers are undone. Both are reimplementations of the
// algorithms in libdivecomputer's shearwater_common.c, which keeps them
// private to that translation unit.

// The payload is a continuous MSB-first stream of 9-bit values. Bit 0x100
// marks a literal byte; zero ends the stream; anything else is a run of that
// many zero bytes.
static bool shearwater_lre_decompress(const uint8_t* data, size_t size,
                                      std::vector<uint8_t>& out)
{
    const size_t nbits = size * 8;
    if (nbits == 0 || nbits % 9 != 0) return false;

    for (size_t offset = 0; offset + 9 <= nbits; offset += 9) {
        const size_t byte = offset / 8;
        const unsigned bit = (unsigned)(offset % 8);
        const unsigned shift = 16 - (bit + 9);
        const unsigned pair = ((unsigned)data[byte] << 8) |
                              (byte + 1 < size ? (unsigned)data[byte + 1] : 0u);
        const unsigned value = (pair >> shift) & 0x1FF;

        if (value & 0x100)      out.push_back((uint8_t)(value & 0xFF));
        else if (value == 0)    break;                       // end of stream
        else                    out.resize(out.size() + value, 0);
    }
    return true;
}

// Every 32-byte block after the first is XOR'ed with the one before it.
static void shearwater_xor_decompress(std::vector<uint8_t>& d) {
    for (size_t i = 32; i < d.size(); i++) d[i] ^= d[i - 32];
}

// Finds a descriptor by vendor and product rather than by advertised name;
// database imports know exactly which computer wrote the log.
static dc_descriptor_t* descriptor_by_product(dc_context_t* ctx,
                                              const char* vendor,
                                              const char* product)
{
    dc_iterator_t* iter = nullptr;
    if (dc_descriptor_iterator_new(&iter, ctx) != DC_STATUS_SUCCESS) return nullptr;

    dc_descriptor_t* desc = nullptr;
    dc_descriptor_t* found = nullptr;
    while (dc_iterator_next(iter, &desc) == DC_STATUS_SUCCESS) {
        const char* v = dc_descriptor_get_vendor(desc);
        const char* p = dc_descriptor_get_product(desc);
        if (!found && v && p &&
            strcasecmp(v, vendor) == 0 && name_equal_loose(p, product)) {
            found = desc;
            continue;
        }
        dc_descriptor_free(desc);
    }
    dc_iterator_free(iter);
    return found;
}

// ── JNI entry points ──────────────────────────────────────────────────────────

extern "C" {

JNIEXPORT void JNICALL
Java_fi_deeplog_bridge_DcBridge_onBleData(JNIEnv* env, jclass, jbyteArray arr)
{
    if (!g_bio) return;
    jsize len  = env->GetArrayLength(arr);
    jbyte* raw = env->GetByteArrayElements(arr, nullptr);
    g_bio->push((const uint8_t*)raw, (size_t)len);
    env->ReleaseByteArrayElements(arr, raw, JNI_ABORT);
}

// Parses one raw dive log as stored by a desktop app's database, returning the
// same dive JSON a live download produces (or NULL when it cannot be read).
// When `compressed` is set the Shearwater LRE+XOR layers are undone first.
JNIEXPORT jstring JNICALL
Java_fi_deeplog_bridge_DcBridge_parseRaw(
    JNIEnv* env, jclass,
    jstring jVendor, jstring jProduct, jbyteArray jData, jboolean compressed)
{
    const char* vendor  = env->GetStringUTFChars(jVendor, nullptr);
    const char* product = env->GetStringUTFChars(jProduct, nullptr);

    jsize len = env->GetArrayLength(jData);
    std::vector<uint8_t> raw((size_t)len);
    env->GetByteArrayRegion(jData, 0, len, (jbyte*)raw.data());

    if (compressed) {
        std::vector<uint8_t> expanded;
        if (!shearwater_lre_decompress(raw.data(), raw.size(), expanded)) {
            LOGE("parseRaw: not a valid LRE stream (%zu bytes)", raw.size());
            env->ReleaseStringUTFChars(jVendor, vendor);
            env->ReleaseStringUTFChars(jProduct, product);
            return nullptr;
        }
        shearwater_xor_decompress(expanded);
        raw.swap(expanded);
    }

    dc_context_t* ctx = nullptr;
    dc_context_new(&ctx);
    dc_descriptor_t* desc = descriptor_by_product(ctx, vendor, product);
    env->ReleaseStringUTFChars(jVendor, vendor);
    env->ReleaseStringUTFChars(jProduct, product);

    jstring result = nullptr;
    if (desc) {
        Dive dive;
        if (parse_dive_buffer(ctx, desc, raw.data(), (unsigned int)raw.size(), dive))
            result = env->NewStringUTF(diveToJson(dive).c_str());
        dc_descriptor_free(desc);
    } else {
        LOGE("parseRaw: no descriptor for that vendor/product");
    }
    dc_context_free(ctx);
    return result;
}

// Resolve an advertised BLE name to "Vendor|Product", or NULL when this is
// not a dive computer libdivecomputer supports. Lets the scan list label real
// devices and drop the headphones and fitness bands sharing the airwaves.
JNIEXPORT jstring JNICALL
Java_fi_deeplog_bridge_DcBridge_matchDevice(
    JNIEnv* env, jclass, jstring jName, jint jtransport)
{
    const char* name = env->GetStringUTFChars(jName, nullptr);

    dc_context_t* ctx = nullptr;
    dc_context_new(&ctx);
    dc_descriptor_t* desc = resolve_descriptor(ctx, name, (dc_transport_t)jtransport);
    env->ReleaseStringUTFChars(jName, name);

    jstring result = nullptr;
    if (desc) {
        const char* v = dc_descriptor_get_vendor(desc);
        const char* pr = dc_descriptor_get_product(desc);
        std::string s = std::string(v ? v : "") + "|" + std::string(pr ? pr : "");
        result = env->NewStringUTF(s.c_str());
        dc_descriptor_free(desc);
    }
    dc_context_free(ctx);
    return result;
}

JNIEXPORT jstring JNICALL
Java_fi_deeplog_bridge_DcBridge_download(
    JNIEnv* env, jclass,
    jstring jDeviceName,
    jint    jtransport,   // DC_TRANSPORT_BLE = 32
    jobject bleObj,
    jbyteArray jFingerprint)
{
    env->GetJavaVM(&g_jvm);
    g_bleObj  = env->NewGlobalRef(bleObj);
    jclass bleCls = env->GetObjectClass(bleObj);
    g_writeId = env->GetMethodID(bleCls, "write", "([B)Z");

    const char* devName = env->GetStringUTFChars(jDeviceName, nullptr);
    LOGI("download: device='%s' transport=%d", devName, jtransport);

    dc_context_t* ctx = nullptr;
    dc_context_new(&ctx);

    dc_descriptor_t* descriptor =
        resolve_descriptor(ctx, devName, (dc_transport_t)jtransport);

    // Report the resolved model to the UI, or say plainly that the device is
    // not supported instead of opening some other vendor's driver on it.
    {
        jclass mainCls = env->FindClass("fi/deeplog/bridge/MainActivity");
        jmethodID statusId = env->GetStaticMethodID(mainCls, "onStatus", "(Ljava/lang/String;)V");
        char buf[192];
        if (descriptor)
            snprintf(buf, sizeof(buf), "Recognised as %s %s",
                     dc_descriptor_get_vendor(descriptor),
                     dc_descriptor_get_product(descriptor));
        else
            snprintf(buf, sizeof(buf), "'%s' is not a supported dive computer", devName);
        jstring js = env->NewStringUTF(buf);
        env->CallStaticVoidMethod(mainCls, statusId, js);
        env->DeleteLocalRef(js);
        env->DeleteLocalRef(mainCls);
    }

    env->ReleaseStringUTFChars(jDeviceName, devName);

    if (!descriptor) {
        dc_context_free(ctx);
        env->DeleteGlobalRef(g_bleObj);
        return env->NewStringUTF("[]");
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

    dc_iostream_t* stream = nullptr;
    dc_status_t rc = dc_custom_open(&stream, ctx, (dc_transport_t)jtransport, &k_cbs, &bio);
    if (rc != DC_STATUS_SUCCESS) {
        LOGE("dc_custom_open failed: %d", rc);
        dc_descriptor_free(descriptor); dc_context_free(ctx);
        g_bio = nullptr; env->DeleteGlobalRef(g_bleObj);
        return env->NewStringUTF("[]");
    }

    dc_device_t* device = nullptr;
    rc = dc_device_open(&device, ctx, descriptor, stream);
    if (rc != DC_STATUS_SUCCESS) {
        LOGE("dc_device_open failed: %d", rc);
        dc_iostream_close(stream); dc_descriptor_free(descriptor); dc_context_free(ctx);
        g_bio = nullptr; env->DeleteGlobalRef(g_bleObj);
        return env->NewStringUTF("[]");
    }

    DownloadCtx dc;
    dc.ctx        = ctx;
    dc.descriptor = descriptor;
    dc.mainCls    = (jclass)env->NewGlobalRef(env->FindClass("fi/deeplog/bridge/MainActivity"));
    dc.progressId   = env->GetStaticMethodID(dc.mainCls, "onProgress",    "(II)V");
    dc.statusId     = env->GetStaticMethodID(dc.mainCls, "onStatus",      "(Ljava/lang/String;)V");
    dc.diveFoundId  = env->GetStaticMethodID(dc.mainCls, "onDiveFound",   "(Ljava/lang/String;)V");
    dc.fingerprintId= env->GetStaticMethodID(dc.mainCls, "onFingerprint", "(Ljava/lang/String;)V");

    // Set fingerprint so libdivecomputer stops when it reaches the already-downloaded dive.
    if (jFingerprint) {
        jsize fpLen  = env->GetArrayLength(jFingerprint);
        jbyte* fpRaw = env->GetByteArrayElements(jFingerprint, nullptr);
        if (fpLen > 0 && fpRaw) {
            dc_status_t fps = dc_device_set_fingerprint(device, (const unsigned char*)fpRaw, (unsigned int)fpLen);
            LOGI("dc_device_set_fingerprint (%d bytes): %d", (int)fpLen, fps);
        }
        env->ReleaseByteArrayElements(jFingerprint, fpRaw, JNI_ABORT);
    }

    dc_device_set_events(device,
        DC_EVENT_PROGRESS | DC_EVENT_DEVINFO,
        event_cb, &dc);

    rc = dc_device_foreach(device, dive_cb, &dc);
    LOGI("dc_device_foreach returned %d, %zu dives", rc, dc.dives.size());

    // Send newest fingerprint to Kotlin for storage.
    if (!dc.newestFingerprint.empty() && dc.fingerprintId) {
        std::ostringstream hexss;
        for (uint8_t b : dc.newestFingerprint)
            hexss << std::hex << std::setw(2) << std::setfill('0') << (int)b;
        jstring hexStr = env->NewStringUTF(hexss.str().c_str());
        env->CallStaticVoidMethod(dc.mainCls, dc.fingerprintId, hexStr);
        env->DeleteLocalRef(hexStr);
    }

    dc_device_close(device);
    dc_iostream_close(stream);
    dc_descriptor_free(descriptor);
    dc_context_free(ctx);
    env->DeleteGlobalRef(dc.mainCls);
    g_bio = nullptr;
    env->DeleteGlobalRef(g_bleObj);

    return env->NewStringUTF("[]"); // dives already sent via onDiveFound
}

} // extern "C"
