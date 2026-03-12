// BridgeService.kt
//
// Foreground service that:
//   1. Runs a minimal HTTP server on localhost:8765 — same API as suunto_bridge.py
//   2. Handles BLE download for supported dive computers:
//        • Suunto EON Steel / EON Core / D5 / G5   (HDLC over GATT)
//        • Shearwater Perdix/Perdix 2/Teric/Peregrine/Tern/NERD/Petrel (SLIP over GATT)
//        • Heinrichs-Weikamp OSTC (Telit & U-Blox Terminal-I/O flow-control)
//        • Mares BlueLink Pro, Pelagic/Oceanic/Aqualung/Sherwood/Apeks,
//          ScubaPro G2/G3, Divesoft, Cressi, Nordic UART generic —
//          device detected + connected, dive-data parsing coming soon
//   3. Pushes parsed dives directly to Supabase
//
// BLE UUID knowledge derived from Subsurface open-source project (GPLv2+):
//   https://github.com/subsurface/subsurface/blob/master/core/qt-ble.cpp
//   https://github.com/subsurface/subsurface/blob/master/core/btdiscovery.cpp

package fi.deeplog.bridge

import android.annotation.SuppressLint
import android.app.*
import android.bluetooth.*
import android.bluetooth.le.*
import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.content.IntentFilter
import android.os.*
import android.util.Log
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.PrintWriter
import java.net.ServerSocket
import java.net.Socket
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import org.json.JSONArray
import org.json.JSONObject
import java.util.*
import java.util.concurrent.atomic.AtomicReference

private const val TAG           = "DeepLogBridge"
private const val PORT          = 8765
private const val NOTIF_CHANNEL = "deeplog_bridge"
private const val NOTIF_ID      = 1

// ── Supabase ──────────────────────────────────────────────────────────────────
private const val SUPABASE_URL = "https://bdquivweiecffyopsevs.supabase.co"
private const val SUPABASE_KEY =
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9" +
    ".eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJkcXVpdndlaWVjZmZ5b3BzZXZzIiwi" +
    "cm9sZSI6ImFub24iLCJpYXQiOjE3NzIyMTM2MjYsImV4cCI6MjA4Nzc4OTYyNn0" +
    ".wXLAcj5NeyVnO2nTB5ZzNWwe_hFtPkZYHBKkMT2FmAo"

// ── BLE UUIDs ─────────────────────────────────────────────────────────────────
// Suunto (EON Steel, EON Core, D5, G5)
private val EON_SERVICE  = UUID.fromString("98ae7120-e62e-11e3-badd-0002a5d5c51b")
private val EON_TX       = UUID.fromString("0000fefb-0000-1000-8000-00805f9b34fb")
private val EON_RX       = UUID.fromString("00000002-0000-1000-8000-00805f9b34fb")

// Shearwater (Perdix, Perdix 2, Teric, Peregrine, Tern, NERD, Petrel)
private val SW_SERVICE   = UUID.fromString("fe25c237-0ece-443c-b0aa-e02033e7029d")
private val SW_CHAR      = UUID.fromString("27b7570b-359e-45a3-91bb-cf7e70049bd2")

// Heinrichs-Weikamp OSTC — Telit/Stollmann Terminal I/O
private val HW_TELIT_SERVICE     = UUID.fromString("0000fefb-0000-1000-8000-00805f9b34fb")
private val HW_TELIT_DATA_RX     = UUID.fromString("00000001-0000-1000-8000-008025000000") // write commands
private val HW_TELIT_DATA_TX     = UUID.fromString("00000002-0000-1000-8000-008025000000") // subscribe notify
private val HW_TELIT_CREDITS_RX  = UUID.fromString("00000003-0000-1000-8000-008025000000") // write credits
private val HW_TELIT_CREDITS_TX  = UUID.fromString("00000004-0000-1000-8000-008025000000") // subscribe indicate

// Heinrichs-Weikamp OSTC — U-Blox Terminal I/O
private val HW_UBLOX_SERVICE     = UUID.fromString("2456e1b9-26e2-8f83-e744-f34f01e9d701")
private val HW_UBLOX_DATA        = UUID.fromString("2456e1b9-26e2-8f83-e744-f34f01e9d703")
private val HW_UBLOX_CREDITS     = UUID.fromString("2456e1b9-26e2-8f83-e744-f34f01e9d704")

// Mares BlueLink Pro
private val MARES_SERVICE        = UUID.fromString("544e326b-5b72-c6b0-1c46-41c1bc448118")

// Pelagic (Oceanic/Aqualung/Sherwood/Apeks) — two service variant groups
private val PELAGIC_A_SERVICE    = UUID.fromString("cb3c4555-d670-4670-bc20-b61dbc851e9a") // i770R, i200C, Pro Plus X, Geo 4.0
private val PELAGIC_B_SERVICE    = UUID.fromString("ca7b0001-f785-4c38-b599-c7c5fbadb034") // i330R, DSX

// ScubaPro G2 / G3
private val SCUBAPRO_SERVICE     = UUID.fromString("fdcdeaaa-295d-470e-bf15-04217b7aa0a0")

// Divesoft
private val DIVESOFT_SERVICE     = UUID.fromString("0000fcef-0000-1000-8000-00805f9b34fb")

// Cressi — modified Nordic UART Service (last bytes differ from standard NUS)
private val CRESSI_SERVICE       = UUID.fromString("6e400001-b5a3-f393-e0a9-e50e24dc10b8")
private val CRESSI_RX            = UUID.fromString("6e400002-b5a3-f393-e0a9-e50e24dc10b8") // write (phone→device)
private val CRESSI_TX            = UUID.fromString("6e400003-b5a3-f393-e0a9-e50e24dc10b8") // notify (device→phone)
private val CRESSI_FW_CHAR       = UUID.fromString("6e400004-b5a3-f393-e0a9-e50e24dc10b8") // firmware (uint16 LE)

// Nordic UART Service — generic, used by some Ratio, Tecdiving, McLean etc.
private val NORDIC_UART_SERVICE  = UUID.fromString("6e400001-b5a3-f393-e0a9-e50e24dcca9e")

// Halcyon Symbios
private val HALCYON_SERVICE      = UUID.fromString("00000001-8c3b-4f2c-a59e-8c08224f3253")

// Standard GATT Client Characteristic Configuration Descriptor
private val CCCD = UUID.fromString("00002902-0000-1000-8000-00805f9b34fb")

// Divesoft Freedom BLE characteristics
private val DIVESOFT_RX = UUID.fromString("0000fcf0-0000-1000-8000-00805f9b34fb")
private val DIVESOFT_TX = UUID.fromString("0000fcf1-0000-1000-8000-00805f9b34fb")

// Pelagic (Oceanic / Aqualung / Sherwood / Apeks) BLE characteristics
private val PELAGIC_RX  = UUID.fromString("ca7b0002-f785-4c38-b599-c7c5fbadb034")
private val PELAGIC_TX  = UUID.fromString("ca7b0003-f785-4c38-b599-c7c5fbadb034")

// ScubaPro G2 / G3 BLE characteristics
private val SCUBAPRO_RX = UUID.fromString("fdcdeaab-295d-470e-bf15-04217b7aa0a0")
private val SCUBAPRO_TX = UUID.fromString("fdcdeaac-295d-470e-bf15-04217b7aa0a0")

// ── HW Terminal I/O flow-control constants ────────────────────────────────────
private const val HW_MAXIMAL_CREDIT = 254
private const val HW_MINIMAL_CREDIT  = 32

// ── OSTC command protocol (hw_ostc3.c) ───────────────────────────────────────
private const val OSTC_CMD_INIT     = 0xBB.toByte()  // enter download mode
private const val OSTC_CMD_COMPACT  = 0x6D.toByte()  // compact logbook (16 × 256 = 4096 bytes)
private const val OSTC_CMD_DIVE     = 0x66.toByte()  // download dive by slot index
private const val OSTC_CMD_EXIT     = 0xFF.toByte()  // exit download mode
private const val OSTC_READY        = 0x4D.toByte()  // device READY sentinel
private const val OSTC_LOGBOOK_SLOTS = 256
private const val OSTC_COMPACT_ENTRY = 16             // bytes per compact logbook entry

// ── Cressi BLE command bytes (cressi_goa.c) ───────────────────────────────────
private const val CRESSI_CMD_LOGBOOK = 0x02.toByte()  // request logbook list
private const val CRESSI_CMD_DIVE    = 0x03.toByte()  // request single dive

// ── Divesoft Freedom message types (divesoft_freedom.c) ───────────────────────
private const val DS_MSG_CONNECT       = 2
private const val DS_MSG_CONNECTED     = 3
private const val DS_MSG_VERSION       = 4
private const val DS_MSG_DIVE_LIST     = 66
private const val DS_MSG_DIVE_LIST_V1  = 67
private const val DS_MSG_DIVE_LIST_V2  = 71
private const val DS_MSG_DIVE_DATA     = 64
private const val DS_MSG_DIVE_DATA_RSP = 65
private const val DS_EPOCH             = 946684800L  // 2000-01-01 00:00:00 UTC

// ── Pelagic (Oceanic atom2) BLE protocol constants ────────────────────────────
private const val PEL_SYNC   = 0xCD.toByte()
private const val PEL_ACK    = 0x5A.toByte()
private const val PEL_NAK    = 0xA5.toByte()
private const val PEL_CMD_VERSION   = 0x84.toByte()
private const val PEL_CMD_HANDSHAKE = 0xE5.toByte()
private const val PEL_CMD_READ8     = 0xB4.toByte()
private const val PEL_CMD_QUIT      = 0x6A.toByte()
private const val PEL_PAGESIZE      = 16

// ── ScubaPro / Uwatec Smart BLE protocol constants ────────────────────────────
private const val SC_CMD_SIZE  = 0xC6.toByte()
private const val SC_CMD_DATA  = 0xC4.toByte()
private const val SC_EPOCH     = 946684800L  // 2000-01-01 00:00:00 UTC

// ── SLIP ──────────────────────────────────────────────────────────────────────
private const val SLIP_END      = 0xC0.toByte()
private const val SLIP_ESC      = 0xDB.toByte()
private const val SLIP_ESC_END  = 0xDC.toByte()
private const val SLIP_ESC_ESC  = 0xDD.toByte()

// ── HDLC ──────────────────────────────────────────────────────────────────────
private const val HDLC_FLAG     = 0x7E.toByte()
private const val HDLC_ESC      = 0x7D.toByte()

// ── Detected dive computer type ───────────────────────────────────────────────
enum class DiveComputerType {
    SUUNTO_EON,       // Suunto EON Steel / Core / D5 / G5
    SHEARWATER,       // Shearwater Perdix / Teric / Peregrine / Tern / NERD / Petrel
    HW_OSTC_TELIT,    // Heinrichs-Weikamp OSTC via Telit Terminal I/O
    HW_OSTC_UBLOX,    // Heinrichs-Weikamp OSTC via U-Blox Terminal I/O
    MARES,            // Mares BlueLink Pro — detected, parsing coming soon
    PELAGIC_A,        // Oceanic / Aqualung / Sherwood / Apeks group A — detected
    PELAGIC_B,        // Oceanic / Aqualung / Sherwood / Apeks group B — detected
    SCUBAPRO,         // ScubaPro G2 / G3 — detected
    DIVESOFT,         // Divesoft — detected
    CRESSI,           // Cressi — detected
    GENERIC_UART,     // Nordic UART / other generic — detected
    UNKNOWN
}

data class FoundDevice(
    val device: BluetoothDevice,
    val type:   DiveComputerType,
    val advertisedServiceUuid: UUID? = null
)

// ── State shared between BLE coroutine and HTTP server ────────────────────────
data class BridgeState(
    val status:   String  = "idle",     // idle|scanning|downloading|uploading|done|error
    val message:  String  = "",
    val progress: Int     = 0,          // 0–100
    val dives:    List<Map<String,Any?>> = emptyList(),
    val lastError:String  = ""
) {
    fun toJson(): String = JSONObject().apply {
        put("status",    status)
        put("message",   message)
        put("progress",  progress)
        put("last_error",lastError)
        put("dives", JSONArray().also { arr ->
            dives.forEach { d -> arr.put(JSONObject(d.filterValues { it != null })) }
        })
    }.toString()
}

class BridgeService : Service() {

    companion object {
        var instance: BridgeService? = null
        var currentState: BridgeState? = null
    }

    private val scope      = CoroutineScope(Dispatchers.IO + SupervisorJob())
    private val state      = AtomicReference(BridgeState())
    private var httpServer : BridgeHttpServer? = null
    private var username   = "anonymous"

    // ── Service lifecycle ─────────────────────────────────────────────────────

    override fun onCreate() {
        super.onCreate()
        instance = this
        try {
            createNotificationChannel()
            startForeground(NOTIF_ID, buildNotification("DeepLog running"))
            httpServer = BridgeHttpServer(PORT, this).also { it.start() }
            Log.i(TAG, "Bridge started on port $PORT")
        } catch (e: Exception) {
            Log.e(TAG, "BridgeService onCreate error: ${e.message}", e)
        }
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        username = intent?.getStringExtra("username") ?: username
        updateNotification("Bridge running · user: $username")
        return START_STICKY
    }

    override fun onDestroy() {
        instance = null
        httpServer?.stop()
        scope.cancel()
        super.onDestroy()
    }

    override fun onBind(intent: Intent?): IBinder? = null

    // ── API called by HTTP server ─────────────────────────────────────────────

    fun getState() = state.get().toJson()

    fun startDownload(address: String?, user: String) {
        username = user
        if (state.get().status in listOf("scanning", "downloading", "uploading")) return
        setState(BridgeState(status = "scanning", message = "Starting scan…", progress = 5))
        scope.launch { runDownload(address) }
    }

    fun resetState() = setState(BridgeState())

    // ── BLE download orchestrator ─────────────────────────────────────────────

    @SuppressLint("MissingPermission")
    private suspend fun runDownload(targetAddress: String?) {
        val bleManager = getSystemService(Context.BLUETOOTH_SERVICE) as BluetoothManager
        val adapter    = bleManager.adapter

        if (!adapter.isEnabled) {
            setState(BridgeState(status = "error", lastError = "Bluetooth is off"))
            return
        }

        try {
            setState(state.get().copy(status = "scanning", message = "Scanning for dive computers…", progress = 10))

            val found: FoundDevice = if (targetAddress != null) {
                val device = adapter.getRemoteDevice(targetAddress)
                FoundDevice(device, detectDeviceTypeByName(device.name ?: ""))
            } else {
                scanForDevice(adapter) ?: run {
                    setState(BridgeState(
                        status = "error",
                        lastError = "No dive computers found. Make sure Bluetooth mode is active on the device."
                    ))
                    return
                }
            }

            val deviceLabel = found.device.name ?: found.device.address
            setState(state.get().copy(
                status   = "downloading",
                message  = "Connecting to $deviceLabel…",
                progress = 20
            ))

            Log.i(TAG, "Device: $deviceLabel  type=${found.type}")

            val dives: List<DiveSummary> = when (found.type) {
                DiveComputerType.SUUNTO_EON    -> downloadEon(found.device)
                DiveComputerType.SHEARWATER    -> downloadShearwater(found.device)
                DiveComputerType.HW_OSTC_TELIT -> downloadHwOstcTelit(found.device)
                DiveComputerType.HW_OSTC_UBLOX -> downloadHwOstcUblox(found.device)
                DiveComputerType.CRESSI        -> downloadCressl(found.device)
                DiveComputerType.DIVESOFT      -> downloadDivesoft(found.device)
                DiveComputerType.PELAGIC_A,
                DiveComputerType.PELAGIC_B     -> downloadPelagic(found.device)
                DiveComputerType.SCUBAPRO      -> downloadScubaPro(found.device)
                else -> {
                    val brand = found.type.toBrandName()
                    setState(BridgeState(
                        status    = "error",
                        lastError = "Device found: $deviceLabel ($brand). " +
                                    "Dive data download for this brand is not yet implemented."
                    ))
                    return
                }
            }

            if (dives.isEmpty()) {
                setState(BridgeState(status = "error", lastError = "No dives found on device"))
                return
            }

            setState(state.get().copy(
                status   = "uploading",
                message  = "Uploading ${dives.size} dives…",
                progress = 80
            ))

            var uploaded = 0
            dives.forEachIndexed { i, dive ->
                try {
                    val diveId = uploadDive(dive)
                    if (diveId != null && dive.samples.isNotEmpty()) {
                        runCatching { uploadSamples(diveId, dive) }
                            .onFailure { Log.w(TAG, "Samples upload failed for dive ${i + 1}: ${it.message}") }
                    }
                    uploaded++
                    setState(state.get().copy(
                        progress = 80 + 18 * (i + 1) / dives.size,
                        message  = "Uploading dive ${i + 1}/${dives.size}…"
                    ))
                } catch (e: Exception) {
                    Log.w(TAG, "Upload failed for dive ${i + 1}: ${e.message}")
                }
            }

            setState(BridgeState(
                status   = "done",
                message  = "Uploaded $uploaded/${dives.size} dives",
                progress = 100,
                dives    = dives.map { it.summary() }
            ))

        } catch (e: Exception) {
            Log.e(TAG, "Download error", e)
            setState(BridgeState(status = "error", lastError = e.message ?: "Unknown error"))
        }
    }

    // ── BLE scanning ──────────────────────────────────────────────────────────
    //
    // Scans for all known dive computer BLE service UUIDs.
    // Returns FoundDevice with the device type detected from advertised service UUIDs.

    @SuppressLint("MissingPermission")
    private suspend fun scanForDevice(adapter: BluetoothAdapter): FoundDevice? {
        data class ScanHit(val device: BluetoothDevice, val serviceUuid: UUID)
        val result  = CompletableDeferred<ScanHit?>()
        val scanner = adapter.bluetoothLeScanner
            ?: run {
                setState(state.get().copy(lastError = "BLE scanner unavailable — is Bluetooth on?"))
                return null
            }

        // All known dive computer BLE service UUIDs — order matches Subsurface priority
        val knownServiceUuids = listOf(
            HW_TELIT_SERVICE, HW_UBLOX_SERVICE, MARES_SERVICE, EON_SERVICE,
            PELAGIC_A_SERVICE, PELAGIC_B_SERVICE, SCUBAPRO_SERVICE, SW_SERVICE,
            DIVESOFT_SERVICE, CRESSI_SERVICE, NORDIC_UART_SERVICE, HALCYON_SERVICE,
        )

        val filters  = knownServiceUuids.map { ScanFilter.Builder().setServiceUuid(ParcelUuid(it)).build() }
        val settings = ScanSettings.Builder().setScanMode(ScanSettings.SCAN_MODE_LOW_LATENCY).build()

        val callback = object : ScanCallback() {
            override fun onScanResult(callbackType: Int, r: ScanResult) {
                if (result.isCompleted) return
                val advUuids    = r.scanRecord?.serviceUuids?.map { it.uuid } ?: emptyList()
                val matchedUuid = knownServiceUuids.firstOrNull { it in advUuids } ?: EON_SERVICE
                scanner.stopScan(this)
                result.complete(ScanHit(r.device, matchedUuid))
            }
            override fun onScanFailed(errorCode: Int) {
                val reason = when (errorCode) {
                    ScanCallback.SCAN_FAILED_ALREADY_STARTED        -> "scan already running"
                    ScanCallback.SCAN_FAILED_APPLICATION_REGISTRATION_FAILED -> "app registration failed"
                    ScanCallback.SCAN_FAILED_FEATURE_UNSUPPORTED     -> "BLE unsupported"
                    ScanCallback.SCAN_FAILED_INTERNAL_ERROR          -> "internal BLE error"
                    else -> "error code $errorCode"
                }
                setState(state.get().copy(message = "BLE scan failed: $reason"))
                result.complete(null)
            }
        }

        setState(state.get().copy(message = "Scanning… (15 s timeout)", progress = 12))
        scanner.startScan(filters, settings, callback)

        // Tick every 3 s so the user sees progress
        val tickJob = scope.launch {
            for (t in 3..15 step 3) {
                delay(3_000)
                if (!result.isCompleted)
                    setState(state.get().copy(message = "Scanning… ${15 - t}s remaining", progress = 12 + t))
            }
        }

        withTimeoutOrNull(15_000) { result.await() }
        tickJob.cancel()
        runCatching { scanner.stopScan(callback) }

        val scanHit = if (result.isCompleted) result.await() else null
        if (scanHit == null) return null

        val type = detectDeviceTypeByServiceUuid(scanHit.serviceUuid)
            .takeIf { it != DiveComputerType.UNKNOWN }
            ?: detectDeviceTypeByName(scanHit.device.name ?: "")
        return FoundDevice(scanHit.device, type, scanHit.serviceUuid)
    }

    // ── Device type detection ─────────────────────────────────────────────────

    private fun detectDeviceTypeByServiceUuid(uuid: UUID): DiveComputerType = when (uuid) {
        HW_TELIT_SERVICE    -> DiveComputerType.HW_OSTC_TELIT
        HW_UBLOX_SERVICE    -> DiveComputerType.HW_OSTC_UBLOX
        EON_SERVICE         -> DiveComputerType.SUUNTO_EON
        SW_SERVICE          -> DiveComputerType.SHEARWATER
        MARES_SERVICE       -> DiveComputerType.MARES
        PELAGIC_A_SERVICE   -> DiveComputerType.PELAGIC_A
        PELAGIC_B_SERVICE   -> DiveComputerType.PELAGIC_B
        SCUBAPRO_SERVICE    -> DiveComputerType.SCUBAPRO
        DIVESOFT_SERVICE    -> DiveComputerType.DIVESOFT
        CRESSI_SERVICE      -> DiveComputerType.CRESSI
        NORDIC_UART_SERVICE -> DiveComputerType.GENERIC_UART
        HALCYON_SERVICE     -> DiveComputerType.UNKNOWN
        else                -> DiveComputerType.UNKNOWN
    }

    private fun detectDeviceTypeByName(name: String): DiveComputerType {
        val n = name.lowercase()
        return when {
            // Heinrichs-Weikamp OSTC — name starts with "OSTC"
            n.startsWith("ostc") -> DiveComputerType.HW_OSTC_TELIT
            // Suunto
            n.contains("eon") || n.contains("suunto") || n.startsWith("d5") -> DiveComputerType.SUUNTO_EON
            // Shearwater — all model names
            n.startsWith("perdix") || n.startsWith("teric") || n.startsWith("peregrine") ||
            n.startsWith("tern")   || n.startsWith("nerd")  || n.startsWith("petrel") ||
            n.startsWith("predator") -> DiveComputerType.SHEARWATER
            // ScubaPro
            n.startsWith("g2") || n.startsWith("g3") || n.contains("aladin") || n.contains("luna") -> DiveComputerType.SCUBAPRO
            // Mares
            n.contains("mares") || n.contains("sirius") || n.contains("genius") -> DiveComputerType.MARES
            // Cressi
            n.startsWith("caresio") || n.startsWith("goa_") || n.startsWith("neon_") || n.startsWith("nepto_") -> DiveComputerType.CRESSI
            else -> DiveComputerType.UNKNOWN
        }
    }

    // ── EON Steel / EON Core / Suunto D5 download (HDLC over BLE) ────────────
    //
    // Android handles BLE Secure Pairing passkey dialog automatically when
    // connectGatt() is called. The user sees the system dialog, types the
    // passkey shown on the dive computer, and the connection proceeds.

    // ── EON Steel / Core / D5 / G5 download (SBEM protocol over HDLC / BLE) ──
    //
    // Packet format (after HDLC decode, = USB format without 2-byte HID prefix):
    //   [cmd u16 LE][magic u32 LE][seq u16 LE][payLen u32 LE][payload...][crc32 LE]
    // Response magic = request magic + 5.
    //
    // Command flow:
    //   0x0000 CMD_INIT       payload {0x02,0x00,0x2a,0x00}
    //   0x0810 CMD_DIR_OPEN   payload {0,0,0,0} + "0:/dives\0"
    //   0x0910 CMD_DIR_READDIR (repeat until empty payload)
    //   0x0a10 CMD_DIR_CLOSE
    //   0x0010 CMD_FILE_OPEN  payload {0,0,0,0} + "0:/dives/<filename>\0"
    //   0x0710 CMD_FILE_STAT  → payload[0..3] = file size LE uint32
    //   0x0110 CMD_FILE_READ  payload {marker u32 LE, askBytes u32 LE} (repeat)
    //   0x0510 CMD_FILE_CLOSE
    //
    // Dive file format: SBEM (.LOG)
    //   Bytes 0-3:  Unix timestamp LE uint32 (dive start, UTC)
    //   Bytes 4-7:  "SBEM" magic
    //   Bytes 8-11: 0x00000000
    //   Then stream of blocks, each starting with 0x00 validation:
    //     Type descriptor: 0x00 [textLen>0] [descriptor XML] [typeId u16 LE]
    //     Data record:     0x00 0x00 [typeId u8] [dataLen u8] [bytes...]
    //   Key field paths: "Sample.Depth" (uint16 cm), "Sample.Temperature" (int16 deci-°C),
    //                    "Sample.Time" (uint16 ms delta)

    @SuppressLint("MissingPermission")
    private suspend fun downloadEon(device: BluetoothDevice): List<DiveSummary> {
        val pktCh    = Channel<ByteArray>(Channel.UNLIMITED)
        val done     = CompletableDeferred<Unit>()
        val wrDone   = Channel<Unit>(1)
        var txChar: BluetoothGattCharacteristic? = null
        val setupDone = CompletableDeferred<Unit>()
        val rxBuf     = mutableListOf<Byte>()

        setState(state.get().copy(message = "Connecting to ${device.name ?: device.address}…", progress = 22))

        val gattCb = object : BluetoothGattCallback() {
            override fun onConnectionStateChange(gatt: BluetoothGatt, status: Int, newState: Int) {
                when {
                    newState == BluetoothProfile.STATE_CONNECTED && status == BluetoothGatt.GATT_SUCCESS -> {
                        setState(state.get().copy(message = "Connected — requesting MTU…", progress = 28))
                        gatt.requestMtu(512)
                    }
                    newState == BluetoothProfile.STATE_DISCONNECTED -> {
                        if (status != BluetoothGatt.GATT_SUCCESS && !done.isCompleted)
                            done.completeExceptionally(Exception("GATT disconnected (status $status). Move closer and retry."))
                        else done.complete(Unit)
                    }
                    status != BluetoothGatt.GATT_SUCCESS && !done.isCompleted ->
                        done.completeExceptionally(Exception("GATT error $status — try toggling Bluetooth"))
                }
            }

            override fun onMtuChanged(gatt: BluetoothGatt, mtu: Int, status: Int) {
                setState(state.get().copy(message = "Connected (MTU=$mtu) — discovering services…", progress = 30))
                gatt.discoverServices()
            }

            override fun onServicesDiscovered(gatt: BluetoothGatt, status: Int) {
                if (status != BluetoothGatt.GATT_SUCCESS) {
                    done.completeExceptionally(Exception("Service discovery failed (status $status)")); return
                }
                val svc = gatt.getService(EON_SERVICE) ?: run {
                    done.completeExceptionally(Exception("EON service not found — is the device in download mode?")); return
                }
                setState(state.get().copy(message = "Pairing… confirm passkey on device if prompted", progress = 38))
                txChar = svc.getCharacteristic(EON_TX)
                val rxChar = svc.getCharacteristic(EON_RX)
                gatt.setCharacteristicNotification(rxChar, true)
                rxChar.getDescriptor(CCCD)?.also {
                    it.value = BluetoothGattDescriptor.ENABLE_NOTIFICATION_VALUE
                    gatt.writeDescriptor(it)
                } ?: setupDone.complete(Unit)
            }

            override fun onDescriptorWrite(gatt: BluetoothGatt, descriptor: BluetoothGattDescriptor, status: Int) {
                setupDone.complete(Unit)
            }

            override fun onCharacteristicWrite(gatt: BluetoothGatt, characteristic: BluetoothGattCharacteristic, status: Int) {
                wrDone.trySend(Unit)
            }

            @Deprecated("Required for API < 33")
            override fun onCharacteristicChanged(gatt: BluetoothGatt, characteristic: BluetoothGattCharacteristic) {
                rxBuf.addAll(characteristic.value.toList())
                val decoded = mutableListOf<ByteArray>()
                hdlcDecode(rxBuf, decoded)
                decoded.forEach { pktCh.trySend(it) }
            }
        }

        val gatt = device.connectGatt(this, false, gattCb, BluetoothDevice.TRANSPORT_LE)
        withTimeoutOrNull(30_000) { setupDone.await() }
            ?: run { gatt.close(); throw Exception("EON BLE setup timed out") }

        // ── Protocol helpers ──────────────────────────────────────────────────
        var eonMagic = 1L
        var eonSeq   = 0

        fun eonCrc32(data: ByteArray): ByteArray {
            val v = java.util.zip.CRC32().also { it.update(data) }.value
            return byteArrayOf((v and 0xFF).toByte(), ((v shr 8) and 0xFF).toByte(),
                               ((v shr 16) and 0xFF).toByte(), ((v shr 24) and 0xFF).toByte())
        }

        suspend fun eonSend(cmd: Int, payload: ByteArray = ByteArray(0)) {
            val pLen = payload.size
            val hdr  = ByteArray(12)
            hdr[0] = (cmd and 0xFF).toByte();       hdr[1] = ((cmd shr 8) and 0xFF).toByte()
            hdr[2] = (eonMagic and 0xFF).toByte();  hdr[3] = ((eonMagic shr 8) and 0xFF).toByte()
            hdr[4] = ((eonMagic shr 16) and 0xFF).toByte(); hdr[5] = ((eonMagic shr 24) and 0xFF).toByte()
            hdr[6] = (eonSeq and 0xFF).toByte();    hdr[7] = ((eonSeq shr 8) and 0xFF).toByte()
            hdr[8] = (pLen and 0xFF).toByte();      hdr[9] = ((pLen shr 8) and 0xFF).toByte()
            hdr[10]= ((pLen shr 16) and 0xFF).toByte(); hdr[11]= ((pLen shr 24) and 0xFF).toByte()
            val full = hdr + payload
            val pkt  = hdlcEncode(full + eonCrc32(full))
            eonMagic += 5; eonSeq++
            val tx = txChar ?: throw Exception("EON TX characteristic unavailable")
            // Send in ≤20-byte chunks; EON's BLE layer buffers bytes before HDLC decode
            for (chunk in pkt.toList().chunked(20)) {
                tx.value = chunk.toByteArray()
                gatt.writeCharacteristic(tx)
                withTimeoutOrNull(5_000) { wrDone.receive() }
            }
        }

        suspend fun eonRecv(ms: Long = 10_000) = withTimeoutOrNull(ms) { pktCh.receive() }

        fun eonPayload(pkt: ByteArray): ByteArray? {
            if (pkt.size < 12) return null
            val pLen = (pkt[8].toLong() and 0xFF) or ((pkt[9].toLong() and 0xFF) shl 8) or
                       ((pkt[10].toLong() and 0xFF) shl 16) or ((pkt[11].toLong() and 0xFF) shl 24)
            val end = (12 + pLen).toInt()
            return if (end <= pkt.size) pkt.copyOfRange(12, end) else null
        }

        return try {
            // 1. CMD_INIT
            setState(state.get().copy(message = "Initializing EON connection…", progress = 42))
            eonSend(0x0000, byteArrayOf(0x02, 0x00, 0x2a, 0x00))
            eonRecv(10_000) ?: throw Exception("EON init timeout — put device in Bluetooth mode")

            // 2. CMD_DIR_OPEN  "0:/dives"
            setState(state.get().copy(message = "Opening dive directory…", progress = 48))
            eonSend(0x0810, byteArrayOf(0, 0, 0, 0) + "0:/dives\u0000".toByteArray(Charsets.US_ASCII))
            eonRecv(5_000) ?: throw Exception("EON dir open timeout")

            // 3. CMD_DIR_READDIR — repeat until empty payload
            val diveFiles = mutableListOf<String>()
            setState(state.get().copy(message = "Reading dive list…", progress = 52))
            while (true) {
                eonSend(0x0910)
                val r = eonRecv(5_000) ?: break
                val pay = eonPayload(r) ?: break
                if (pay.isEmpty()) break
                var p = 0
                while (p + 8 <= pay.size) {
                    val type    = (pay[p].toLong() and 0xFF) or ((pay[p+1].toLong() and 0xFF) shl 8) or
                                  ((pay[p+2].toLong() and 0xFF) shl 16) or ((pay[p+3].toLong() and 0xFF) shl 24)
                    val nameLen = ((pay[p+4].toInt() and 0xFF) or ((pay[p+5].toInt() and 0xFF) shl 8) or
                                  ((pay[p+6].toInt() and 0xFF) shl 16) or ((pay[p+7].toInt() and 0xFF) shl 24))
                    p += 8
                    if (nameLen <= 0 || p + nameLen > pay.size) break
                    val name = String(pay, p, nameLen).trimEnd('\u0000')
                    p += nameLen
                    if (type == 1L && name.uppercase().endsWith(".LOG")) diveFiles.add(name)
                }
            }
            // CMD_DIR_CLOSE
            eonSend(0x0a10); eonRecv(3_000)
            Log.i(TAG, "EON: ${diveFiles.size} dive files: $diveFiles")
            if (diveFiles.isEmpty()) throw Exception("No dives found on device")

            // 4. Download + parse each dive file
            val dives = mutableListOf<DiveSummary>()
            diveFiles.sorted().forEachIndexed { idx, fn ->
                setState(state.get().copy(
                    message  = "Downloading dive ${idx+1}/${diveFiles.size}…",
                    progress = 55 + 20 * (idx+1) / diveFiles.size.coerceAtLeast(1)
                ))
                try {
                    // CMD_FILE_OPEN
                    eonSend(0x0010, byteArrayOf(0,0,0,0) + "0:/dives/$fn\u0000".toByteArray(Charsets.US_ASCII))
                    eonRecv(5_000)
                    // CMD_FILE_STAT → file size
                    eonSend(0x0710)
                    val statPay = eonRecv(5_000)?.let { eonPayload(it) }
                    val fSize = if (statPay != null && statPay.size >= 4)
                        (statPay[0].toLong() and 0xFF) or ((statPay[1].toLong() and 0xFF) shl 8) or
                        ((statPay[2].toLong() and 0xFF) shl 16) or ((statPay[3].toLong() and 0xFF) shl 24)
                    else 0L
                    if (fSize < 13 || fSize > 2_000_000L) {
                        Log.w(TAG, "EON $fn: bad file size $fSize, skipping")
                        eonSend(0x0510); eonRecv(3_000); return@forEachIndexed
                    }
                    // CMD_FILE_READ in 1024-byte chunks (marker starts at 1234, per libdivecomputer)
                    val fileData = mutableListOf<Byte>()
                    var marker = 1234
                    while (fileData.size < fSize) {
                        val ask = 1024.coerceAtMost((fSize - fileData.size).toInt())
                        eonSend(0x0110, byteArrayOf(
                            (marker and 0xFF).toByte(), ((marker shr 8) and 0xFF).toByte(),
                            ((marker shr 16) and 0xFF).toByte(), ((marker shr 24) and 0xFF).toByte(),
                            (ask and 0xFF).toByte(), ((ask shr 8) and 0xFF).toByte(),
                            ((ask shr 16) and 0xFF).toByte(), ((ask shr 24) and 0xFF).toByte()
                        ))
                        val rPay = eonRecv(15_000)?.let { eonPayload(it) } ?: break
                        if (rPay.isEmpty()) break
                        fileData.addAll(rPay.toList())
                        marker++
                    }
                    // CMD_FILE_CLOSE
                    eonSend(0x0510); eonRecv(3_000)
                    Log.i(TAG, "EON $fn: ${fileData.size} bytes (expected $fSize)")
                    parseSbemFile(fileData.toByteArray(), fn, idx + 1, device.name ?: "Suunto EON")
                        ?.let { dives.add(it) }
                } catch (ex: Exception) {
                    Log.w(TAG, "EON $fn: ${ex.message}")
                    runCatching { eonSend(0x0510); eonRecv(1_000) }
                }
            }

            gatt.disconnect()
            runCatching { withTimeoutOrNull(5_000) { done.await() } }
            gatt.close()
            dives
        } catch (e: Exception) {
            runCatching { gatt.disconnect() }
            runCatching { withTimeoutOrNull(3_000) { done.await() } }
            gatt.close()
            throw e
        }
    }

    // ── Shearwater download (SLIP over BLE, Shearwater proprietary protocol) ──
    //
    // Wire format (from libdivecomputer shearwater_petrel.c / shearwater_common.c):
    //   Packet = SLIP( [0xFF, 0x01, payloadLen+1, 0x00, ...payload] )
    //   BLE write = [nChunks, chunkIdx] + up to 30 bytes of SLIP data per write
    //   BLE notify = same 2-byte header, must be stripped before SLIP decode
    //   Write type = WRITE_WITHOUT_RESPONSE (no onCharacteristicWrite callback)
    //
    // Command flow:
    //   RDBI 0x8011 → firmware version
    //   RDBI 0x8021 → manifest address + dive count
    //   0x35 init + 0x36 block × N + 0x37 quit → download manifest
    //   Manifest: 32-byte entries with address/size/date/depth/duration/temp

    @SuppressLint("MissingPermission")
    private suspend fun downloadShearwater(device: BluetoothDevice): List<DiveSummary> {
        val pktCh    = Channel<ByteArray>(Channel.UNLIMITED)
        val done     = CompletableDeferred<Unit>()
        val charRef  = CompletableDeferred<BluetoothGattCharacteristic>()
        var frameSeq = 0
        // Frame reassembly: each BLE notification = [nframes, frameIdx, data...]
        var swExpected = 0
        val swChunks   = mutableListOf<ByteArray>()

        setState(state.get().copy(message = "Connecting to ${device.name ?: device.address}…", progress = 22))

        val gattCb = object : BluetoothGattCallback() {
            override fun onConnectionStateChange(gatt: BluetoothGatt, status: Int, newState: Int) {
                when {
                    newState == BluetoothProfile.STATE_CONNECTED && status == BluetoothGatt.GATT_SUCCESS -> {
                        setState(state.get().copy(message = "Connected — discovering services…", progress = 30))
                        gatt.discoverServices()
                    }
                    newState == BluetoothProfile.STATE_DISCONNECTED -> {
                        if (!done.isCompleted) {
                            if (status != BluetoothGatt.GATT_SUCCESS)
                                done.completeExceptionally(Exception("GATT disconnected (status $status)"))
                            else done.complete(Unit)
                        }
                    }
                    status != BluetoothGatt.GATT_SUCCESS && !done.isCompleted ->
                        done.completeExceptionally(Exception("GATT error $status — try toggling Bluetooth"))
                }
            }

            override fun onServicesDiscovered(gatt: BluetoothGatt, status: Int) {
                if (status != BluetoothGatt.GATT_SUCCESS) {
                    done.completeExceptionally(Exception("Service discovery failed (status $status)"))
                    return
                }
                val char = gatt.getService(SW_SERVICE)?.getCharacteristic(SW_CHAR) ?: run {
                    done.completeExceptionally(Exception("Shearwater service not found — put device in Bluetooth mode"))
                    return
                }
                char.writeType = BluetoothGattCharacteristic.WRITE_TYPE_NO_RESPONSE
                setState(state.get().copy(message = "Enabling notifications…", progress = 36))
                gatt.setCharacteristicNotification(char, true)
                char.getDescriptor(CCCD)?.also {
                    it.value = BluetoothGattDescriptor.ENABLE_NOTIFICATION_VALUE
                    gatt.writeDescriptor(it)
                } ?: charRef.complete(char)
            }

            override fun onDescriptorWrite(gatt: BluetoothGatt, descriptor: BluetoothGattDescriptor, status: Int) {
                gatt.getService(SW_SERVICE)?.getCharacteristic(SW_CHAR)?.let { charRef.complete(it) }
            }

            @Deprecated("Required for API < 33")
            override fun onCharacteristicChanged(gatt: BluetoothGatt, characteristic: BluetoothGattCharacteristic) {
                val data = characteristic.value
                if (data.size < 3) return
                val nframes  = data[0].toInt() and 0xFF
                val frameIdx = data[1].toInt() and 0xFF
                val chunk    = data.copyOfRange(2, data.size)
                Log.d(TAG, "SW notify($nframes/$frameIdx/${data.size}): ${data.take(8).map { "0x${it.toUByte().toString(16)}" }}")
                if (frameIdx == 0) {
                    swChunks.clear()
                    swExpected = nframes
                }
                if (frameIdx == swChunks.size) swChunks.add(chunk)
                if (swChunks.size >= swExpected && swExpected > 0) {
                    val pkt = swChunks.fold(ByteArray(0)) { acc, b -> acc + b }
                    Log.d(TAG, "SW packet(${pkt.size}): ${pkt.take(12).map { "0x${it.toUByte().toString(16)}" }}")
                    pktCh.trySend(pkt)
                    swChunks.clear()
                    swExpected = 0
                }
            }
        }

        val gatt = device.connectGatt(this, false, gattCb, BluetoothDevice.TRANSPORT_LE)
        val char = withTimeoutOrNull(15_000) { charRef.await() }
            ?: run { gatt.close(); throw Exception("Shearwater BLE setup timed out") }

        // ── Bonding (required for download commands) ──────────────────────────
        if (device.bondState != BluetoothDevice.BOND_BONDED) {
            setState(state.get().copy(message = "Pairing with device — approve on phone…", progress = 38))
            val bondDone = CompletableDeferred<Boolean>()
            val bondReceiver = object : BroadcastReceiver() {
                override fun onReceive(ctx: Context, intent: Intent) {
                    if (intent.getParcelableExtra<BluetoothDevice>(BluetoothDevice.EXTRA_DEVICE)?.address != device.address) return
                    when (intent.getIntExtra(BluetoothDevice.EXTRA_BOND_STATE, -1)) {
                        BluetoothDevice.BOND_BONDED  -> bondDone.complete(true)
                        BluetoothDevice.BOND_NONE    -> bondDone.complete(false)
                    }
                }
            }
            registerReceiver(bondReceiver, IntentFilter(BluetoothDevice.ACTION_BOND_STATE_CHANGED))
            try {
                device.createBond()
                val bonded = withTimeoutOrNull(30_000) { bondDone.await() }
                if (bonded != true) {
                    gatt.disconnect(); gatt.close()
                    throw Exception("Pairing failed — please try again and approve the pairing request")
                }
                delay(1_000) // brief settle after bond
            } finally {
                unregisterReceiver(bondReceiver)
            }
        }

        // ── Protocol helpers ─────────────────────────────────────────────────
        // Small delay between chunks avoids Android BLE TX-queue overflow
        // when using WRITE_TYPE_NO_RESPONSE (no onCharacteristicWrite callback).
        suspend fun swSend(payload: ByteArray) {
            val slip   = slipEncode(byteArrayOf(0xFF.toByte(), 0x01, (payload.size + 1).toByte(), 0x00) + payload)
            val chunks = slip.toList().chunked(30)
            for ((i, chunk) in chunks.withIndex()) {
                char.value = byteArrayOf(chunks.size.toByte(), (frameSeq + i).toByte()) + chunk.toByteArray()
                gatt.writeCharacteristic(char)
                if (chunks.size > 1) delay(20) // give BLE stack time between chunks
            }
            frameSeq += chunks.size
        }

        // Response packets: [0x01, 0xFF, len, 0x00, payload...] (confirmed from logcat)
        fun swPayload(pkt: ByteArray): ByteArray? =
            if (pkt.size >= 5 && pkt[0] == 0x01.toByte() && pkt[1] == 0xFF.toByte() && pkt[3] == 0x00.toByte())
                pkt.copyOfRange(4, pkt.size)
            else { Log.w(TAG, "SW unexpected pkt header: ${pkt.take(4).map { "0x${it.toUByte().toString(16)}" }}"); null }

        suspend fun swRecv(ms: Long = 10_000): ByteArray? =
            withTimeoutOrNull(ms) { pktCh.receive() }

        // Byte-packing helpers used by swDownload and the manifest/profile sends.
        fun Int.b3() = byteArrayOf(((this shr 16) and 0xFF).toByte(), ((this shr 8) and 0xFF).toByte(), (this and 0xFF).toByte())
        fun Long.b4() = byteArrayOf(((this shr 24) and 0xFF).toByte(), ((this shr 16) and 0xFF).toByte(), ((this shr 8) and 0xFF).toByte(), (this and 0xFF).toByte())

        // ── Decompression (shearwater_common_decompress_lre / _xor) ──────────
        // LRE: data is a stream of 9-bit values.
        //   bit8 set   → raw byte (bits 0-7 appended as-is)
        //   bit8 clear, value > 0 → run of `value` zero bytes
        //   value == 0 → end of stream
        fun decompressLRE(data: ByteArray): ByteArray? {
            val nbits = data.size * 8
            if (nbits % 9 != 0) return null
            val result = mutableListOf<Byte>()
            var offset = 0
            while (offset + 9 <= nbits) {
                val byteIdx = offset / 8
                val bitIdx  = offset % 8
                val shift   = 16 - (bitIdx + 9)
                val b0 = if (byteIdx < data.size)         (data[byteIdx].toInt()       and 0xFF) else 0
                val b1 = if (byteIdx + 1 < data.size)     (data[byteIdx + 1].toInt()   and 0xFF) else 0
                val value = ((b0 shl 8) or b1) ushr shift and 0x1FF
                when {
                    value and 0x100 != 0 -> result.add((value and 0xFF).toByte())
                    value == 0           -> break  // end of compressed stream
                    else                 -> repeat(value) { result.add(0) }
                }
                offset += 9
            }
            return result.toByteArray()
        }

        // XOR: each 32-byte block XOR'd in-place with the previous block.
        fun decompressXOR(data: ByteArray) {
            for (i in 32 until data.size) {
                data[i] = (data[i].toInt() xor data[i - 32].toInt()).toByte()
            }
        }

        // ── swDownload — mirrors shearwater_common_download ──────────────────
        // Sends RequestUpload (0x35), fetches blocks (0x36), closes (0x37).
        // When useCompression=true the raw response is LRE-decompressed then XOR-expanded.
        suspend fun swDownload(address: Long, maxSize: Int, useCompression: Boolean): ByteArray? {
            val compFlag = if (useCompression) 0x10.toByte() else 0x00.toByte()
            swSend(byteArrayOf(0x35, compFlag, 0x34) + address.b4() + maxSize.b3())
            val initPkt = swRecv(8_000) ?: return null
            val initPay = swPayload(initPkt) ?: return null
            // 0x75 = positive response to RequestUpload
            if (initPay.isEmpty() || initPay[0] != 0x75.toByte()) {
                Log.w(TAG, "SW swDownload init failed: ${initPay.take(4).map { "0x${it.toUByte().toString(16)}" }}")
                return null
            }

            val rawData = mutableListOf<Byte>()
            var block = 1
            while (rawData.size < maxSize) {
                swSend(byteArrayOf(0x36, (block and 0xFF).toByte()))
                val pkt = swRecv(5_000) ?: break
                val pay = swPayload(pkt) ?: break
                // 0x76 = TransferData response; byte[1] is the echoed block sequence number
                if (pay.size < 2 || pay[0] != 0x76.toByte() || pay[1] != (block and 0xFF).toByte()) break
                rawData.addAll(pay.drop(2))
                block++
            }

            // Close UDS session: drain any stale packet, then send RequestTransferExit (0x37)
            swRecv(300)
            swSend(byteArrayOf(0x37))
            swRecv(3_000)

            val raw = rawData.toByteArray()
            if (!useCompression) return raw
            // Apply LRE then XOR decompression (shearwater_common_decompress_lre/xor)
            val lre = decompressLRE(raw) ?: run {
                Log.w(TAG, "SW LRE decompression failed (${raw.size} bytes)")
                return null
            }
            decompressXOR(lre)
            return lre
        }

        return try {
            // ── 1. Firmware version ───────────────────────────────────────────
            setState(state.get().copy(message = "Reading firmware version…", progress = 40))
            swSend(byteArrayOf(0x22, 0x80.toByte(), 0x11)) // RDBI ID_FIRMWARE
            val fwPkt = swRecv(8_000)
            Log.i(TAG, "SW firmware pkt: ${fwPkt?.map { "0x${it.toUByte().toString(16)}" }}")

            // ── 2. Log upload info → manifest address + dive count ────────────
            setState(state.get().copy(message = "Requesting dive manifest info…", progress = 45))
            swSend(byteArrayOf(0x22, 0x80.toByte(), 0x21)) // RDBI ID_LOGUPLOAD
            val logPkt     = swRecv(8_000)
            val logPayload = logPkt?.let { swPayload(it) }
            Log.i(TAG, "SW logupload: ${logPayload?.map { "0x${it.toUByte().toString(16)}" }}")

            // logupload[4..7] = profile region base address (doubles as format indicator):
            //   0x80000000 = Perdix / Petrel 2 (new native format); manifest at 0xE0000000
            //   0xC0000000 = Predator / Petrel 1 (old format);      manifest at 0x80000000
            // Manifest for new format is ALWAYS at 0xE0000000 with fixed size 0x600
            var formatAddr = 0xC0000000L
            if (logPayload != null && logPayload.size >= 8 && logPayload[0] == 0x62.toByte()) {
                formatAddr = ((logPayload[4].toLong() and 0xFF) shl 24) or
                             ((logPayload[5].toLong() and 0xFF) shl 16) or
                             ((logPayload[6].toLong() and 0xFF) shl 8)  or
                             (logPayload[7].toLong() and 0xFF)
            }
            Log.i(TAG, "SW logbook format indicator: 0x${formatAddr.toString(16)}")

            // ── 3 & 4. Download manifest (single session) + parse entries ───────
            // libdivecomputer downloads the entire manifest in ONE 0x35 session.
            // The device NAKs the next 0x36 after the last valid block, signalling
            // end-of-data. We do NOT send 0x37 to close — on real hardware the device
            // exits download mode immediately at the NAK and ignores the 0x37 anyway,
            // causing a 3 s timeout before the first profile 0x35 can be sent.
            val ENTRY_SIZE   = 30       // Perdix V87: 30 bytes per BLE block = 1 entry
            val manifestAddr = 0xE0000000L
            val manifestSize = 0x600    // 1536 bytes — libdivecomputer constant

            // Entry layout (30 bytes over BLE, 32 bytes on flash — last 2 stripped):
            //  [0..1]   0xA5C4 = valid, 0x5A23 = deleted
            //  [4..7]   dive START Unix timestamp (BE)
            //  [8..11]  dive END   Unix timestamp (BE)
            //  [20..23] dive record offset (BE) — add to formatAddr for absolute address
            val sdf = java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.US)
                .apply { timeZone = java.util.TimeZone.getTimeZone("UTC") }
            val dives = mutableListOf<DiveSummary>()
            var seq = 0

            setState(state.get().copy(message = "Downloading dive list…", progress = 52))
            swSend(byteArrayOf(0x35, 0x00, 0x34) + manifestAddr.b4() + manifestSize.b3())
            val manifestInitAck = swRecv(8_000)
            Log.i(TAG, "SW manifest init: ${manifestInitAck?.take(8)?.map { "0x${it.toUByte().toString(16)}" }}")
            if (manifestInitAck == null) throw Exception("Manifest init timeout — put device in Bluetooth mode")

            // Parse maxBlockLen from 0x75 response to know exactly how many blocks to
            // request.  Sending one block beyond the last valid entry triggers a UDS
            // requestSequenceError (NRC 0x24) which causes the Perdix to disconnect.
            // Standard UDS: lengthFormatIdentifier bits[7:4] = #bytes for maxBlockLen.
            val initPay = swPayload(manifestInitAck)
            val maxBlockLen = if (initPay != null && initPay.size >= 3) {
                val nLenBytes = (initPay[1].toInt() and 0xF0) shr 4
                if (nLenBytes == 1 && initPay.size >= 3) (initPay[2].toInt() and 0xFF) else 128
            } else 128
            val maxDataPerBlock = (maxBlockLen - 2).coerceAtLeast(1)   // minus 0x76 + seqNum
            // The Perdix sends one 30-byte entry per block. Requesting a block beyond the last
            // valid entry triggers NRC 0x24, which locks the device UDS state for the session —
            // no subsequent 0x35 will be accepted.  Cap at ceil(manifestSize/maxDataPerBlock)
            // so we never send an out-of-bounds request.
            val expectedBlocks = (manifestSize + maxDataPerBlock - 1) / maxDataPerBlock
            Log.i(TAG, "SW manifest: maxBlockLen=$maxBlockLen maxDataPerBlock=$maxDataPerBlock expectedBlocks=$expectedBlocks")

            val manifestData = mutableListOf<Byte>()
            var blockNum = 1
            var gotNrc24 = false    // device sent NRC 0x24 to a 0x36 block request
            var naturalEnd = false  // block request timed out — device ended transfer silently
            while (blockNum <= expectedBlocks) {
                swSend(byteArrayOf(0x36, (blockNum and 0xFF).toByte()))
                val blkPkt = swRecv(2_000)
                if (blkPkt == null) { naturalEnd = true; break }
                val blkPay = swPayload(blkPkt)
                if (blkPay != null && blkPay.isNotEmpty() && blkPay[0] == 0x76.toByte()) {
                    manifestData.addAll(blkPay.drop(2))
                    blockNum++
                } else {
                    if (blkPay != null && blkPay.size >= 3 &&
                            blkPay[0] == 0x7f.toByte() && blkPay[2] == 0x24.toByte()) {
                        gotNrc24 = true
                        Log.i(TAG, "SW manifest: NRC 0x24 at block $blockNum (device has fewer entries than cap)")
                    } else {
                        Log.w(TAG, "SW manifest unexpected end at block $blockNum: ${blkPay?.take(4)?.map { "0x${it.toUByte().toString(16)}" }}")
                    }
                    break
                }
            }
            when {
                naturalEnd -> {
                    // Device ended the transfer silently after the last valid block — no active
                    // session remains.  Sending 0x37 here would get NRC 0x24 and lock the device
                    // UDS state, preventing subsequent profile 0x35 requests.
                    // Drain any late NRC that may arrive, then let the device settle.
                    Log.i(TAG, "SW manifest: natural end after ${blockNum - 1} blocks (no 0x37 sent)")
                    swRecv(500)
                    delay(400)
                }
                gotNrc24 -> {
                    // NRC 0x24 on a 0x36 — device locked its UDS state.  Send 0x37 immediately
                    // (tight-window recovery) and give extra time to recover.
                    swSend(byteArrayOf(0x37))
                    val quitResp = swRecv(1_000)
                    Log.i(TAG, "SW manifest NRC-path quit resp: ${quitResp?.let { swPayload(it)?.take(2)?.map { b -> "0x${b.toUByte().toString(16)}" } }}")
                    delay(1_500)
                }
                else -> {
                    // All expectedBlocks received — close cleanly with 0x37.
                    swRecv(300)
                    swSend(byteArrayOf(0x37))
                    val quitResp = swRecv(5_000)
                    Log.i(TAG, "SW manifest quit resp: ${quitResp?.let { swPayload(it)?.take(2)?.map { b -> "0x${b.toUByte().toString(16)}" } }}")
                    delay(400)
                }
            }
            Log.i(TAG, "SW manifest: ${manifestData.size} bytes, ${blockNum - 1} blocks  BLE=${if (done.isCompleted) "DISCONNECTED" else "ok"}")

            // Parse all entries from the flat manifest buffer
            var off = 0
            while (off + ENTRY_SIZE <= manifestData.size) {
                val e = manifestData.subList(off, off + ENTRY_SIZE).toByteArray()
                off += ENTRY_SIZE
                val hdr = ((e[0].toInt() and 0xFF) shl 8) or (e[1].toInt() and 0xFF)
                if (hdr != 0xA5C4) continue
                Log.i(TAG, "SW entry: ${e.map { "0x${it.toUByte().toString(16)}" }}")
                try {
                    val startTs = ((e[4].toLong() and 0xFF) shl 24) or
                                  ((e[5].toLong() and 0xFF) shl 16) or
                                  ((e[6].toLong() and 0xFF) shl 8)  or
                                   (e[7].toLong() and 0xFF)
                    val endTs   = ((e[8].toLong() and 0xFF) shl 24) or
                                  ((e[9].toLong() and 0xFF) shl 16) or
                                  ((e[10].toLong() and 0xFF) shl 8) or
                                   (e[11].toLong() and 0xFF)
                    val durSec  = (endTs - startTs).toInt().coerceIn(0, 86400)
                    val year    = java.util.Calendar.getInstance(java.util.TimeZone.getTimeZone("UTC"))
                        .apply { time = java.util.Date(startTs * 1000L) }
                        .get(java.util.Calendar.YEAR)
                    if (startTs < 1_000_000_000L || year !in 2010..2035) {
                        Log.w(TAG, "SW entry: timestamp out of range ($startTs)"); continue
                    }
                    seq++
                    val divedAt = sdf.format(java.util.Date(startTs * 1000L))
                    val recAddr = ((e[20].toLong() and 0xFF) shl 24) or
                                  ((e[21].toLong() and 0xFF) shl 16) or
                                  ((e[22].toLong() and 0xFF) shl 8)  or
                                   (e[23].toLong() and 0xFF)
                    dives.add(DiveSummary(
                        source     = device.name ?: "Shearwater",
                        number     = seq,
                        divedAt    = divedAt,
                        maxDepthM  = 0.0,
                        durationS  = durSec,
                        tempC      = null,
                        recordAddr = recAddr
                    ))
                    Log.i(TAG, "SW dive $seq: $divedAt dur=${durSec}s recOff=0x${recAddr.toString(16)}")
                } catch (ex: Exception) {
                    Log.w(TAG, "SW entry parse: ${ex.message}")
                }
            }
            Log.i(TAG, "SW manifest parsed: ${dives.size} dives")

            // ── 5. Download individual dive profiles ─────────────────────────
            // Each manifest entry has a record address at bytes[20..23].
            // Request that block and parse 32-byte samples:
            //   [0..1]  depth uint16 BE / 10 → metres
            //   [13]    temperature int8     → °C
            // Sample interval: 10 s (Perdix default).
            val PROF_SAMPLE_SIZE = 32
            val PROF_HEADER_SIZE = 128
            val PROF_INTERVAL_S  = 10

            if (done.isCompleted) throw Exception("BLE disconnected after manifest — device may require reconnect")

            // The logupload response byte[4..7] (formatAddr) doubles as the profile region
            // base address.  For this Perdix "V87 Classic" it is 0x80000000.  Manifest
            // entries store byte offsets from that base.
            //   addr = formatAddr + recAddr
            // Entries with recAddr >= 0x10000000 are already in a high-address region
            // (manifest range 0xE0000000, or SLIP artefacts like 0xDC882B00) — skip them.
            val divesFinal = dives.mapIndexed { idx, dive ->
                if (dive.recordAddr >= 0x10000000L) {
                    Log.w(TAG, "SW profile ${idx + 1}: recAddr=0x${dive.recordAddr.toString(16)} high-addr, skipping")
                    return@mapIndexed dive
                }
                val addr = (formatAddr + dive.recordAddr) and 0xFFFFFFFFL
                Log.i(TAG, "SW profile ${idx + 1}/${dives.size}: addr=0x${addr.toString(16)} (base=0x${formatAddr.toString(16)} off=0x${dive.recordAddr.toString(16)})")
                setState(state.get().copy(
                    message  = "Downloading profile ${idx + 1}/${dives.size}…",
                    progress = 70 + idx * 8 / dives.size.coerceAtLeast(1)
                ))

                // 256 KB — generous enough for any dive profile yet small enough that
                // address + maxSz stays within the Perdix profile flash region.
                // (0xFFFFFF / 16 MB caused NRC 0x31 on every address.)
                val maxSz = 0x040000

                // Perdix/new-format (0x80000000) stores profiles uncompressed (0x00).
                // Old-format Predator/Petrel-1 (0xC0000000) uses LRE+XOR compression (0x10).
                val useComp = formatAddr == 0xC0000000L
                val profBytes = swDownload(addr, maxSz, useComp)
                Log.i(TAG, "SW profile ${idx + 1}: ${profBytes?.size ?: 0} bytes (comp=$useComp)")

                if (profBytes == null || profBytes.size < PROF_HEADER_SIZE + PROF_SAMPLE_SIZE) {
                    Log.w(TAG, "SW profile ${idx + 1}: too small or null (${profBytes?.size} bytes)")
                    return@mapIndexed dive
                }
                Log.d(TAG, "SW rec hdr[0..63]: ${profBytes.take(64).map { "0x${it.toUByte().toString(16)}" }}")
                Log.d(TAG, "SW rec sample@128: ${profBytes.drop(PROF_HEADER_SIZE).take(PROF_SAMPLE_SIZE).map { "0x${it.toUByte().toString(16)}" }}")

                // PNF (Petrel Native Format): first 2 bytes are NOT both 0xFF.
                // In PNF: byte[0] of each 32-byte block is the record type
                //   0x01 = sample  |  0x10-0x19 = opening  |  0x20-0x29 = closing
                // Depth offset: +1 in PNF (after type byte), +0 in legacy.
                // Temp offset: pnf+13 (signed int8, °C).
                val isPnf = !((profBytes[0].toInt() and 0xFF == 0xFF) && (profBytes[1].toInt() and 0xFF == 0xFF))
                val pnf   = if (isPnf) 1 else 0
                Log.i(TAG, "SW profile ${idx + 1}: format=${if (isPnf) "PNF" else "legacy"}")

                val samples = mutableListOf<DiveSample>()
                var off = PROF_HEADER_SIZE
                var t = 0
                val maxT = dive.durationS + 120
                while (off + PROF_SAMPLE_SIZE <= profBytes.size && t <= maxT) {
                    val s = profBytes.copyOfRange(off, off + PROF_SAMPLE_SIZE)
                    off += PROF_SAMPLE_SIZE
                    // Skip opening/closing blocks in PNF mode — do NOT advance time
                    if (isPnf && (s[0].toInt() and 0xFF) != 0x01) continue
                    val depthM = (((s[pnf].toInt() and 0xFF) shl 8) or (s[pnf + 1].toInt() and 0xFF)) / 10.0
                    // Temperature: signed int8; libdivecomputer fixup for wrapped negatives
                    var tempRaw = s[pnf + 13].toInt()   // Kotlin Byte.toInt() sign-extends
                    if (tempRaw < 0) { tempRaw += 102; if (tempRaw > 0) tempRaw = 0 }
                    val tempC  = tempRaw.toDouble()
                    samples.add(DiveSample(t, depthM, tempC.takeIf { it in -5.0..50.0 }))
                    t += PROF_INTERVAL_S
                }
                Log.i(TAG, "SW profile ${idx + 1}: ${samples.size} samples maxD=${samples.maxOfOrNull { it.depthM }?.let { "%.1f".format(it) }}m")

                dive.copy(
                    maxDepthM = samples.maxOfOrNull { it.depthM } ?: 0.0,
                    tempC     = samples.mapNotNull { it.tempC }.minOrNull(),
                    samples   = samples
                )
            }

            gatt.disconnect()
            runCatching { withTimeoutOrNull(5_000) { done.await() } }
            gatt.close()
            divesFinal
        } catch (e: Exception) {
            runCatching { gatt.disconnect() }
            runCatching { withTimeoutOrNull(3_000) { done.await() } }
            gatt.close()
            throw e
        }
    }

    // ── Heinrichs-Weikamp OSTC download — Telit Terminal I/O + OSTC command protocol
    //
    // BLE transport: Terminal I/O credit-based flow control (Subsurface qt-ble.cpp)
    //   DATA_RX (write)     — phone sends command bytes to device
    //   DATA_TX (notify)    — device sends response bytes to phone
    //   CREDITS_TX (indicate) — device reports credit consumption
    //   CREDITS_RX (write)  — phone tops up device receive budget
    //
    // OSTC command layer (libdivecomputer hw_ostc3.c):
    //   0xBB         → INIT: echo 0xBB + READY(0x4D)
    //   0x6D         → COMPACT logbook: echo 0x6D + 4096 bytes + READY(0x4D)
    //   0x66 + [idx] → DIVE: echo 0x66 + <length> bytes + READY(0x4D)
    //   0xFF         → EXIT

    @SuppressLint("MissingPermission")
    private suspend fun downloadHwOstcTelit(device: BluetoothDevice): List<DiveSummary> {
        val rxChannel  = Channel<Byte>(Channel.UNLIMITED)
        val dataWrDone = Channel<Unit>(1)
        val credWrDone = Channel<Unit>(1)
        val done       = CompletableDeferred<Unit>()
        var hwCredits  = HW_MAXIMAL_CREDIT
        var dataRxChar:    BluetoothGattCharacteristic? = null
        var creditsRxChar: BluetoothGattCharacteristic? = null
        val setupDone  = CompletableDeferred<Unit>()
        var setupStep  = 0

        setState(state.get().copy(message = "Connecting to OSTC…", progress = 25))

        val gattCb = object : BluetoothGattCallback() {
            override fun onConnectionStateChange(gatt: BluetoothGatt, status: Int, newState: Int) {
                if (newState == BluetoothProfile.STATE_CONNECTED) {
                    setState(state.get().copy(message = "Connected — discovering services…", progress = 32))
                    gatt.discoverServices()
                } else if (newState == BluetoothProfile.STATE_DISCONNECTED) done.complete(Unit)
            }

            override fun onServicesDiscovered(gatt: BluetoothGatt, status: Int) {
                val svc = gatt.getService(HW_TELIT_SERVICE)
                    ?: run { done.completeExceptionally(Exception("HW Telit service not found")); return }
                dataRxChar    = svc.getCharacteristic(HW_TELIT_DATA_RX)
                creditsRxChar = svc.getCharacteristic(HW_TELIT_CREDITS_RX)
                val creditsTx = svc.getCharacteristic(HW_TELIT_CREDITS_TX)
                gatt.setCharacteristicNotification(creditsTx, true)
                creditsTx.getDescriptor(CCCD).also {
                    it.value = BluetoothGattDescriptor.ENABLE_INDICATION_VALUE
                    setupStep = 0; gatt.writeDescriptor(it)
                }
            }

            override fun onDescriptorWrite(gatt: BluetoothGatt, descriptor: BluetoothGattDescriptor, status: Int) {
                when (setupStep) {
                    0 -> {
                        val dataTx = gatt.getService(HW_TELIT_SERVICE)?.getCharacteristic(HW_TELIT_DATA_TX) ?: return
                        gatt.setCharacteristicNotification(dataTx, true)
                        dataTx.getDescriptor(CCCD).also {
                            it.value = BluetoothGattDescriptor.ENABLE_NOTIFICATION_VALUE
                            setupStep = 1; gatt.writeDescriptor(it)
                        }
                    }
                    1 -> {
                        creditsRxChar?.let {
                            it.value = byteArrayOf(HW_MAXIMAL_CREDIT.toByte())
                            setupStep = 2; gatt.writeCharacteristic(it)
                        }
                    }
                }
            }

            override fun onCharacteristicWrite(gatt: BluetoothGatt, characteristic: BluetoothGattCharacteristic, status: Int) {
                when (characteristic.uuid) {
                    HW_TELIT_DATA_RX    -> dataWrDone.trySend(Unit)
                    HW_TELIT_CREDITS_RX -> {
                        if (setupStep == 2) { setupStep = 3; setupDone.complete(Unit) }
                        else credWrDone.trySend(Unit)
                    }
                }
            }

            @Deprecated("Required for API < 33")
            override fun onCharacteristicChanged(gatt: BluetoothGatt, characteristic: BluetoothGattCharacteristic) {
                if (characteristic.uuid == HW_TELIT_DATA_TX) {
                    characteristic.value.forEach { rxChannel.trySend(it) }
                    hwCredits -= characteristic.value.size
                    if (hwCredits <= HW_MINIMAL_CREDIT) {
                        val topUp = (HW_MAXIMAL_CREDIT - hwCredits).coerceAtMost(255)
                        hwCredits += topUp
                        creditsRxChar?.let { it.value = byteArrayOf(topUp.toByte()); gatt.writeCharacteristic(it) }
                    }
                }
            }
        }

        val gatt = device.connectGatt(this, false, gattCb, BluetoothDevice.TRANSPORT_LE)
        withTimeoutOrNull(30_000) { setupDone.await() }
            ?: run { gatt.close(); throw Exception("OSTC Terminal I/O setup timed out") }

        setState(state.get().copy(message = "OSTC ready — starting download…", progress = 40))

        // ── Serial I/O helpers ──────────────────────────────────────────────
        suspend fun writeCmd(vararg bytes: Byte) {
            val ch = dataRxChar ?: return
            bytes.toList().chunked(20).forEach { chunk ->
                ch.value = chunk.toByteArray()
                gatt.writeCharacteristic(ch)
                withTimeout(5_000) { dataWrDone.receive() }
            }
        }
        suspend fun readByte(ms: Long = 10_000): Byte = withTimeout(ms) { rxChannel.receive() }
        suspend fun readBytes(n: Int, ms: Long = 60_000): ByteArray {
            val buf = ByteArray(n)
            withTimeout(ms) { for (i in 0 until n) buf[i] = rxChannel.receive() }
            return buf
        }

        return try {
            // INIT
            writeCmd(OSTC_CMD_INIT)
            val initEcho = readByte()
            if (initEcho != OSTC_CMD_INIT)
                throw Exception("OSTC init echo mismatch: 0x${initEcho.toUByte().toString(16)}")
            readByte() // READY

            // COMPACT logbook → 4096 bytes
            setState(state.get().copy(message = "Downloading logbook…", progress = 45))
            writeCmd(OSTC_CMD_COMPACT)
            val compactEcho = readByte()
            if (compactEcho != OSTC_CMD_COMPACT)
                throw Exception("OSTC compact echo mismatch")
            val compact = readBytes(OSTC_LOGBOOK_SLOTS * OSTC_COMPACT_ENTRY, ms = 90_000)
            readByte() // READY

            // Find valid slots: non-0xFF entries with non-zero length
            data class Slot(val idx: Int, val len: Int, val diveNum: Int)
            val slots = mutableListOf<Slot>()
            for (i in 0 until OSTC_LOGBOOK_SLOTS) {
                val e = compact.copyOfRange(i * OSTC_COMPACT_ENTRY, (i + 1) * OSTC_COMPACT_ENTRY)
                if (e.all { it == 0xFF.toByte() }) continue
                val len = (e[0].toInt() and 0xFF) or
                          ((e[1].toInt() and 0xFF) shl 8) or
                          ((e[2].toInt() and 0xFF) shl 16)
                val num = (e[13].toInt() and 0xFF) or ((e[14].toInt() and 0xFF) shl 8)
                if (len > 0 && len < 500_000) slots.add(Slot(i, len, num))
            }
            Log.i(TAG, "OSTC: found ${slots.size} dives in logbook")

            // Download each dive profile
            val dives = mutableListOf<DiveSummary>()
            slots.forEachIndexed { n, slot ->
                setState(state.get().copy(
                    message  = "Downloading dive ${n + 1}/${slots.size}…",
                    progress = 50 + 25 * (n + 1) / slots.size
                ))
                try {
                    writeCmd(OSTC_CMD_DIVE, slot.idx.toByte())
                    val diveEcho = readByte()
                    if (diveEcho != OSTC_CMD_DIVE) return@forEachIndexed
                    val profile = readBytes(slot.len, ms = 120_000)
                    readByte() // READY
                    parseOstcProfile(profile, slot.diveNum, device.name ?: "HW OSTC")
                        ?.let { dives.add(it) }
                } catch (e: Exception) {
                    Log.w(TAG, "OSTC slot ${slot.idx}: ${e.message}")
                }
            }

            try { writeCmd(OSTC_CMD_EXIT) } catch (_: Exception) {}
            gatt.disconnect(); withTimeoutOrNull(5_000) { done.await() }; gatt.close()
            dives
        } catch (e: Exception) {
            try { gatt.disconnect() } catch (_: Exception) {}
            withTimeoutOrNull(3_000) { done.await() }
            gatt.close()
            throw e
        }
    }

    // ── HW OSTC U-Blox variant ────────────────────────────────────────────────
    // Same OSTC command protocol; different characteristic layout.
    // U-Blox uses a single DATA characteristic for both RX and TX,
    // plus a CREDITS characteristic (both as notify+write).

    @SuppressLint("MissingPermission")
    private suspend fun downloadHwOstcUblox(device: BluetoothDevice): List<DiveSummary> {
        val rxChannel  = Channel<Byte>(Channel.UNLIMITED)
        val dataWrDone = Channel<Unit>(1)
        val done       = CompletableDeferred<Unit>()
        var hwCredits  = HW_MAXIMAL_CREDIT
        var dataChar:    BluetoothGattCharacteristic? = null
        var creditsChar: BluetoothGattCharacteristic? = null
        val setupDone  = CompletableDeferred<Unit>()
        var setupStep  = 0

        setState(state.get().copy(message = "Connecting to OSTC (U-Blox)…", progress = 25))

        val gattCb = object : BluetoothGattCallback() {
            override fun onConnectionStateChange(gatt: BluetoothGatt, status: Int, newState: Int) {
                if (newState == BluetoothProfile.STATE_CONNECTED) gatt.discoverServices()
                else if (newState == BluetoothProfile.STATE_DISCONNECTED) done.complete(Unit)
            }

            override fun onServicesDiscovered(gatt: BluetoothGatt, status: Int) {
                val svc = gatt.getService(HW_UBLOX_SERVICE)
                    ?: run { done.completeExceptionally(Exception("HW U-Blox service not found")); return }
                dataChar    = svc.getCharacteristic(HW_UBLOX_DATA)
                creditsChar = svc.getCharacteristic(HW_UBLOX_CREDITS)
                gatt.setCharacteristicNotification(dataChar!!, true)
                dataChar!!.getDescriptor(CCCD).also {
                    it.value = BluetoothGattDescriptor.ENABLE_NOTIFICATION_VALUE
                    setupStep = 0; gatt.writeDescriptor(it)
                }
            }

            override fun onDescriptorWrite(gatt: BluetoothGatt, descriptor: BluetoothGattDescriptor, status: Int) {
                when (setupStep) {
                    0 -> {
                        val cc = creditsChar ?: return
                        gatt.setCharacteristicNotification(cc, true)
                        cc.getDescriptor(CCCD).also {
                            it.value = BluetoothGattDescriptor.ENABLE_INDICATION_VALUE
                            setupStep = 1; gatt.writeDescriptor(it)
                        }
                    }
                    1 -> {
                        creditsChar?.let {
                            it.value = byteArrayOf(HW_MAXIMAL_CREDIT.toByte())
                            setupStep = 2; gatt.writeCharacteristic(it)
                        }
                    }
                }
            }

            override fun onCharacteristicWrite(gatt: BluetoothGatt, characteristic: BluetoothGattCharacteristic, status: Int) {
                when (characteristic.uuid) {
                    HW_UBLOX_DATA    -> dataWrDone.trySend(Unit)
                    HW_UBLOX_CREDITS -> if (setupStep == 2) { setupStep = 3; setupDone.complete(Unit) }
                }
            }

            @Deprecated("Required for API < 33")
            override fun onCharacteristicChanged(gatt: BluetoothGatt, characteristic: BluetoothGattCharacteristic) {
                if (characteristic.uuid == HW_UBLOX_DATA) {
                    characteristic.value.forEach { rxChannel.trySend(it) }
                    hwCredits -= characteristic.value.size
                    if (hwCredits <= HW_MINIMAL_CREDIT) {
                        val topUp = (HW_MAXIMAL_CREDIT - hwCredits).coerceAtMost(255)
                        hwCredits += topUp
                        creditsChar?.let { it.value = byteArrayOf(topUp.toByte()); gatt.writeCharacteristic(it) }
                    }
                }
            }
        }

        val gatt = device.connectGatt(this, false, gattCb, BluetoothDevice.TRANSPORT_LE)
        withTimeoutOrNull(30_000) { setupDone.await() }
            ?: run { gatt.close(); throw Exception("OSTC U-Blox setup timed out") }

        setState(state.get().copy(message = "OSTC U-Blox ready — starting download…", progress = 40))

        // Reuse same serial helpers wired to U-Blox data characteristic
        suspend fun writeCmd(vararg bytes: Byte) {
            val ch = dataChar ?: return
            bytes.toList().chunked(20).forEach { chunk ->
                ch.value = chunk.toByteArray()
                gatt.writeCharacteristic(ch)
                withTimeout(5_000) { dataWrDone.receive() }
            }
        }
        suspend fun readByte(ms: Long = 10_000): Byte = withTimeout(ms) { rxChannel.receive() }
        suspend fun readBytes(n: Int, ms: Long = 60_000): ByteArray {
            val buf = ByteArray(n)
            withTimeout(ms) { for (i in 0 until n) buf[i] = rxChannel.receive() }
            return buf
        }

        return try {
            writeCmd(OSTC_CMD_INIT)
            if (readByte() != OSTC_CMD_INIT) throw Exception("OSTC U-Blox init echo mismatch")
            readByte() // READY

            setState(state.get().copy(message = "Downloading logbook…", progress = 45))
            writeCmd(OSTC_CMD_COMPACT)
            if (readByte() != OSTC_CMD_COMPACT) throw Exception("OSTC U-Blox compact echo mismatch")
            val compact = readBytes(OSTC_LOGBOOK_SLOTS * OSTC_COMPACT_ENTRY, ms = 90_000)
            readByte() // READY

            data class Slot(val idx: Int, val len: Int, val diveNum: Int)
            val slots = mutableListOf<Slot>()
            for (i in 0 until OSTC_LOGBOOK_SLOTS) {
                val e = compact.copyOfRange(i * OSTC_COMPACT_ENTRY, (i + 1) * OSTC_COMPACT_ENTRY)
                if (e.all { it == 0xFF.toByte() }) continue
                val len = (e[0].toInt() and 0xFF) or ((e[1].toInt() and 0xFF) shl 8) or ((e[2].toInt() and 0xFF) shl 16)
                val num = (e[13].toInt() and 0xFF) or ((e[14].toInt() and 0xFF) shl 8)
                if (len in 1..499_999) slots.add(Slot(i, len, num))
            }

            val dives = mutableListOf<DiveSummary>()
            slots.forEachIndexed { n, slot ->
                setState(state.get().copy(message = "Downloading dive ${n + 1}/${slots.size}…", progress = 50 + 25 * (n + 1) / slots.size))
                try {
                    writeCmd(OSTC_CMD_DIVE, slot.idx.toByte())
                    if (readByte() != OSTC_CMD_DIVE) return@forEachIndexed
                    val profile = readBytes(slot.len, ms = 120_000)
                    readByte() // READY
                    parseOstcProfile(profile, slot.diveNum, device.name ?: "HW OSTC")?.let { dives.add(it) }
                } catch (e: Exception) { Log.w(TAG, "OSTC U-Blox slot ${slot.idx}: ${e.message}") }
            }

            try { writeCmd(OSTC_CMD_EXIT) } catch (_: Exception) {}
            gatt.disconnect(); withTimeoutOrNull(5_000) { done.await() }; gatt.close()
            dives
        } catch (e: Exception) {
            try { gatt.disconnect() } catch (_: Exception) {}
            withTimeoutOrNull(3_000) { done.await() }; gatt.close(); throw e
        }
    }

    // ── Cressi download (cressi_goa.c) ────────────────────────────────────────
    // Uses modified Nordic UART Service for BLE transport.
    // Commands: CMD_LOGBOOK_BLE (0x02) → logbook entries;
    //           CMD_DIVE_BLE (0x03) + [num_hi, num_lo] → dive profile.
    // Dive number in logbook entry: bytes [1,0] (little-endian stored, big-endian sent).
    // Profile header (v4 firmware 200-299): datetime at 12, divetime at 20,
    //   maxdepth at 73, temp at 77 (offsets from libdivecomputer cressi_goa_parser.c).

    @SuppressLint("MissingPermission")
    private suspend fun downloadCressl(device: BluetoothDevice): List<DiveSummary> {
        val rxChannel = Channel<Byte>(Channel.UNLIMITED)
        val wrDone    = Channel<Unit>(1)
        val done      = CompletableDeferred<Unit>()
        var writeChar: BluetoothGattCharacteristic? = null
        val setupDone = CompletableDeferred<Unit>()
        var firmware  = 250 // default to v4 range

        setState(state.get().copy(message = "Connecting to Cressi…", progress = 25))

        val gattCb = object : BluetoothGattCallback() {
            override fun onConnectionStateChange(gatt: BluetoothGatt, status: Int, newState: Int) {
                if (newState == BluetoothProfile.STATE_CONNECTED) gatt.discoverServices()
                else if (newState == BluetoothProfile.STATE_DISCONNECTED) done.complete(Unit)
            }

            override fun onServicesDiscovered(gatt: BluetoothGatt, status: Int) {
                val svc = gatt.getService(CRESSI_SERVICE)
                    ?: run { done.completeExceptionally(Exception("Cressi service not found")); return }
                writeChar = svc.getCharacteristic(CRESSI_RX)
                val fwChar = svc.getCharacteristic(CRESSI_FW_CHAR)
                if (fwChar != null) gatt.readCharacteristic(fwChar)
                else subscribeCressl(gatt, svc)
            }

            override fun onCharacteristicRead(gatt: BluetoothGatt, characteristic: BluetoothGattCharacteristic, status: Int) {
                if (characteristic.uuid == CRESSI_FW_CHAR && characteristic.value.size >= 2) {
                    firmware = (characteristic.value[0].toInt() and 0xFF) or
                               ((characteristic.value[1].toInt() and 0xFF) shl 8)
                }
                subscribeCressl(gatt, gatt.getService(CRESSI_SERVICE) ?: return)
            }

            private fun subscribeCressl(gatt: BluetoothGatt, svc: BluetoothGattService) {
                val tx = svc.getCharacteristic(CRESSI_TX) ?: run {
                    done.completeExceptionally(Exception("Cressi TX char not found")); return
                }
                gatt.setCharacteristicNotification(tx, true)
                tx.getDescriptor(CCCD)?.also {
                    it.value = BluetoothGattDescriptor.ENABLE_NOTIFICATION_VALUE
                    gatt.writeDescriptor(it)
                } ?: setupDone.complete(Unit) // no CCCD — try without
            }

            override fun onDescriptorWrite(gatt: BluetoothGatt, descriptor: BluetoothGattDescriptor, status: Int) {
                setupDone.complete(Unit)
            }

            override fun onCharacteristicWrite(gatt: BluetoothGatt, characteristic: BluetoothGattCharacteristic, status: Int) {
                wrDone.trySend(Unit)
            }

            @Deprecated("Required for API < 33")
            override fun onCharacteristicChanged(gatt: BluetoothGatt, characteristic: BluetoothGattCharacteristic) {
                characteristic.value.forEach { rxChannel.trySend(it) }
            }
        }

        val gatt = device.connectGatt(this, false, gattCb, BluetoothDevice.TRANSPORT_LE)
        withTimeoutOrNull(30_000) { setupDone.await() }
            ?: run { gatt.close(); throw Exception("Cressi setup timed out") }

        // logbook entry length depends on firmware version (cressi_goa.c conf->logbook_len)
        val logbookLen = if (firmware >= 200) 92 else 52

        suspend fun write(vararg bytes: Byte) {
            val ch = writeChar ?: return
            ch.value = byteArrayOf(*bytes)
            gatt.writeCharacteristic(ch)
            withTimeout(5_000) { wrDone.receive() }
        }
        suspend fun readUntilIdle(ms: Long = 8_000): ByteArray {
            val buf = mutableListOf<Byte>()
            try { withTimeout(ms) { while (true) buf.add(rxChannel.receive()) } }
            catch (_: TimeoutCancellationException) {}
            return buf.toByteArray()
        }

        setState(state.get().copy(message = "Requesting Cressi logbook…", progress = 45))
        write(CRESSI_CMD_LOGBOOK, 0x00)
        val logbook = readUntilIdle()

        if (logbook.size < logbookLen) {
            gatt.disconnect(); withTimeoutOrNull(3_000) { done.await() }; gatt.close()
            throw Exception("Cressi: no logbook data (received ${logbook.size} bytes)")
        }

        // Parse dive numbers — stored LE in logbook, sent BE on wire
        val diveNumbers = mutableListOf<Int>()
        var offset = 0
        while (offset + logbookLen <= logbook.size) {
            val lo = logbook[offset].toInt() and 0xFF
            val hi = logbook[offset + 1].toInt() and 0xFF
            val num = lo or (hi shl 8)  // little-endian stored
            if (num == 0) break
            diveNumbers.add(num)
            offset += logbookLen
        }

        val dives = mutableListOf<DiveSummary>()
        diveNumbers.forEachIndexed { n, num ->
            setState(state.get().copy(
                message  = "Downloading Cressi dive ${n + 1}/${diveNumbers.size}…",
                progress = 50 + 25 * (n + 1) / diveNumbers.size
            ))
            try {
                // Send dive number big-endian on wire (cressi_goa.c CMD_DIVE_BLE)
                write(CRESSI_CMD_DIVE, (num shr 8).toByte(), (num and 0xFF).toByte())
                val diveData = readUntilIdle()
                parseCressiDive(diveData, num, device.name ?: "Cressi")?.let { dives.add(it) }
            } catch (e: Exception) { Log.w(TAG, "Cressi dive $num: ${e.message}") }
        }

        gatt.disconnect(); withTimeoutOrNull(5_000) { done.await() }; gatt.close()
        return dives
    }

    // ── Divesoft Freedom BLE download ─────────────────────────────────────────
    // Protocol: HDLC-framed message packets over custom service fcef / fcf0 / fcf1.
    // Each packet: [seqbyte, flags, msgtype_LE16, datalen_LE16, data..., crc16_LE16]
    // CRC-16/KERMIT (reflected, poly 0x8408, init=0xFFFF, xorout=0xFFFF)

    private fun crc16Kermit(data: ByteArray): Int {
        var crc = 0xFFFF
        for (b in data) {
            crc = crc xor (b.toInt() and 0xFF)
            repeat(8) { crc = if (crc and 1 != 0) (crc ushr 1) xor 0x8408 else crc ushr 1 }
        }
        return crc xor 0xFFFF
    }

    private suspend fun downloadDivesoft(device: BluetoothDevice): List<DiveSummary> {
        val dives = mutableListOf<DiveSummary>()
        val source = device.name ?: "Divesoft"

        val notifications = Channel<ByteArray>(Channel.UNLIMITED)
        val hdlcBuf = mutableListOf<Byte>()
        val hdlcFrames = mutableListOf<ByteArray>()
        val connected = CompletableDeferred<Unit>()
        val done = CompletableDeferred<Unit>()

        var txChar: android.bluetooth.BluetoothGattCharacteristic? = null
        var rxChar: android.bluetooth.BluetoothGattCharacteristic? = null
        var dsSeq = 0      // outgoing sequence nibble (0-15)
        var dsCount = 0    // message count (upper nibble of seqbyte)

        val gatt = device.connectGatt(this, false, object : android.bluetooth.BluetoothGattCallback() {
            override fun onConnectionStateChange(g: android.bluetooth.BluetoothGatt, s: Int, ns: Int) {
                if (ns == android.bluetooth.BluetoothProfile.STATE_CONNECTED) g.discoverServices()
                else done.complete(Unit)
            }
            override fun onServicesDiscovered(g: android.bluetooth.BluetoothGatt, s: Int) {
                val svc = g.getService(DIVESOFT_SERVICE) ?: run { done.complete(Unit); return }
                rxChar = svc.getCharacteristic(DIVESOFT_RX)
                txChar = svc.getCharacteristic(DIVESOFT_TX)
                val tx = txChar ?: run { done.complete(Unit); return }
                g.setCharacteristicNotification(tx, true)
                val desc = tx.getDescriptor(CCCD) ?: run { connected.complete(Unit); return }
                desc.value = android.bluetooth.BluetoothGattDescriptor.ENABLE_NOTIFICATION_VALUE
                g.writeDescriptor(desc)
            }
            override fun onDescriptorWrite(g: android.bluetooth.BluetoothGatt,
                    d: android.bluetooth.BluetoothGattDescriptor, s: Int) { connected.complete(Unit) }
            override fun onCharacteristicChanged(g: android.bluetooth.BluetoothGatt,
                    c: android.bluetooth.BluetoothGattCharacteristic) {
                notifications.trySend(c.value.copyOf())
            }
            override fun onCharacteristicWrite(g: android.bluetooth.BluetoothGatt,
                    c: android.bluetooth.BluetoothGattCharacteristic, s: Int) {}
        }, android.bluetooth.BluetoothDevice.TRANSPORT_LE)

        // helper: send a Divesoft message
        fun dsSend(msgType: Int, payload: ByteArray) {
            val rx = rxChar ?: return
            val pktLen = 6 + payload.size  // seqbyte + flags + msgtype(2) + datalen(2) + payload + crc(2)
            val raw = ByteArray(pktLen)
            dsSeq = (dsSeq + 1) and 0xF
            dsCount = (dsCount + 1) and 0xF
            raw[0] = ((dsCount shl 4) or dsSeq).toByte()
            raw[1] = 0xC0.toByte()  // flags: host→device last fragment
            raw[2] = (msgType and 0xFF).toByte()
            raw[3] = ((msgType ushr 8) and 0xFF).toByte()
            raw[4] = (payload.size and 0xFF).toByte()
            raw[5] = ((payload.size ushr 8) and 0xFF).toByte()
            payload.copyInto(raw, 6)
            val crcVal = crc16Kermit(raw.copyOf(pktLen - 2))
            raw[pktLen - 2] = (crcVal and 0xFF).toByte()
            raw[pktLen - 1] = ((crcVal ushr 8) and 0xFF).toByte()
            val framed = hdlcEncode(raw)
            // send in 20-byte BLE chunks
            var offset = 0
            while (offset < framed.size) {
                val chunk = framed.copyOfRange(offset, minOf(offset + 20, framed.size))
                rx.value = chunk
                gatt.writeCharacteristic(rx)
                offset += chunk.size
                Thread.sleep(10)
            }
        }

        // helper: collect HDLC frames until a complete message of expected type arrives
        suspend fun dsRecvMsg(expectedType: Int, timeoutMs: Long = 10_000L): ByteArray? {
            val deadline = System.currentTimeMillis() + timeoutMs
            while (System.currentTimeMillis() < deadline) {
                val remaining = deadline - System.currentTimeMillis()
                val chunk = withTimeoutOrNull(remaining) { notifications.receive() } ?: break
                hdlcBuf.addAll(chunk.toList())
                hdlcDecode(hdlcBuf, hdlcFrames)
                val frame = hdlcFrames.removeFirstOrNull() ?: continue
                if (frame.size < 6) continue
                val msgType = (frame[2].toInt() and 0xFF) or ((frame[3].toInt() and 0xFF) shl 8)
                if (msgType != expectedType) continue
                val dataLen = (frame[4].toInt() and 0xFF) or ((frame[5].toInt() and 0xFF) shl 8)
                return if (frame.size >= 6 + dataLen) frame.copyOfRange(6, 6 + dataLen) else null
            }
            return null
        }

        withTimeoutOrNull(8_000) { connected.await() }

        try {
            // MSG_CONNECT → MSG_CONNECTED
            val connectPayload = byteArrayOf(1, 0) + "libdivecomputer".toByteArray(Charsets.US_ASCII)
            dsSend(DS_MSG_CONNECT, connectPayload)
            dsRecvMsg(DS_MSG_CONNECTED, 8_000) ?: throw Exception("DS: no CONNECTED response")

            // MSG_VERSION → MSG_VERSION_RSP
            dsSend(DS_MSG_VERSION, ByteArray(0))
            dsRecvMsg(DS_MSG_VERSION + 1, 5_000)  // response, ignore content

            // MSG_DIVE_LIST loop — device returns records in pages of up to 100
            var currentHandle = 0L
            var listVersion = DS_MSG_DIVE_LIST_V1
            var diveSeq = 0

            while (true) {
                val listPayload = ByteArray(10)
                listPayload[0] = (currentHandle and 0xFF).toByte()
                listPayload[1] = ((currentHandle ushr 8) and 0xFF).toByte()
                listPayload[2] = ((currentHandle ushr 16) and 0xFF).toByte()
                listPayload[3] = ((currentHandle ushr 24) and 0xFF).toByte()
                listPayload[4] = 1  // direction forward
                listPayload[5] = 0; listPayload[6] = 0; listPayload[7] = 0
                listPayload[8] = 100; listPayload[9] = 0  // nrecords = 100

                dsSend(DS_MSG_DIVE_LIST, listPayload)

                // accept either V1(67) or V2(71) response
                val deadline2 = System.currentTimeMillis() + 15_000L
                var listResp: ByteArray? = null
                while (System.currentTimeMillis() < deadline2) {
                    val remaining = deadline2 - System.currentTimeMillis()
                    val chunk = withTimeoutOrNull(remaining) { notifications.receive() } ?: break
                    hdlcBuf.addAll(chunk.toList())
                    hdlcDecode(hdlcBuf, hdlcFrames)
                    val frame = hdlcFrames.removeFirstOrNull() ?: continue
                    if (frame.size < 6) continue
                    val mt = (frame[2].toInt() and 0xFF) or ((frame[3].toInt() and 0xFF) shl 8)
                    if (mt == DS_MSG_DIVE_LIST_V1 || mt == DS_MSG_DIVE_LIST_V2) {
                        listVersion = mt
                        val dl = (frame[4].toInt() and 0xFF) or ((frame[5].toInt() and 0xFF) shl 8)
                        listResp = if (frame.size >= 6 + dl) frame.copyOfRange(6, 6 + dl) else null
                        break
                    }
                }
                if (listResp == null) break

                // parse records from response
                // response: [count_LE32, records...]
                if (listResp.size < 4) break
                val count = (listResp[0].toInt() and 0xFF) or
                    ((listResp[1].toInt() and 0xFF) shl 8) or
                    ((listResp[2].toInt() and 0xFF) shl 16) or
                    ((listResp[3].toInt() and 0xFF) shl 24)

                val recordSize = if (listVersion == DS_MSG_DIVE_LIST_V2) 88 else 56
                var pos = 4
                var lastHandle = currentHandle
                while (pos + recordSize <= listResp.size) {
                    val record = listResp.copyOfRange(pos, pos + recordSize)
                    // first 4 bytes = handle, next 20 bytes = metadata, rest = header
                    val handle = (record[0].toLong() and 0xFF) or
                        ((record[1].toLong() and 0xFF) shl 8) or
                        ((record[2].toLong() and 0xFF) shl 16) or
                        ((record[3].toLong() and 0xFF) shl 24)
                    lastHandle = handle
                    val header = record.copyOfRange(24, recordSize)  // skip 4 handle + 20 meta
                    parseDivesoftRecord(header, listVersion, ++diveSeq, source)?.let { dives.add(it) }
                    pos += recordSize
                }

                if (count < 100) break
                currentHandle = lastHandle + 1
            }
        } catch (e: Exception) {
            Log.e(TAG, "Divesoft download error: ${e.message}")
        }

        gatt.disconnect(); withTimeoutOrNull(5_000) { done.await() }; gatt.close()
        return dives
    }

    // ── Divesoft header parser ─────────────────────────────────────────────────
    // V1 sig 0x45766944 ("DivE", 32 bytes):
    //   [8-11] timestamp LE uint32 (seconds since 2000)
    //   [12-15] misc1 LE uint32 (divetime = misc1 & 0x1FFFF)
    //   [16-19] misc2 (temp in bits[18:27], 10-bit signed)
    //   [20-21] maxdepth LE uint16 (1/100 m)
    // V2 sig 0x45566944 ("DiVE", 64 bytes):
    //   [8-11] timestamp LE uint32 (seconds since 2000)
    //   [12-15] divetime LE uint32 (seconds)
    //   [24-25] temperature LE int16 (1/10 °C)
    //   [28-29] maxdepth LE uint16 (1/100 m)
    //   [40-41] timezone LE int16 (minutes)

    private fun parseDivesoftRecord(header: ByteArray, listVersion: Int, diveNum: Int, source: String): DiveSummary? {
        if (header.size < 32) return null
        return try {
            val sig = (header[0].toInt() and 0xFF) or
                ((header[1].toInt() and 0xFF) shl 8) or
                ((header[2].toInt() and 0xFF) shl 16) or
                ((header[3].toInt() and 0xFF) shl 24)

            val isV2 = (sig == 0x45566944)  // "DiVE"

            val timestampSec = (header[8].toLong() and 0xFF) or
                ((header[9].toLong() and 0xFF) shl 8) or
                ((header[10].toLong() and 0xFF) shl 16) or
                ((header[11].toLong() and 0xFF) shl 24)

            val durationS: Int
            val maxDepthM: Double
            val tempC: Double?
            val tzOffsetSec: Long

            if (isV2 && header.size >= 42) {
                durationS = ((header[12].toInt() and 0xFF) or
                    ((header[13].toInt() and 0xFF) shl 8) or
                    ((header[14].toInt() and 0xFF) shl 16) or
                    ((header[15].toInt() and 0xFF) shl 24))
                val tempRaw = (header[24].toInt() and 0xFF) or ((header[25].toInt() and 0xFF) shl 8)
                tempC = (tempRaw.toShort().toInt()) / 10.0
                maxDepthM = ((header[28].toInt() and 0xFF) or ((header[29].toInt() and 0xFF) shl 8)) / 100.0
                val tzMin = (header[40].toInt() and 0xFF) or ((header[41].toInt() and 0xFF) shl 8)
                tzOffsetSec = (tzMin.toShort().toInt()) * 60L
            } else {
                // V1
                val misc1 = (header[12].toInt() and 0xFF) or
                    ((header[13].toInt() and 0xFF) shl 8) or
                    ((header[14].toInt() and 0xFF) shl 16) or
                    ((header[15].toInt() and 0xFF) shl 24)
                durationS = misc1 and 0x1FFFF
                val misc2 = (header[16].toInt() and 0xFF) or
                    ((header[17].toInt() and 0xFF) shl 8) or
                    ((header[18].toInt() and 0xFF) shl 16) or
                    ((header[19].toInt() and 0xFF) shl 24)
                val tempBits = (misc2 ushr 18) and 0x3FF
                val tempSigned = if (tempBits and 0x200 != 0) (tempBits or 0xFFFFFC00.toInt()) else tempBits
                tempC = tempSigned / 10.0
                maxDepthM = ((header[20].toInt() and 0xFF) or ((header[21].toInt() and 0xFF) shl 8)) / 100.0
                tzOffsetSec = 0L
            }

            if (maxDepthM < 0.5 || durationS < 30) return null

            val unixMs = (timestampSec + DS_EPOCH + tzOffsetSec) * 1000L
            val divedAt = java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.US)
                .apply { timeZone = TimeZone.getTimeZone("UTC") }
                .format(java.util.Date(unixMs))

            DiveSummary(source, diveNum, divedAt, maxDepthM, durationS,
                tempC?.takeIf { it >= -5 && it <= 50 })
        } catch (_: Exception) { null }
    }

    // ── ScubaPro G2/G3 (Uwatec Smart) BLE download ────────────────────────────
    // Protocol: write [size+1, cmd, data...] to RX characteristic.
    // Notifications: each chunk has 1-byte length prefix; actual payload from byte 1 onward.
    // CMD_SIZE(0xC6) with 8-byte params → 4-byte LE response = data size.
    // CMD_DATA(0xC4) with same params → 4-byte LE = total length, then `size` bytes of dump.
    // Dump parsing: scan backwards for magic [0xa5,0xa5,0x5a,0x5a];
    //   len at magic+4 (LE uint32); timestamp (half-seconds since 2000) at magic+8;
    //   G2/G3 header layout (trimix_header): maxdepth@22, divetime@26, temp@30, timezone@16.

    private suspend fun downloadScubaPro(device: BluetoothDevice): List<DiveSummary> {
        val dives = mutableListOf<DiveSummary>()
        val source = device.name ?: "ScubaPro"

        val notifications = Channel<ByteArray>(Channel.UNLIMITED)
        val connected = CompletableDeferred<Unit>()
        val done = CompletableDeferred<Unit>()

        var rxChar: android.bluetooth.BluetoothGattCharacteristic? = null

        val gatt = device.connectGatt(this, false, object : android.bluetooth.BluetoothGattCallback() {
            override fun onConnectionStateChange(g: android.bluetooth.BluetoothGatt, s: Int, ns: Int) {
                if (ns == android.bluetooth.BluetoothProfile.STATE_CONNECTED) g.discoverServices()
                else done.complete(Unit)
            }
            override fun onServicesDiscovered(g: android.bluetooth.BluetoothGatt, s: Int) {
                val svc = g.getService(SCUBAPRO_SERVICE) ?: run { done.complete(Unit); return }
                rxChar = svc.getCharacteristic(SCUBAPRO_RX)
                val tx = svc.getCharacteristic(SCUBAPRO_TX) ?: run { connected.complete(Unit); return }
                g.setCharacteristicNotification(tx, true)
                val desc = tx.getDescriptor(CCCD) ?: run { connected.complete(Unit); return }
                desc.value = android.bluetooth.BluetoothGattDescriptor.ENABLE_NOTIFICATION_VALUE
                g.writeDescriptor(desc)
            }
            override fun onDescriptorWrite(g: android.bluetooth.BluetoothGatt,
                    d: android.bluetooth.BluetoothGattDescriptor, s: Int) { connected.complete(Unit) }
            override fun onCharacteristicChanged(g: android.bluetooth.BluetoothGatt,
                    c: android.bluetooth.BluetoothGattCharacteristic) {
                notifications.trySend(c.value.copyOf())
            }
            override fun onCharacteristicWrite(g: android.bluetooth.BluetoothGatt,
                    c: android.bluetooth.BluetoothGattCharacteristic, s: Int) {}
        }, android.bluetooth.BluetoothDevice.TRANSPORT_LE)

        fun scSend(cmd: Byte, vararg args: Byte) {
            val rx = rxChar ?: return
            val buf = byteArrayOf((args.size + 1).toByte(), cmd, *args)
            rx.value = buf
            gatt.writeCharacteristic(rx)
        }

        suspend fun scRecv(expectedBytes: Int, timeoutMs: Long = 10_000L): ByteArray {
            val buf = mutableListOf<Byte>()
            val deadline = System.currentTimeMillis() + timeoutMs
            while (buf.size < expectedBytes && System.currentTimeMillis() < deadline) {
                val remaining = deadline - System.currentTimeMillis()
                val chunk = withTimeoutOrNull(remaining) { notifications.receive() } ?: break
                // skip first byte (length indicator), append rest
                if (chunk.size > 1) buf.addAll(chunk.drop(1))
            }
            return buf.toByteArray()
        }

        withTimeoutOrNull(8_000) { connected.await() }

        try {
            // CMD_SIZE: 8-byte params (timestamp=0 = all dives)
            val sizeParams = ByteArray(8)
            scSend(SC_CMD_SIZE, *sizeParams)
            val sizeResp = scRecv(4, 5_000)
            if (sizeResp.size < 4) throw Exception("ScubaPro: no size response")
            val dataSize = (sizeResp[0].toLong() and 0xFF) or
                ((sizeResp[1].toLong() and 0xFF) shl 8) or
                ((sizeResp[2].toLong() and 0xFF) shl 16) or
                ((sizeResp[3].toLong() and 0xFF) shl 24)

            if (dataSize <= 0 || dataSize > 2_000_000) throw Exception("ScubaPro: bad data size $dataSize")

            // CMD_DATA: same params → 4 header bytes + dataSize bytes
            scSend(SC_CMD_DATA, *sizeParams)
            val headerResp = scRecv(4, 5_000)
            if (headerResp.size < 4) throw Exception("ScubaPro: no data header")
            val totalSize = (headerResp[0].toLong() and 0xFF) or
                ((headerResp[1].toLong() and 0xFF) shl 8) or
                ((headerResp[2].toLong() and 0xFF) shl 16) or
                ((headerResp[3].toLong() and 0xFF) shl 24)
            val dump = scRecv(totalSize.toInt() - 4, 60_000)

            parseUwatecSmartDump(dump, source, dives)
        } catch (e: Exception) {
            Log.e(TAG, "ScubaPro download error: ${e.message}")
        }

        gatt.disconnect(); withTimeoutOrNull(5_000) { done.await() }; gatt.close()
        return dives
    }

    private fun parseUwatecSmartDump(dump: ByteArray, source: String, dives: MutableList<DiveSummary>) {
        // Scan backwards for magic [0xa5, 0xa5, 0x5a, 0x5a]
        val magic = byteArrayOf(0xa5.toByte(), 0xa5.toByte(), 0x5a.toByte(), 0x5a.toByte())
        var pos = dump.size - 4
        var diveNum = 0
        while (pos >= 0) {
            if (dump[pos] == magic[0] && pos + 3 < dump.size &&
                dump[pos + 1] == magic[1] && dump[pos + 2] == magic[2] && dump[pos + 3] == magic[3]) {
                if (pos + 12 > dump.size) { pos--; continue }
                val len = (dump[pos + 4].toLong() and 0xFF) or
                    ((dump[pos + 5].toLong() and 0xFF) shl 8) or
                    ((dump[pos + 6].toLong() and 0xFF) shl 16) or
                    ((dump[pos + 7].toLong() and 0xFF) shl 24)
                if (len < 84 || pos + len.toInt() > dump.size) { pos--; continue }
                val diveData = dump.copyOfRange(pos, pos + len.toInt())
                parseUwatecSmartDive(diveData, ++diveNum, source)?.let { dives.add(0, it) }  // prepend = chronological
                pos -= len.toInt()
            } else {
                pos--
            }
        }
    }

    // Uwatec Smart trimix_header layout (G2/G3):
    //   [8-11]  timestamp LE uint32 (half-seconds since SC_EPOCH)
    //   [16]    timezone signed byte (15-minute units)
    //   [22-23] maxdepth LE uint16 (mbar)
    //   [26-27] divetime LE uint16 (minutes)
    //   [30-31] temp_minimum LE int16 (0.1 °C)

    private fun parseUwatecSmartDive(data: ByteArray, diveNum: Int, source: String): DiveSummary? {
        if (data.size < 32) return null
        return try {
            val tsHalf = (data[8].toLong() and 0xFF) or
                ((data[9].toLong() and 0xFF) shl 8) or
                ((data[10].toLong() and 0xFF) shl 16) or
                ((data[11].toLong() and 0xFF) shl 24)
            val tzUnits = data[16].toInt()  // signed byte
            val tzOffsetSec = tzUnits.toByte().toInt() * 900L  // 15-min units

            val timestampSec = tsHalf / 2L
            val unixMs = (timestampSec + SC_EPOCH + tzOffsetSec) * 1000L

            val maxDepthMbar = (data[22].toInt() and 0xFF) or ((data[23].toInt() and 0xFF) shl 8)
            val maxDepthM = maxDepthMbar / 100.0  // mbar → approx metres (freshwater)

            val durationMin = (data[26].toInt() and 0xFF) or ((data[27].toInt() and 0xFF) shl 8)
            val durationS = durationMin * 60

            val tempRaw = (data[30].toInt() and 0xFF) or ((data[31].toInt() and 0xFF) shl 8)
            val tempC = (tempRaw.toShort().toInt()) / 10.0

            if (maxDepthM < 0.5 || durationS < 30) return null

            val divedAt = java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.US)
                .apply { timeZone = TimeZone.getTimeZone("UTC") }
                .format(java.util.Date(unixMs))

            DiveSummary(source, diveNum, divedAt, maxDepthM, durationS,
                tempC.takeIf { it >= -5 && it <= 50 })
        } catch (_: Exception) { null }
    }

    // ── Pelagic / Oceanic A+B BLE download ────────────────────────────────────
    // Protocol: 20-byte BLE packets [0xCD, status|pkt_seq, cmd_seq, len, data(0-16)].
    // Write: status 0x40 = last fragment, 0x60 = more fragments.
    // Read: accumulate until buf[1] & 0x20 == 0 (last fragment indicator).
    // CMD_VERSION(0x84) → 16 bytes product info + 1 checksum.
    // CMD_HANDSHAKE(0xE5) → derive passphrase from name digits.
    // CMD_READ8(0xB4, addrHi, addrLo) → 128 bytes + 1 checksum (8 pages × 16 bytes).
    // Best-effort ProPlus3/ProPlus4 memory layout (ring buffer at 0x03E0, entry size 8 bytes).

    private suspend fun downloadPelagic(device: BluetoothDevice): List<DiveSummary> {
        val dives = mutableListOf<DiveSummary>()
        val source = device.name ?: "Oceanic"

        val notifications = Channel<ByteArray>(Channel.UNLIMITED)
        val connected = CompletableDeferred<Unit>()
        val done = CompletableDeferred<Unit>()

        var rxChar: android.bluetooth.BluetoothGattCharacteristic? = null
        var pelCmdSeq = 0

        val gatt = device.connectGatt(this, false, object : android.bluetooth.BluetoothGattCallback() {
            override fun onConnectionStateChange(g: android.bluetooth.BluetoothGatt, s: Int, ns: Int) {
                if (ns == android.bluetooth.BluetoothProfile.STATE_CONNECTED) g.discoverServices()
                else done.complete(Unit)
            }
            override fun onServicesDiscovered(g: android.bluetooth.BluetoothGatt, s: Int) {
                // Try both service UUIDs (A and B)
                val svc = g.getService(PELAGIC_A_SERVICE) ?: g.getService(PELAGIC_B_SERVICE)
                    ?: run { done.complete(Unit); return }
                rxChar = svc.getCharacteristic(PELAGIC_RX)
                val tx = svc.getCharacteristic(PELAGIC_TX) ?: run { connected.complete(Unit); return }
                g.setCharacteristicNotification(tx, true)
                val desc = tx.getDescriptor(CCCD) ?: run { connected.complete(Unit); return }
                desc.value = android.bluetooth.BluetoothGattDescriptor.ENABLE_NOTIFICATION_VALUE
                g.writeDescriptor(desc)
            }
            override fun onDescriptorWrite(g: android.bluetooth.BluetoothGatt,
                    d: android.bluetooth.BluetoothGattDescriptor, s: Int) { connected.complete(Unit) }
            override fun onCharacteristicChanged(g: android.bluetooth.BluetoothGatt,
                    c: android.bluetooth.BluetoothGattCharacteristic) {
                notifications.trySend(c.value.copyOf())
            }
            override fun onCharacteristicWrite(g: android.bluetooth.BluetoothGatt,
                    c: android.bluetooth.BluetoothGattCharacteristic, s: Int) {}
        }, android.bluetooth.BluetoothDevice.TRANSPORT_LE)

        // Send a command (may be split into 20-byte BLE packets)
        fun pelSend(cmdData: ByteArray) {
            val rx = rxChar ?: return
            pelCmdSeq = (pelCmdSeq + 1) and 0xFF
            val dataLen = cmdData.size
            var offset = 0
            var pktSeq = 0
            while (offset < dataLen || offset == 0) {
                val chunkSize = minOf(16, dataLen - offset)
                val isLast = (offset + chunkSize >= dataLen)
                val status: Byte = if (isLast) 0x40 else 0x60
                val pkt = ByteArray(4 + chunkSize)
                pkt[0] = PEL_SYNC
                pkt[1] = (status.toInt() or (pktSeq and 0x1F)).toByte()
                pkt[2] = pelCmdSeq.toByte()
                pkt[3] = chunkSize.toByte()
                cmdData.copyInto(pkt, 4, offset, offset + chunkSize)
                rx.value = pkt
                gatt.writeCharacteristic(rx)
                Thread.sleep(20)
                offset += chunkSize
                pktSeq++
                if (isLast) break
            }
        }

        // Receive a response (accumulate until last-fragment flag)
        suspend fun pelRecv(timeoutMs: Long = 8_000L): ByteArray {
            val buf = mutableListOf<Byte>()
            val deadline = System.currentTimeMillis() + timeoutMs
            while (System.currentTimeMillis() < deadline) {
                val remaining = deadline - System.currentTimeMillis()
                val chunk = withTimeoutOrNull(remaining) { notifications.receive() } ?: break
                if (chunk.size < 4) continue
                val dataLen = chunk[3].toInt() and 0xFF
                for (i in 4 until minOf(4 + dataLen, chunk.size)) buf.add(chunk[i])
                if (chunk[1].toInt() and 0x20 == 0) break  // last fragment
            }
            return buf.toByteArray()
        }

        // Read 8 pages (128 bytes) from address
        suspend fun pelRead8(address: Int): ByteArray {
            val pageNum = address / PEL_PAGESIZE
            pelSend(byteArrayOf(PEL_CMD_READ8, (pageNum ushr 8).toByte(), (pageNum and 0xFF).toByte()))
            val resp = pelRecv(8_000)
            return if (resp.size >= 128) resp.copyOf(128) else resp
        }

        withTimeoutOrNull(8_000) { connected.await() }

        try {
            // CMD_VERSION
            pelSend(byteArrayOf(PEL_CMD_VERSION))
            val verResp = pelRecv(5_000)
            if (verResp.isEmpty() || verResp[0] != PEL_ACK) throw Exception("Pelagic: no ACK on VERSION")

            // CMD_HANDSHAKE — derive passphrase from device name digits
            val name = device.name ?: ""
            val digits = name.filter { it.isDigit() }.take(6).padEnd(6, '0')
            val passphrase = ByteArray(8)
            for (i in 0 until 6) passphrase[i] = (digits[i] - '0').toByte()
            passphrase[6] = 0; passphrase[7] = 0
            var chk: Int = 0
            for (b in passphrase) chk += b.toInt() and 0xFF
            passphrase[7] = (chk and 0xFF).toByte()
            pelSend(byteArrayOf(PEL_CMD_HANDSHAKE) + passphrase)
            val hsResp = pelRecv(5_000)
            if (hsResp.isEmpty() || hsResp[0] != PEL_ACK) throw Exception("Pelagic: handshake failed")

            // Read logbook ring buffer — ProPlus3/ProPlus4 layout:
            // rb_logbook_begin = 0x03E0, entry size = 8 bytes, 256 entries max (2048 bytes)
            // Each entry: [0-3] start_addr LE uint32, [4-7] end_addr LE uint32 (or other metadata)
            val logbookBase = 0x03E0
            val entrySize = 8
            val maxEntries = 256
            val logbookBytes = mutableListOf<Byte>()
            val bytesNeeded = maxEntries * entrySize  // 2048 bytes = 16 × 128-byte reads

            for (chunk in 0 until (bytesNeeded + 127) / 128) {
                val addr = logbookBase + chunk * 128
                val block = pelRead8(addr)
                logbookBytes.addAll(block.toList())
                setState(state.get().copy(
                    message = "Reading Oceanic logbook…",
                    progress = 30 + 20 * chunk / ((bytesNeeded + 127) / 128)
                ))
            }
            val logbook = logbookBytes.toByteArray()

            // Parse logbook entries — find valid dive entries (non-zero start addresses)
            var diveSeq = 0
            var pos = 0
            while (pos + entrySize <= logbook.size) {
                val entry = logbook.copyOfRange(pos, pos + entrySize)
                val startAddr = (entry[0].toLong() and 0xFF) or
                    ((entry[1].toLong() and 0xFF) shl 8) or
                    ((entry[2].toLong() and 0xFF) shl 16) or
                    ((entry[3].toLong() and 0xFF) shl 24)
                pos += entrySize
                if (startAddr == 0L || startAddr == 0xFFFFFFFFL) continue

                // Read the dive profile header (128 bytes from start address)
                try {
                    val profileData = pelRead8(startAddr.toInt())
                    parsePelagicDiveHeader(profileData, ++diveSeq, source)?.let { dives.add(it) }
                } catch (e: Exception) {
                    Log.w(TAG, "Pelagic dive read error at $startAddr: ${e.message}")
                }
            }

            // CMD_QUIT
            pelSend(byteArrayOf(PEL_CMD_QUIT))
            pelRecv(2_000)
        } catch (e: Exception) {
            Log.e(TAG, "Pelagic download error: ${e.message}")
        }

        gatt.disconnect(); withTimeoutOrNull(5_000) { done.await() }; gatt.close()
        return dives
    }

    // ── Pelagic/Oceanic ProPlus3 profile header parser ────────────────────────
    // Profile header layout (oceanic_atom2_parser.c, default case):
    //   [0]   magic 0xA5
    //   [1]   profile version
    //   [2]   BCD dive number
    //   [3]   packed: year=(p[3]&0xC0)>>2 + (p[4]&0x0F), month=(p[5]&0xF0)>>4, day=p[5]&0x0F
    //   [4]   packed: hour=p[6]&0x1F, min=p[7]
    //   [6-7] maxdepth: (p[6]|((p[7]&0xF0)>>4)) in feet × 0.3048
    //   Alternative: some models store depth directly in metres × 10 at offsets [8-9]
    //   [8-9] divetime minutes, [10-11] divetime seconds (BCD or binary)

    private fun parsePelagicDiveHeader(data: ByteArray, diveNum: Int, source: String): DiveSummary? {
        if (data.size < 16) return null
        return try {
            // Check for valid start marker
            if (data[0] != 0xA5.toByte() && data[0] != 0x00.toByte()) return null
            if (data[0] == 0x00.toByte()) return null  // empty slot

            // Date: oceanic_atom2_parser.c default case (lines 304-355 roughly)
            val year = (((data[3].toInt() and 0xC0) ushr 2) + (data[4].toInt() and 0x0F)) + 2000
            val month = (data[5].toInt() and 0xF0) ushr 4
            val day = data[5].toInt() and 0x0F
            val hour = data[6].toInt() and 0x1F
            val min = data[7].toInt() and 0xFF

            if (month < 1 || month > 12 || day < 1 || day > 31 || year < 2000 || year > 2100) return null

            // Depth: stored as tenths of feet in most models
            val depthTenthFt = ((data[8].toInt() and 0xFF) or ((data[9].toInt() and 0x0F) shl 8))
            val maxDepthM = depthTenthFt * 0.03048  // tenths of feet → metres

            // Dive time in minutes at [10-11] (LE uint16 in some, BCD in others — try binary)
            val durationMin = (data[10].toInt() and 0xFF) or ((data[11].toInt() and 0xFF) shl 8)
            val durationS = durationMin * 60

            if (maxDepthM < 0.5 || durationS < 30) return null

            val cal = java.util.Calendar.getInstance(TimeZone.getTimeZone("UTC"))
            cal.set(year, month - 1, day, hour, min, 0)
            val divedAt = java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.US)
                .apply { timeZone = TimeZone.getTimeZone("UTC") }
                .format(cal.time)

            DiveSummary(source, diveNum, divedAt, maxDepthM, durationS, null)
        } catch (_: Exception) { null }
    }

    // ── SLIP ──────────────────────────────────────────────────────────────────

    private fun slipEncode(data: ByteArray): ByteArray {
        val out = mutableListOf(SLIP_END)
        for (b in data) when (b) {
            SLIP_END -> out.addAll(listOf(SLIP_ESC, SLIP_ESC_END))
            SLIP_ESC -> out.addAll(listOf(SLIP_ESC, SLIP_ESC_ESC))
            else     -> out.add(b)
        }
        out.add(SLIP_END)
        return out.toByteArray()
    }

    private fun slipDecode(buf: MutableList<Byte>, out: MutableList<ByteArray>) {
        while (true) {
            val s = buf.indexOf(SLIP_END).takeIf { it >= 0 } ?: break
            val e = buf.indexOf(SLIP_END).let { buf.subList(s + 1, buf.size).indexOf(SLIP_END) }
                .takeIf { it >= 0 }?.plus(s + 1) ?: break
            val raw = buf.subList(s + 1, e)
            buf.subList(0, e + 1).clear()
            val decoded = mutableListOf<Byte>()
            var i = 0
            while (i < raw.size) {
                if (raw[i] == SLIP_ESC && i + 1 < raw.size) {
                    decoded.add(if (raw[i + 1] == SLIP_ESC_END) SLIP_END else SLIP_ESC)
                    i += 2
                } else { decoded.add(raw[i]); i++ }
            }
            if (decoded.isNotEmpty()) out.add(decoded.toByteArray())
        }
    }

    // ── HDLC ──────────────────────────────────────────────────────────────────

    private fun hdlcEncode(data: ByteArray): ByteArray {
        val out = mutableListOf(HDLC_FLAG)
        for (b in data) {
            if (b == HDLC_FLAG || b == HDLC_ESC) { out.add(HDLC_ESC); out.add((b.toInt() xor 0x20).toByte()) }
            else out.add(b)
        }
        out.add(HDLC_FLAG)
        return out.toByteArray()
    }

    private fun hdlcDecode(buf: MutableList<Byte>, out: MutableList<ByteArray>) {
        while (true) {
            val s = buf.indexOf(HDLC_FLAG).takeIf { it >= 0 } ?: break
            val e = buf.subList(s + 1, buf.size).indexOf(HDLC_FLAG)
                .takeIf { it >= 0 }?.plus(s + 1) ?: break
            val raw = buf.subList(s + 1, e)
            buf.subList(0, e + 1).clear()
            val decoded = mutableListOf<Byte>()
            var i = 0
            while (i < raw.size) {
                if (raw[i] == HDLC_ESC && i + 1 < raw.size) {
                    decoded.add((raw[i + 1].toInt() xor 0x20).toByte()); i += 2
                } else { decoded.add(raw[i]); i++ }
            }
            if (decoded.size > 4) out.add(decoded.toByteArray())
        }
    }

    // ── Packet parsers ────────────────────────────────────────────────────────

    // ── Suunto EON SBEM file parser ───────────────────────────────────────────
    // File header (12 bytes): [timestamp LE u32][SBEM magic 4 bytes][0x00000000]
    // Then a stream of typed blocks, each starting with 0x00 validation byte:
    //   Descriptor: [0x00][textLen>0][XML descriptor string][typeId u16 LE]
    //   Data:       [0x00][0x00][typeId u8][dataLen u8][bytes...]
    // Key field paths (parsed dynamically):
    //   "Sample.Depth"       → uint16 LE cm      → ÷100 = metres
    //   "Sample.Temperature" → int16 LE deci-°C  → ÷10 = °C  (skip if ≤ -3000)
    //   "Sample.Time"        → uint16 LE ms delta → accumulate

    private fun parseSbemFile(data: ByteArray, filename: String, diveNum: Int, source: String): DiveSummary? {
        if (data.size < 12) return null
        return try {
            val fileTs = (data[0].toLong() and 0xFF) or ((data[1].toLong() and 0xFF) shl 8) or
                         ((data[2].toLong() and 0xFF) shl 16) or ((data[3].toLong() and 0xFF) shl 24)

            // Dynamic type ID registry
            var depthId = -1; var tempId = -1; var timeId = -1

            val samples    = mutableListOf<DiveSample>()
            var accTimeMs  = 0L
            var maxDepthM  = 0.0
            var lastTempC: Double? = null
            var pos        = 12

            while (pos + 1 < data.size) {
                // Each block starts with 0x00 validation byte
                if ((data[pos].toInt() and 0xFF) != 0x00) { pos++; continue }
                pos++
                if (pos >= data.size) break

                val textLen = data[pos].toInt() and 0xFF
                pos++

                if (textLen == 0xFF) {
                    // Long descriptor (4-byte LE length)
                    if (pos + 4 > data.size) break
                    val longLen = (data[pos].toInt() and 0xFF) or ((data[pos+1].toInt() and 0xFF) shl 8) or
                                  ((data[pos+2].toInt() and 0xFF) shl 16) or ((data[pos+3].toInt() and 0xFF) shl 24)
                    pos += 4
                    if (longLen <= 0 || pos + longLen + 2 > data.size) break
                    val desc = String(data, pos, longLen, Charsets.ISO_8859_1)
                    pos += longLen
                    val typeId = (data[pos].toInt() and 0xFF) or ((data[pos+1].toInt() and 0xFF) shl 8)
                    pos += 2
                    when {
                        "Sample.Depth"       in desc || "SampleDepth"       in desc -> depthId = typeId
                        "Sample.Temperature" in desc || "SampleTemperature" in desc -> tempId  = typeId
                        "Sample.Time"        in desc || "SampleTime"        in desc -> timeId  = typeId
                    }
                    Log.d(TAG, "EON SBEM type $typeId (long): ${desc.take(80)}")

                } else if (textLen > 0) {
                    // Short descriptor
                    if (pos + textLen + 2 > data.size) break
                    val desc = String(data, pos, textLen, Charsets.ISO_8859_1)
                    pos += textLen
                    val typeId = (data[pos].toInt() and 0xFF) or ((data[pos+1].toInt() and 0xFF) shl 8)
                    pos += 2
                    when {
                        "Sample.Depth"       in desc || "SampleDepth"       in desc -> depthId = typeId
                        "Sample.Temperature" in desc || "SampleTemperature" in desc -> tempId  = typeId
                        "Sample.Time"        in desc || "SampleTime"        in desc -> timeId  = typeId
                    }
                    Log.d(TAG, "EON SBEM type $typeId: ${desc.take(80)}")

                } else {
                    // Data record: [typeId u8][dataLen u8][bytes...]
                    if (pos + 2 > data.size) break
                    val typeId  = data[pos].toInt() and 0xFF
                    val dataLen = data[pos+1].toInt() and 0xFF
                    pos += 2
                    if (pos + dataLen > data.size) break
                    val b = data.copyOfRange(pos, pos + dataLen)
                    pos += dataLen

                    when (typeId) {
                        timeId -> if (b.size >= 2) {
                            accTimeMs += (b[0].toInt() and 0xFF) or ((b[1].toInt() and 0xFF) shl 8)
                        }
                        depthId -> if (b.size >= 2) {
                            val depthCm = (b[0].toInt() and 0xFF) or ((b[1].toInt() and 0xFF) shl 8)
                            val depthM  = depthCm / 100.0
                            val timeS   = (accTimeMs / 1000).toInt()
                            if (depthM > maxDepthM) maxDepthM = depthM
                            val lastS   = samples.lastOrNull()?.timeS ?: -10
                            if (timeS - lastS >= 9)
                                samples.add(DiveSample(timeS, depthM, lastTempC))
                        }
                        tempId -> if (b.size >= 2) {
                            val raw = (b[0].toInt() and 0xFF) or ((b[1].toInt() and 0xFF) shl 8)
                            val tc  = raw.toShort().toInt() / 10.0
                            if (tc > -300.0 && tc in -5.0..50.0) {
                                lastTempC = tc
                                if (samples.isNotEmpty())
                                    samples[samples.lastIndex] = samples.last().copy(tempC = tc)
                            }
                        }
                    }
                }
            }

            val durationS = (accTimeMs / 1000).toInt()
            if (maxDepthM < 0.5 || durationS < 30) return null

            val sdf = java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.US)
                .apply { timeZone = TimeZone.getTimeZone("UTC") }
            val divedAt  = sdf.format(java.util.Date(fileTs * 1000L))
            val minTempC = samples.mapNotNull { it.tempC }.minOrNull()
            Log.i(TAG, "EON SBEM $filename: ${samples.size} samples maxD=${"%.1f".format(maxDepthM)}m dur=${durationS}s depthId=$depthId tempId=$tempId timeId=$timeId")
            DiveSummary(source, diveNum, divedAt, maxDepthM, durationS, minTempC, samples)
        } catch (e: Exception) {
            Log.w(TAG, "EON SBEM parse error $filename: ${e.message}"); null
        }
    }

    private fun parseEonPackets(packets: List<ByteArray>, source: String): List<DiveSummary> {
        val dives = mutableListOf<DiveSummary>()
        var seq = 0
        for (pkt in packets) {
            if (pkt.size < 20) continue
            if (pkt[0] != 0x00.toByte() || pkt[3] != 0x01.toByte()) continue
            try {
                val ts = java.nio.ByteBuffer.wrap(pkt, 4, 4).int.toLong() and 0xFFFFFFFFL
                val divedAt = java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.US)
                    .apply { timeZone = TimeZone.getTimeZone("UTC") }
                    .format(java.util.Date(ts * 1000L))
                val depth    = (java.nio.ByteBuffer.wrap(pkt, 8, 2).short.toInt() and 0xFFFF) * 0.01
                val duration = java.nio.ByteBuffer.wrap(pkt, 10, 2).short.toInt() and 0xFFFF
                val tempRaw  = if (pkt.size >= 14) java.nio.ByteBuffer.wrap(pkt, 12, 2).short.toInt() else null
                val tempC    = tempRaw?.let { it / 10.0 }?.takeIf { it >= -5 && it <= 50 }
                if (depth > 0.5 && duration > 30) {
                    seq++
                    dives.add(DiveSummary(source, seq, divedAt, depth, duration, tempC))
                }
            } catch (_: Exception) { continue }
        }
        return dives
    }

    // ── OSTC profile parser (hw_ostc_parser.c) ───────────────────────────────
    // Supports versions 0x20/0x21 (original OSTC) and 0x23/0x24 (OSTC3/4/5).
    // Profile layout (OSTC3 v0x23/v0x24):
    //   offset  8 = version byte
    //   offset 12 = datetime [YY-2000, MM, DD, HH, mm]
    //   offset 17 = maxdepth uint16 LE (1/100 m)
    //   offset 19 = divetime uint16 LE (seconds)
    //   offset 22 = temperature int16 LE (1/10 °C)

    private fun parseOstcProfile(data: ByteArray, diveNum: Int, source: String): DiveSummary? {
        if (data.size < 30) return null
        return try {
            val version = data[8].toInt() and 0xFF
            val (year, month, day, hour, min) = when (version) {
                0x23, 0x24 -> listOf(
                    (data[12].toInt() and 0xFF) + 2000,
                    data[13].toInt() and 0xFF,
                    data[14].toInt() and 0xFF,
                    data[15].toInt() and 0xFF,
                    data[16].toInt() and 0xFF
                )
                0x20, 0x21 -> listOf(  // original OSTC: [MM, DD, YY-2000, HH, mm]
                    (data[14].toInt() and 0xFF) + 2000,
                    data[12].toInt() and 0xFF,
                    data[13].toInt() and 0xFF,
                    data[15].toInt() and 0xFF,
                    data[16].toInt() and 0xFF
                )
                else -> return null
            }
            val maxDepthM = ((data[17].toInt() and 0xFF) or ((data[18].toInt() and 0xFF) shl 8)) / 100.0
            val durationS = (data[19].toInt() and 0xFF) or ((data[20].toInt() and 0xFF) shl 8)
            val tempRaw   = (data[22].toInt() and 0xFF) or ((data[23].toInt() and 0xFF) shl 8)
            val tempC     = (tempRaw.toShort().toInt()) / 10.0

            if (maxDepthM < 0.5 || durationS < 30 || year < 2000 || year > 2100) return null

            val cal = java.util.Calendar.getInstance(TimeZone.getTimeZone("UTC"))
            cal.set(year, month - 1, day, hour, min, 0)
            val divedAt = java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.US)
                .apply { timeZone = TimeZone.getTimeZone("UTC") }
                .format(cal.time)

            DiveSummary(source, diveNum, divedAt, maxDepthM, durationS,
                tempC.takeIf { it >= -5 && it <= 50 })
        } catch (_: Exception) { null }
    }

    // ── Cressi dive parser (cressi_goa_parser.c, v4 firmware layout) ──────────
    // v4 layout (firmware 200-299): header starts at offset 0 of dive blob.
    //   offset 12 = datetime [year_lo, year_hi, MM, DD, HH, mm]
    //   offset 20 = divetime uint16 LE (seconds)
    //   offset 73 = maxdepth uint16 LE (cm)
    //   offset 77 = temperature int16 LE (1/10 °C)

    private fun parseCressiDive(data: ByteArray, diveNum: Int, source: String): DiveSummary? {
        if (data.size < 80) return null
        return try {
            val year  = (data[12].toInt() and 0xFF) or ((data[13].toInt() and 0xFF) shl 8)
            val month = data[14].toInt() and 0xFF
            val day   = data[15].toInt() and 0xFF
            val hour  = data[16].toInt() and 0xFF
            val min   = data[17].toInt() and 0xFF
            val durationS  = (data[20].toInt() and 0xFF) or ((data[21].toInt() and 0xFF) shl 8)
            val maxDepthM  = ((data[73].toInt() and 0xFF) or ((data[74].toInt() and 0xFF) shl 8)) / 100.0
            val tempRaw    = (data[77].toInt() and 0xFF) or ((data[78].toInt() and 0xFF) shl 8)
            val tempC      = (tempRaw.toShort().toInt()) / 10.0

            if (maxDepthM < 0.5 || durationS < 30 || year < 2000 || year > 2100) return null

            val cal = java.util.Calendar.getInstance(TimeZone.getTimeZone("UTC"))
            cal.set(year, month - 1, day, hour, min, 0)
            val divedAt = java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.US)
                .apply { timeZone = TimeZone.getTimeZone("UTC") }
                .format(cal.time)

            DiveSummary(source, diveNum, divedAt, maxDepthM, durationS,
                tempC.takeIf { it >= -5 && it <= 50 })
        } catch (_: Exception) { null }
    }

    // ── Supabase upload ───────────────────────────────────────────────────────

    private fun uploadDive(dive: DiveSummary): String? {
        val prefs = getSharedPreferences("deeplog", android.content.Context.MODE_PRIVATE)
        val sbUrl = prefs.getString("supabase_url", SUPABASE_URL)?.trimEnd('/') ?: SUPABASE_URL
        val sbKey = prefs.getString("supabase_key", SUPABASE_KEY) ?: SUPABASE_KEY
        val url  = java.net.URL("$sbUrl/rest/v1/dives")
        val conn = url.openConnection() as java.net.HttpURLConnection
        conn.requestMethod = "POST"
        conn.setRequestProperty("apikey",        sbKey)
        conn.setRequestProperty("Authorization", "Bearer $sbKey")
        conn.setRequestProperty("Content-Type",  "application/json")
        conn.setRequestProperty("Prefer",        "return=representation")
        conn.doOutput = true
        conn.connectTimeout = 10_000
        conn.readTimeout    = 10_000

        val body = JSONObject().apply {
            put("username",    username)
            put("computer",    dive.source)
            put("dive_number", dive.number)
            put("dived_at",    dive.divedAt)
            put("max_depth_m", dive.maxDepthM)
            put("duration_s",  dive.durationS)
            if (dive.tempC != null) put("min_temp_c", dive.tempC)
            if (dive.tempC != null) put("avg_temp_c", dive.tempC)
        }.toString()

        conn.outputStream.use { it.write(body.toByteArray()) }
        val code = conn.responseCode
        if (code !in 200..299) {
            val err = runCatching { conn.errorStream?.bufferedReader()?.readText() }.getOrNull()
            conn.disconnect()
            throw Exception("Supabase HTTP $code: $err")
        }
        val resp = conn.inputStream.bufferedReader().readText()
        conn.disconnect()
        return runCatching { JSONArray(resp).getJSONObject(0).getString("id") }.getOrNull()
    }

    private fun uploadSamples(diveId: String, dive: DiveSummary) {
        if (dive.samples.isEmpty()) return
        val prefs = getSharedPreferences("deeplog", android.content.Context.MODE_PRIVATE)
        val sbUrl = prefs.getString("supabase_url", SUPABASE_URL)?.trimEnd('/') ?: SUPABASE_URL
        val sbKey = prefs.getString("supabase_key", SUPABASE_KEY) ?: SUPABASE_KEY
        val url  = java.net.URL("$sbUrl/rest/v1/dive_samples")
        val conn = url.openConnection() as java.net.HttpURLConnection
        conn.requestMethod = "POST"
        conn.setRequestProperty("apikey",        sbKey)
        conn.setRequestProperty("Authorization", "Bearer $sbKey")
        conn.setRequestProperty("Content-Type",  "application/json")
        conn.setRequestProperty("Prefer",        "return=minimal")
        conn.doOutput = true
        conn.connectTimeout = 15_000
        conn.readTimeout    = 30_000

        val arr = JSONArray()
        dive.samples.forEach { s ->
            arr.put(JSONObject().apply {
                put("dive_id",  diveId)
                put("username", username)
                put("dived_at", dive.divedAt)
                put("time_s",   s.timeS)
                put("depth_m",  s.depthM)
                if (s.tempC != null) put("temp_c", s.tempC)
            })
        }

        conn.outputStream.use { it.write(arr.toString().toByteArray()) }
        val code = conn.responseCode
        if (code !in 200..299) {
            val err = runCatching { conn.errorStream?.bufferedReader()?.readText() }.getOrNull()
            conn.disconnect()
            throw Exception("Supabase samples HTTP $code: $err")
        }
        conn.disconnect()
        Log.i(TAG, "Uploaded ${dive.samples.size} samples for dive ${dive.number}")
    }

    // ── State helpers ─────────────────────────────────────────────────────────

    private fun setState(s: BridgeState) {
        state.set(s)
        currentState = s
        updateNotification(s.message.ifEmpty { "Bridge running on :$PORT" })
    }

    // ── Notification ──────────────────────────────────────────────────────────

    private fun createNotificationChannel() {
        val chan = NotificationChannel(
            NOTIF_CHANNEL, "Bridge status", NotificationManager.IMPORTANCE_LOW
        ).apply { description = "DeepLog BLE bridge" }
        getSystemService(NotificationManager::class.java).createNotificationChannel(chan)
    }

    private fun buildNotification(text: String): Notification {
        val tap = PendingIntent.getActivity(
            this, 0,
            Intent(this, MainActivity::class.java),
            PendingIntent.FLAG_IMMUTABLE
        )
        return Notification.Builder(this, NOTIF_CHANNEL)
            .setContentTitle("DeepLog Bridge")
            .setContentText(text)
            .setSmallIcon(android.R.drawable.stat_sys_data_bluetooth)
            .setContentIntent(tap)
            .setOngoing(true)
            .build()
    }

    private fun updateNotification(text: String) {
        getSystemService(NotificationManager::class.java).notify(NOTIF_ID, buildNotification(text))
    }
}

// ── DiveComputerType helper ───────────────────────────────────────────────────

fun DiveComputerType.toBrandName(): String = when (this) {
    DiveComputerType.SUUNTO_EON    -> "Suunto"
    DiveComputerType.SHEARWATER    -> "Shearwater"
    DiveComputerType.HW_OSTC_TELIT -> "Heinrichs-Weikamp OSTC (Telit)"
    DiveComputerType.HW_OSTC_UBLOX -> "Heinrichs-Weikamp OSTC (U-Blox)"
    DiveComputerType.MARES         -> "Mares"
    DiveComputerType.PELAGIC_A,
    DiveComputerType.PELAGIC_B     -> "Oceanic/Aqualung/Sherwood/Apeks"
    DiveComputerType.SCUBAPRO      -> "ScubaPro"
    DiveComputerType.DIVESOFT      -> "Divesoft"
    DiveComputerType.CRESSI        -> "Cressi"
    DiveComputerType.GENERIC_UART  -> "Generic (Nordic UART)"
    DiveComputerType.UNKNOWN       -> "Unknown"
}

// ── Data classes ──────────────────────────────────────────────────────────────

data class DiveSample(
    val timeS:  Int,
    val depthM: Double,
    val tempC:  Double?
)

data class DiveSummary(
    val source:     String,
    val number:     Int,
    val divedAt:    String?,
    val maxDepthM:  Double,
    val durationS:  Int,
    val tempC:      Double?,
    val samples:    List<DiveSample> = emptyList(),
    val recordAddr: Long             = 0L
) {
    fun summary(): Map<String, Any?> = mapOf(
        "source"      to source,
        "number"      to number,
        "date"        to divedAt,
        "max_depth_m" to maxDepthM,
        "duration_s"  to durationS,
        "min_temp_c"  to tempC,
        "avg_temp_c"  to tempC,
    )
}

// ── Minimal HTTP server ───────────────────────────────────────────────────────

class BridgeHttpServer(private val port: Int, private val svc: BridgeService) {

    private var serverSocket: ServerSocket? = null
    private var running = false
    private val executor = java.util.concurrent.Executors.newCachedThreadPool()

    fun start() {
        running = true
        executor.execute {
            try {
                serverSocket = ServerSocket(port).also { ss ->
                    ss.reuseAddress = true
                    while (running) {
                        try { handle(ss.accept()) } catch (_: Exception) {}
                    }
                }
            } catch (e: Exception) {
                android.util.Log.e("BridgeHttp", "Server error: ${e.message}")
            }
        }
    }

    fun stop() {
        running = false
        runCatching { serverSocket?.close() }
        executor.shutdownNow()
    }

    private fun handle(socket: Socket) {
        executor.execute {
            try {
                socket.use {
                    val reader = BufferedReader(InputStreamReader(it.getInputStream()))
                    val writer = PrintWriter(it.getOutputStream(), true)

                    val requestLine = reader.readLine() ?: return@use
                    val parts  = requestLine.split(" ")
                    if (parts.size < 2) return@use
                    val method = parts[0]
                    val path   = parts[1].split("?")[0]

                    var contentLength = 0
                    while (true) {
                        val line = reader.readLine() ?: break
                        if (line.isEmpty()) break
                        val colon = line.indexOf(':')
                        if (colon > 0 && line.substring(0, colon).trim().lowercase() == "content-length")
                            contentLength = line.substring(colon + 1).trim().toIntOrNull() ?: 0
                    }

                    val body = if (contentLength > 0) {
                        val buf = CharArray(contentLength)
                        reader.read(buf, 0, contentLength)
                        String(buf)
                    } else ""

                    val (status, json) = route(method, path, body)
                    val bytes = json.toByteArray(Charsets.UTF_8)
                    writer.print("HTTP/1.1 $status\r\n")
                    writer.print("Content-Type: application/json\r\n")
                    writer.print("Content-Length: ${bytes.size}\r\n")
                    writer.print("Access-Control-Allow-Origin: *\r\n")
                    writer.print("Access-Control-Allow-Methods: GET, POST, OPTIONS\r\n")
                    writer.print("Access-Control-Allow-Headers: Content-Type\r\n")
                    writer.print("Connection: close\r\n")
                    writer.print("\r\n")
                    writer.flush()
                    it.getOutputStream().write(bytes)
                    it.getOutputStream().flush()
                }
            } catch (_: Exception) {}
        }
    }

    private fun route(method: String, path: String, body: String): Pair<String, String> {
        if (method == "OPTIONS") return "204 No Content" to ""
        return when {
            path == "/health" && method == "GET" ->
                "200 OK" to """{"ok":true,"dctool":"android-native","version":"1.0.0"}"""

            path == "/status" && method == "GET" ->
                "200 OK" to svc.getState()

            path == "/download" && method == "POST" -> {
                val obj     = try { JSONObject(body) } catch (_: Exception) { JSONObject() }
                val address = obj.optString("address").ifEmpty { null }
                val user    = obj.optString("username").ifEmpty { "anonymous" }
                svc.startDownload(address, user)
                "200 OK" to """{"started":true}"""
            }

            path == "/reset" && method == "POST" -> {
                svc.resetState()
                "200 OK" to """{"ok":true}"""
            }

            else -> "404 Not Found" to """{"error":"not found"}"""
        }
    }
}
