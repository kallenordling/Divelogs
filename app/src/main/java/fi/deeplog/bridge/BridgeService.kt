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
import android.content.Context
import android.content.Intent
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
                    uploadDive(dive)
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

    @SuppressLint("MissingPermission")
    private suspend fun downloadEon(device: BluetoothDevice): List<DiveSummary> {
        val packets  = mutableListOf<ByteArray>()
        val rxBuffer = mutableListOf<Byte>()
        val done     = CompletableDeferred<Unit>()

        setState(state.get().copy(message = "Connecting to ${device.name ?: device.address}…", progress = 22))

        val gattCallback = object : BluetoothGattCallback() {
            var txChar: BluetoothGattCharacteristic? = null

            override fun onConnectionStateChange(gatt: BluetoothGatt, status: Int, newState: Int) {
                when {
                    newState == BluetoothProfile.STATE_CONNECTED && status == BluetoothGatt.GATT_SUCCESS -> {
                        setState(state.get().copy(message = "Connected — check screen for passkey…", progress = 30))
                        gatt.discoverServices()
                    }
                    newState == BluetoothProfile.STATE_DISCONNECTED -> {
                        if (status != BluetoothGatt.GATT_SUCCESS && !done.isCompleted)
                            done.completeExceptionally(Exception("GATT disconnected (status $status). Move closer and retry."))
                        else done.complete(Unit)
                    }
                    status != BluetoothGatt.GATT_SUCCESS -> {
                        done.completeExceptionally(Exception("GATT error $status — try disabling and re-enabling Bluetooth."))
                    }
                }
            }

            override fun onServicesDiscovered(gatt: BluetoothGatt, status: Int) {
                if (status != BluetoothGatt.GATT_SUCCESS) {
                    done.completeExceptionally(Exception("Service discovery failed (status $status)"))
                    return
                }
                val svc = gatt.getService(EON_SERVICE) ?: run {
                    done.completeExceptionally(Exception("EON service not found — is the device in download mode?"))
                    return
                }
                setState(state.get().copy(message = "Pairing… confirm passkey on device if prompted", progress = 38))
                txChar = svc.getCharacteristic(EON_TX)
                val rxChar = svc.getCharacteristic(EON_RX)
                gatt.setCharacteristicNotification(rxChar, true)
                rxChar.getDescriptor(CCCD)?.also {
                    it.value = BluetoothGattDescriptor.ENABLE_NOTIFICATION_VALUE
                    gatt.writeDescriptor(it)
                }
            }

            override fun onDescriptorWrite(gatt: BluetoothGatt, descriptor: BluetoothGattDescriptor, status: Int) {
                setState(state.get().copy(message = "Requesting dive list…", progress = 45))
                val cmd = hdlcEncode(byteArrayOf(0x00, 0x00, 0x00, 0x01, 0x00, 0x10))
                txChar?.let { it.value = cmd; gatt.writeCharacteristic(it) }
            }

            @Deprecated("Required for API < 33")
            override fun onCharacteristicChanged(gatt: BluetoothGatt, characteristic: BluetoothGattCharacteristic) {
                rxBuffer.addAll(characteristic.value.toList())
                hdlcDecode(rxBuffer, packets)
                setState(state.get().copy(
                    message  = "Receiving data… (${packets.size} frames)",
                    progress = 50 + minOf(25, packets.size)
                ))
            }
        }

        val gatt = device.connectGatt(this, false, gattCallback, BluetoothDevice.TRANSPORT_LE)
        withTimeoutOrNull(60_000) { done.await() }
            ?: run { gatt.close(); throw Exception("Connection timed out after 60 s — is the device awake and nearby?") }
        gatt.close()

        if (packets.isEmpty()) throw Exception("Connected but received no data — check device is in Bluetooth download mode")
        setState(state.get().copy(message = "Parsing ${packets.size} frames…", progress = 75))
        return parseEonPackets(packets, device.name ?: "Suunto EON")
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

            var manifestAddr = 0xE0000000L
            var numDives     = 64
            if (logPayload != null && logPayload.size >= 8 && logPayload[0] == 0x62.toByte()) {
                // Response: [0x62, 0x80, 0x21, format, addr×4, ndives×2, ...]
                if (logPayload.size >= 9) {
                    manifestAddr = ((logPayload[3].toLong() and 0xFF) shl 24) or
                                   ((logPayload[4].toLong() and 0xFF) shl 16) or
                                   ((logPayload[5].toLong() and 0xFF) shl 8)  or
                                   (logPayload[6].toLong() and 0xFF)
                }
                if (logPayload.size >= 11)
                    numDives = ((logPayload[7].toInt() and 0xFF) shl 8) or (logPayload[8].toInt() and 0xFF)
            }
            numDives = numDives.coerceIn(1, 256)
            Log.i(TAG, "SW manifest: addr=0x${manifestAddr.toString(16)} numDives=$numDives")

            // ── 3. Download manifest (32 bytes per dive entry) ────────────────
            val ENTRY_SIZE   = 32
            val manifestSize = numDives * ENTRY_SIZE
            setState(state.get().copy(message = "Downloading dive list ($numDives entries)…", progress = 50))

            fun Int.b3() = byteArrayOf(((this shr 16) and 0xFF).toByte(), ((this shr 8) and 0xFF).toByte(), (this and 0xFF).toByte())
            fun Long.b4() = byteArrayOf(((this shr 24) and 0xFF).toByte(), ((this shr 16) and 0xFF).toByte(), ((this shr 8) and 0xFF).toByte(), (this and 0xFF).toByte())

            swSend(byteArrayOf(0x35, 0x00, 0x34) + manifestAddr.b4() + manifestSize.b3())
            val initAck = swRecv(8_000)
            Log.i(TAG, "SW manifest init: ${initAck?.map { "0x${it.toUByte().toString(16)}" }}")

            val manifestData = mutableListOf<Byte>()
            var blockNum = 1
            while (manifestData.size < manifestSize && blockNum <= 128) {
                swSend(byteArrayOf(0x36, blockNum.toByte()))
                val blkPkt = swRecv(15_000) ?: break
                val blkPay = swPayload(blkPkt)
                if (blkPay != null && blkPay.isNotEmpty() && blkPay[0] == 0x76.toByte()) {
                    val blkData = blkPay.drop(2) // skip [0x76, block_num]
                    manifestData.addAll(blkData)
                    Log.d(TAG, "SW block $blockNum: ${blkData.size} bytes (total ${manifestData.size})")
                } else {
                    Log.w(TAG, "SW block $blockNum unexpected: ${blkPay?.take(4)?.map { "0x${it.toUByte().toString(16)}" }}")
                    break
                }
                blockNum++
            }
            swSend(byteArrayOf(0x37)) // quit download

            Log.i(TAG, "SW manifest: ${manifestData.size}/${manifestSize} bytes, blocks=$blockNum")
            setState(state.get().copy(message = "Parsing dive manifest…", progress = 70))

            // ── 4. Parse manifest entries ─────────────────────────────────────
            // 32-byte entry layout (Shearwater proprietary):
            //  0..3  dive record address (BE)
            //  4..7  dive record size (BE)
            //  8     dive type / flags
            //  9..11 date: year-2000, month, day
            // 12..14 time: hour, minute, second
            // 15..16 max depth cm (BE)
            // 17..18 dive duration seconds (BE)
            // 19..20 min temp ×10°C (BE signed)
            val dives = mutableListOf<DiveSummary>()
            var seq = 0
            var off = 0
            while (off + ENTRY_SIZE <= manifestData.size) {
                val e = manifestData.subList(off, off + ENTRY_SIZE).toByteArray()
                off += ENTRY_SIZE
                if (e.all { it == 0xFF.toByte() } || e.all { it == 0x00.toByte() }) continue
                Log.d(TAG, "SW entry@${off-ENTRY_SIZE}: ${e.map { "0x${it.toUByte().toString(16)}" }}")
                try {
                    val year  = (e[9].toInt()  and 0xFF) + 2000
                    val month = (e[10].toInt() and 0xFF)
                    val day   = (e[11].toInt() and 0xFF)
                    val hour  = (e[12].toInt() and 0xFF)
                    val min   = (e[13].toInt() and 0xFF)
                    val sec   = (e[14].toInt() and 0xFF)
                    val depthCm = ((e[15].toInt() and 0xFF) shl 8) or (e[16].toInt() and 0xFF)
                    val durSec  = ((e[17].toInt() and 0xFF) shl 8) or (e[18].toInt() and 0xFF)
                    val tempRaw = ((e[19].toInt() shl 8) or (e[20].toInt() and 0xFF)).toShort().toInt()

                    if (depthCm < 50 || durSec < 30 || year < 2010 || year > 2035 ||
                        month !in 1..12 || day !in 1..31) continue

                    seq++
                    dives.add(DiveSummary(
                        source    = device.name ?: "Shearwater",
                        number    = seq,
                        divedAt   = String.format(Locale.US, "%04d-%02d-%02dT%02d:%02d:%02dZ", year, month, day, hour, min, sec),
                        maxDepthM = depthCm / 100.0,
                        durationS = durSec,
                        tempC     = if (tempRaw in -50..500) tempRaw / 10.0 else null
                    ))
                    Log.i(TAG, "SW dive $seq: $year-$month-$day ${depthCm/100.0}m ${durSec}s")
                } catch (ex: Exception) {
                    Log.w(TAG, "SW entry parse: ${ex.message}")
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

    private fun uploadDive(dive: DiveSummary) {
        val url  = java.net.URL("$SUPABASE_URL/rest/v1/dives")
        val conn = url.openConnection() as java.net.HttpURLConnection
        conn.requestMethod = "POST"
        conn.setRequestProperty("apikey",        SUPABASE_KEY)
        conn.setRequestProperty("Authorization", "Bearer $SUPABASE_KEY")
        conn.setRequestProperty("Content-Type",  "application/json")
        conn.setRequestProperty("Prefer",        "return=minimal")
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
        conn.disconnect()
        if (code !in 200..299) throw Exception("Supabase HTTP $code")
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

// ── Data class ────────────────────────────────────────────────────────────────

data class DiveSummary(
    val source:    String,
    val number:    Int,
    val divedAt:   String?,
    val maxDepthM: Double,
    val durationS: Int,
    val tempC:     Double?
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
