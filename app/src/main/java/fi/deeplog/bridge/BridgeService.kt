// BridgeService.kt
//
// Foreground service that:
//   1. Runs NanoHTTPD on localhost:8765 — same API as suunto_bridge.py
//   2. Handles BLE download for EON Steel (passkey via Android OS) and Perdix 2 (no passkey)
//   3. Pushes parsed dives directly to Supabase
//
// The web app (DeepLog in Chrome) talks to this via http://localhost:8765
// exactly the same way it talks to the Python bridge — no web app changes needed.

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
private val EON_SERVICE  = UUID.fromString("98ae7120-e62e-11e3-badd-0002a5d5c51b")
private val EON_TX       = UUID.fromString("0000fefb-0000-1000-8000-00805f9b34fb")
private val EON_RX       = UUID.fromString("00000002-0000-1000-8000-00805f9b34fb")
private val SW_SERVICE   = UUID.fromString("fe25c237-0ece-443c-b0aa-e02033e7029d")
private val SW_CHAR      = UUID.fromString("27b7570b-359e-45a3-91bb-cf7e70049bd2")
private val CCCD         = UUID.fromString("00002902-0000-1000-8000-00805f9b34fb")

// ── SLIP ──────────────────────────────────────────────────────────────────────
private const val SLIP_END      = 0xC0.toByte()
private const val SLIP_ESC      = 0xDB.toByte()
private const val SLIP_ESC_END  = 0xDC.toByte()
private const val SLIP_ESC_ESC  = 0xDD.toByte()

// ── HDLC ──────────────────────────────────────────────────────────────────────
private const val HDLC_FLAG     = 0x7E.toByte()
private const val HDLC_ESC      = 0x7D.toByte()

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
        createNotificationChannel()
        startForeground(NOTIF_ID, buildNotification("Bridge running on :$PORT"))
        httpServer = BridgeHttpServer(PORT, this).also { it.start() }
        Log.i(TAG, "Bridge started on port $PORT")
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
            // ── Scan for target device ────────────────────────────────────────
            setState(state.get().copy(status = "scanning", message = "Scanning for dive computers…", progress = 10))

            val device = if (targetAddress != null) {
                adapter.getRemoteDevice(targetAddress)
            } else {
                scanForDevice(adapter) ?: run {
                    setState(BridgeState(
                        status = "error",
                        lastError = "No dive computers found. Make sure Bluetooth mode is active on the device."
                    ))
                    return
                }
            }

            val isEon = isEonSteelDevice(device)
            setState(state.get().copy(
                status  = "downloading",
                message = "Connecting to ${device.name ?: device.address}…",
                progress = 20
            ))

            // ── Download dives ────────────────────────────────────────────────
            val dives = if (isEon) downloadEon(device) else downloadShearwater(device)

            if (dives.isEmpty()) {
                setState(BridgeState(status = "error", lastError = "No dives found on device"))
                return
            }

            // ── Upload to Supabase ────────────────────────────────────────────
            setState(state.get().copy(
                status  = "uploading",
                message = "Uploading ${dives.size} dives…",
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

    @SuppressLint("MissingPermission")
    private suspend fun scanForDevice(adapter: BluetoothAdapter): BluetoothDevice? {
        val result   = CompletableDeferred<BluetoothDevice?>()
        val scanner  = adapter.bluetoothLeScanner ?: return null
        val filters  = listOf(
            ScanFilter.Builder().setServiceUuid(ParcelUuid(EON_SERVICE)).build(),
            ScanFilter.Builder().setServiceUuid(ParcelUuid(SW_SERVICE)).build(),
        )
        val settings = ScanSettings.Builder()
            .setScanMode(ScanSettings.SCAN_MODE_LOW_LATENCY)
            .build()

        val callback = object : ScanCallback() {
            override fun onScanResult(callbackType: Int, r: ScanResult) {
                scanner.stopScan(this)
                result.complete(r.device)
            }
            override fun onScanFailed(errorCode: Int) {
                result.complete(null)
            }
        }

        scanner.startScan(filters, settings, callback)
        // Timeout after 15 seconds
        withTimeoutOrNull(15_000) { result.await() }.also {
            runCatching { scanner.stopScan(callback) }
        }
        return result.await()
    }

    @SuppressLint("MissingPermission")
    private fun isEonSteelDevice(device: BluetoothDevice): Boolean {
        val name = device.name?.lowercase() ?: ""
        return name.contains("eon") || name.contains("suunto") ||
               device.uuids?.any { it.uuid == EON_SERVICE } == true
    }

    // ── EON Steel download ────────────────────────────────────────────────────
    //
    // Android handles the BLE Secure Pairing passkey dialog automatically
    // when we call device.connectGatt(). The user sees the system dialog,
    // types the passkey shown on the EON Steel, and the connection proceeds.
    // No special code needed here — this is the key advantage over Web BT.

    @SuppressLint("MissingPermission")
    private suspend fun downloadEon(device: BluetoothDevice): List<DiveSummary> {
        val packets = mutableListOf<ByteArray>()
        val rxBuffer = mutableListOf<Byte>()
        val done = CompletableDeferred<Unit>()

        setState(state.get().copy(message = "Waiting for passkey pairing…", progress = 25))

        val gattCallback = object : BluetoothGattCallback() {
            var txChar: BluetoothGattCharacteristic? = null

            override fun onConnectionStateChange(gatt: BluetoothGatt, status: Int, newState: Int) {
                if (newState == BluetoothProfile.STATE_CONNECTED) {
                    setState(state.get().copy(message = "Connected — discovering services…", progress = 35))
                    gatt.discoverServices()
                } else if (newState == BluetoothProfile.STATE_DISCONNECTED) {
                    done.complete(Unit)
                }
            }

            override fun onServicesDiscovered(gatt: BluetoothGatt, status: Int) {
                val svc = gatt.getService(EON_SERVICE) ?: run {
                    done.completeExceptionally(Exception("EON service not found"))
                    return
                }
                txChar = svc.getCharacteristic(EON_TX)
                val rxChar = svc.getCharacteristic(EON_RX)

                gatt.setCharacteristicNotification(rxChar, true)
                val desc = rxChar.getDescriptor(CCCD)
                desc.value = BluetoothGattDescriptor.ENABLE_NOTIFICATION_VALUE
                gatt.writeDescriptor(desc)
            }

            override fun onDescriptorWrite(gatt: BluetoothGatt, descriptor: BluetoothGattDescriptor, status: Int) {
                setState(state.get().copy(message = "Requesting dive list…", progress = 45))
                // Send "get directory" command (HDLC-framed)
                val cmd = hdlcEncode(byteArrayOf(0x00, 0x00, 0x00, 0x01, 0x00, 0x10))
                txChar?.let {
                    it.value = cmd
                    gatt.writeCharacteristic(it)
                }
            }

            @Deprecated("Required for API < 33")
            override fun onCharacteristicChanged(gatt: BluetoothGatt, characteristic: BluetoothGattCharacteristic) {
                rxBuffer.addAll(characteristic.value.toList())
                hdlcDecode(rxBuffer, packets)
                setState(state.get().copy(
                    message  = "Receiving… (${packets.size} packets)",
                    progress = 50 + minOf(25, packets.size)
                ))
                // Disconnect after 3s of silence handled by timeout below
            }
        }

        val gatt = device.connectGatt(this, false, gattCallback, BluetoothDevice.TRANSPORT_LE)

        // Wait up to 60s for the connection + passkey + data
        withTimeoutOrNull(60_000) { done.await() }
        gatt.close()

        setState(state.get().copy(message = "Parsing ${packets.size} packets…", progress = 75))
        return parseEonPackets(packets, device.name ?: "Suunto EON")
    }

    // ── Shearwater download ───────────────────────────────────────────────────

    @SuppressLint("MissingPermission")
    private suspend fun downloadShearwater(device: BluetoothDevice): List<DiveSummary> {
        val packets  = mutableListOf<ByteArray>()
        val rxBuffer = mutableListOf<Byte>()
        val ready    = CompletableDeferred<BluetoothGattCharacteristic>()
        val done     = CompletableDeferred<Unit>()

        val gattCallback = object : BluetoothGattCallback() {
            override fun onConnectionStateChange(gatt: BluetoothGatt, status: Int, newState: Int) {
                if (newState == BluetoothProfile.STATE_CONNECTED) gatt.discoverServices()
                else if (newState == BluetoothProfile.STATE_DISCONNECTED) done.complete(Unit)
            }

            override fun onServicesDiscovered(gatt: BluetoothGatt, status: Int) {
                val char = gatt.getService(SW_SERVICE)?.getCharacteristic(SW_CHAR)
                    ?: run { done.completeExceptionally(Exception("Shearwater service not found")); return }
                gatt.setCharacteristicNotification(char, true)
                val desc = char.getDescriptor(CCCD)
                desc.value = BluetoothGattDescriptor.ENABLE_NOTIFICATION_VALUE
                gatt.writeDescriptor(desc)
                ready.complete(char)
            }

            override fun onDescriptorWrite(gatt: BluetoothGatt, descriptor: BluetoothGattDescriptor, status: Int) {
                scope.launch {
                    val char = ready.await()
                    // Firmware version request
                    char.value = slipEncode(byteArrayOf(0x01,0x00,0xFF.toByte(),0x01,0x04,0x00,0x22,0x80.toByte(),0x10,0xC0.toByte()))
                    gatt.writeCharacteristic(char)
                    delay(1500)
                    // Dive list request
                    char.value = slipEncode(byteArrayOf(0x01,0x00,0xFF.toByte(),0x01,0x05,0x00,0x2E,0x90.toByte(),0x20,0x00,0xC0.toByte()))
                    gatt.writeCharacteristic(char)
                    delay(8000)
                    gatt.disconnect()
                }
            }

            @Deprecated("Required for API < 33")
            override fun onCharacteristicChanged(gatt: BluetoothGatt, characteristic: BluetoothGattCharacteristic) {
                rxBuffer.addAll(characteristic.value.toList())
                slipDecode(rxBuffer, packets)
            }
        }

        val gatt = device.connectGatt(this, false, gattCallback, BluetoothDevice.TRANSPORT_LE)
        withTimeoutOrNull(30_000) { done.await() }
        gatt.close()

        return parseShearwaterPackets(packets, device.name ?: "Shearwater")
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

    private fun parseShearwaterPackets(packets: List<ByteArray>, source: String): List<DiveSummary> {
        val dives = mutableListOf<DiveSummary>()
        var seq = 0
        for (pkt in packets) {
            if (pkt.size < 16) continue
            val depth    = ((pkt[8].toInt() and 0xFF) shl 8 or (pkt[9].toInt() and 0xFF)) * 0.01
            val duration = (pkt[10].toInt() and 0xFF) shl 8 or (pkt[11].toInt() and 0xFF)
            val tsOffset = ((pkt[12].toLong() and 0xFF) shl 24) or
                           ((pkt[13].toLong() and 0xFF) shl 16) or
                           ((pkt[14].toLong() and 0xFF) shl 8)  or
                           (pkt[15].toLong()  and 0xFF)
            if (depth < 0.5 || duration < 30) continue

            val epoch2000 = 946684800L // 2000-01-01 UTC in Unix seconds
            val ts = (epoch2000 + tsOffset) * 1000L
            val divedAt = java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.US)
                .apply { timeZone = TimeZone.getTimeZone("UTC") }
                .format(java.util.Date(ts))

            var tempC: Double? = null
            if (pkt.size >= 18) {
                val raw = ((pkt[16].toInt() and 0xFF) shl 8) or (pkt[17].toInt() and 0xFF)
                val k10 = raw / 10.0 - 273.15
                val c10 = raw / 10.0
                tempC = when {
                    k10 >= -5 && k10 <= 50 -> k10
                    c10 >= -5 && c10 <= 50 -> c10
                    else -> null
                }
            }

            seq++
            dives.add(DiveSummary(
                source = source, number = seq, divedAt = divedAt,
                maxDepthM = depth, durationS = duration, tempC = tempC
            ))
        }
        return dives
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
        val nm = getSystemService(NotificationManager::class.java)
        nm.notify(NOTIF_ID, buildNotification(text))
    }
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

// ── Minimal HTTP server (stdlib only, no dependencies) ───────────────────────
//
// Plain Java ServerSocket — no third-party library needed.
// Handles GET /health, GET /status, POST /download, POST /reset
// with CORS headers so the WebView JS bridge also works as HTTP fallback.

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

                    // Read request line + headers
                    val requestLine = reader.readLine() ?: return@use
                    val parts  = requestLine.split(" ")
                    if (parts.size < 2) return@use
                    val method = parts[0]
                    val path   = parts[1].split("?")[0]

                    // Read headers
                    val headers = mutableMapOf<String, String>()
                    var contentLength = 0
                    while (true) {
                        val line = reader.readLine() ?: break
                        if (line.isEmpty()) break
                        val colon = line.indexOf(':')
                        if (colon > 0) {
                            val k = line.substring(0, colon).trim().lowercase()
                            val v = line.substring(colon + 1).trim()
                            headers[k] = v
                            if (k == "content-length") contentLength = v.toIntOrNull() ?: 0
                        }
                    }

                    // Read body
                    val body = if (contentLength > 0) {
                        val buf = CharArray(contentLength)
                        reader.read(buf, 0, contentLength)
                        String(buf)
                    } else ""

                    // Route
                    val (status, json) = route(method, path, body)

                    // Write response
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
