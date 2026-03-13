package fi.deeplog.bridge

import android.Manifest
import android.annotation.SuppressLint
import android.bluetooth.*
import android.bluetooth.le.*
import android.content.Context
import android.content.pm.PackageManager
import android.os.*
import android.util.Log
import android.view.*
import android.widget.*
import androidx.activity.result.contract.ActivityResultContracts
import androidx.appcompat.app.AppCompatActivity
import androidx.core.content.ContextCompat
import androidx.recyclerview.widget.LinearLayoutManager
import androidx.recyclerview.widget.RecyclerView
import kotlinx.coroutines.*
import org.json.JSONArray

private const val TAG = "DeepLog"

// ── Simple adapters ───────────────────────────────────────────────────────────

data class FoundDevice(val name: String, val address: String)

class DeviceAdapter(private val onClick: (FoundDevice) -> Unit) :
    RecyclerView.Adapter<DeviceAdapter.VH>() {

    val items = mutableListOf<FoundDevice>()

    inner class VH(val tv: TextView) : RecyclerView.ViewHolder(tv)

    override fun onCreateViewHolder(parent: ViewGroup, type: Int): VH {
        val tv = TextView(parent.context).apply {
            setPadding(16, 14, 16, 14)
            setTextColor(0xFFCCCCCC.toInt())
            textSize = 14f
        }
        return VH(tv)
    }

    override fun getItemCount() = items.size

    override fun onBindViewHolder(h: VH, pos: Int) {
        val d = items[pos]
        h.tv.text = "${d.name}\n${d.address}"
        h.tv.setOnClickListener { onClick(d) }
    }

    @SuppressLint("NotifyDataSetChanged")
    fun addOrUpdate(dev: FoundDevice) {
        if (items.none { it.address == dev.address }) {
            items.add(dev)
            notifyItemInserted(items.size - 1)
        }
    }
}

class DiveAdapter : RecyclerView.Adapter<DiveAdapter.VH>() {
    val items = mutableListOf<String>()
    inner class VH(val tv: TextView) : RecyclerView.ViewHolder(tv)
    override fun onCreateViewHolder(parent: ViewGroup, type: Int): VH {
        val tv = TextView(parent.context).apply {
            setPadding(16, 10, 16, 10)
            setTextColor(0xFFDDDDDD.toInt())
            textSize = 13f
        }
        return VH(tv)
    }
    override fun getItemCount() = items.size
    override fun onBindViewHolder(h: VH, pos: Int) { h.tv.text = items[pos] }
    @SuppressLint("NotifyDataSetChanged")
    fun setAll(list: List<String>) { items.clear(); items.addAll(list); notifyDataSetChanged() }
}

// ── MainActivity ──────────────────────────────────────────────────────────────

class MainActivity : AppCompatActivity() {

    private lateinit var btnScan:     Button
    private lateinit var btnDownload: Button
    private lateinit var progressBar: ProgressBar
    private lateinit var tvStatus:    TextView
    private lateinit var rvDevices:   RecyclerView
    private lateinit var rvDives:     RecyclerView

    private val deviceAdapter = DeviceAdapter { onDeviceSelected(it) }
    private val diveAdapter   = DiveAdapter()

    private var bleScanner: BluetoothLeScanner? = null
    private var selectedDevice: FoundDevice?    = null
    private var bleTransport:   BleTransport?   = null

    private val scope = CoroutineScope(Dispatchers.Main + SupervisorJob())

    // Permission launcher
    private val permLauncher = registerForActivityResult(
        ActivityResultContracts.RequestMultiplePermissions()
    ) { granted ->
        if (granted.values.all { it }) startScan()
        else status("Bluetooth permission denied")
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_main)

        btnScan     = findViewById(R.id.btnScan)
        btnDownload = findViewById(R.id.btnDownload)
        progressBar = findViewById(R.id.progressBar)
        tvStatus    = findViewById(R.id.tvStatus)
        rvDevices   = findViewById(R.id.rvDevices)
        rvDives     = findViewById(R.id.rvDives)

        rvDevices.layoutManager = LinearLayoutManager(this)
        rvDevices.adapter = deviceAdapter

        rvDives.layoutManager = LinearLayoutManager(this)
        rvDives.adapter = diveAdapter

        btnScan.setOnClickListener { requestPermissionsAndScan() }
        btnDownload.setOnClickListener { startDownload() }

        System.loadLibrary("deeplog")
    }

    override fun onDestroy() {
        super.onDestroy()
        scope.cancel()
        bleTransport?.close()
    }

    // ── BLE scan ─────────────────────────────────────────────────────────────

    private fun requestPermissionsAndScan() {
        val needed = mutableListOf<String>()
        if (Build.VERSION.SDK_INT >= 31) {
            listOf(Manifest.permission.BLUETOOTH_SCAN, Manifest.permission.BLUETOOTH_CONNECT)
        } else {
            listOf(Manifest.permission.ACCESS_FINE_LOCATION)
        }.forEach {
            if (ContextCompat.checkSelfPermission(this, it) != PackageManager.PERMISSION_GRANTED)
                needed.add(it)
        }
        if (needed.isEmpty()) startScan() else permLauncher.launch(needed.toTypedArray())
    }

    @SuppressLint("MissingPermission")
    private fun startScan() {
        val adapter = (getSystemService(Context.BLUETOOTH_SERVICE) as BluetoothManager).adapter
        if (adapter == null || !adapter.isEnabled) { status("Bluetooth off"); return }

        bleScanner = adapter.bluetoothLeScanner
        deviceAdapter.items.clear()
        deviceAdapter.notifyDataSetChanged()
        status("Scanning…")
        btnScan.isEnabled = false
        selectedDevice = null
        btnDownload.isEnabled = false

        bleScanner?.startScan(scanCallback)

        scope.launch {
            delay(10_000)
            stopScan()
        }
    }

    @SuppressLint("MissingPermission")
    private fun stopScan() {
        bleScanner?.stopScan(scanCallback)
        bleScanner = null
        btnScan.isEnabled = true
        if (deviceAdapter.items.isEmpty()) status("No devices found")
        else status("Found ${deviceAdapter.items.size} device(s) — tap to select")
    }

    @SuppressLint("MissingPermission")
    private val scanCallback = object : ScanCallback() {
        override fun onScanResult(callbackType: Int, result: ScanResult) {
            val name = result.device.name ?: return
            deviceAdapter.addOrUpdate(FoundDevice(name, result.device.address))
        }
    }

    // ── Device selection ──────────────────────────────────────────────────────

    private fun onDeviceSelected(dev: FoundDevice) {
        selectedDevice = dev
        btnDownload.isEnabled = true
        status("Selected: ${dev.name}")
    }

    // ── Download ──────────────────────────────────────────────────────────────

    @SuppressLint("MissingPermission")
    private fun startDownload() {
        val dev = selectedDevice ?: return
        btnDownload.isEnabled = false
        btnScan.isEnabled     = false
        progressBar.visibility = android.view.View.VISIBLE
        progressBar.progress   = 0
        status("Connecting to ${dev.name}…")

        val adapter = (getSystemService(Context.BLUETOOTH_SERVICE) as BluetoothManager).adapter
        val btDevice = adapter.getRemoteDevice(dev.address)

        val transport = BleTransport(this, btDevice) { msg -> status(msg) }
        bleTransport = transport

        scope.launch(Dispatchers.IO) {
            try {
                transport.connect()
                // DC_TRANSPORT_BLE = 5
                val json = DcBridge.download(dev.name, 5, transport)
                withContext(Dispatchers.Main) { showDives(json) }
            } catch (e: Exception) {
                withContext(Dispatchers.Main) { status("Error: ${e.message}") }
            } finally {
                transport.close()
                withContext(Dispatchers.Main) {
                    progressBar.visibility = android.view.View.GONE
                    btnScan.isEnabled      = true
                    btnDownload.isEnabled  = selectedDevice != null
                }
            }
        }
    }

    private fun showDives(json: String) {
        try {
            val arr = JSONArray(json)
            val list = (0 until arr.length()).map { i ->
                val o = arr.getJSONObject(i)
                "${o.getString("date")} ${o.getString("time")}  " +
                "%.1fm  ${o.getInt("duration")}s".format(o.getDouble("maxdepth"))
            }
            diveAdapter.setAll(list)
            status("Downloaded ${list.size} dive(s)")
        } catch (e: Exception) {
            status("Parse error: $json")
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private fun status(msg: String) {
        Log.i(TAG, msg)
        tvStatus.text = msg
    }

    // Called from C++ via JNI on the download thread.
    companion object {
        @JvmStatic fun onProgress(current: Int, total: Int) {
            instance?.runOnUiThread {
                instance?.progressBar?.let { pb ->
                    pb.max = total
                    pb.progress = current
                }
            }
        }
        @JvmStatic fun onStatus(msg: String) {
            instance?.runOnUiThread { instance?.status(msg) }
        }
        var instance: MainActivity? = null
    }

    override fun onStart()  { super.onStart();  instance = this }
    override fun onStop()   { super.onStop();   instance = null }
}
