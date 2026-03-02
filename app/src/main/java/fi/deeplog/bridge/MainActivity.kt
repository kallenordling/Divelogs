// MainActivity.kt
//
// Minimal UI — one screen with:
//   • Start/Stop bridge button
//   • Status indicator
//   • Username field
//   • "Open DeepLog" button that launches Chrome at the deployed URL
//
// Everything else happens in the browser.

package fi.deeplog.bridge

import android.Manifest
import android.bluetooth.BluetoothAdapter
import android.bluetooth.BluetoothManager
import android.content.*
import android.content.pm.PackageManager
import android.net.Uri
import android.os.*
import android.widget.*
import androidx.activity.result.contract.ActivityResultContracts
import androidx.appcompat.app.AppCompatActivity
import androidx.core.content.ContextCompat
import fi.deeplog.bridge.databinding.ActivityMainBinding

// Replace with your deployed DeepLog URL after GitHub Pages / Netlify deploy
private const val DEEPLOG_URL = "https://kallenordling.github.io/deeplog"

class MainActivity : AppCompatActivity() {

    private lateinit var binding: ActivityMainBinding
    private var bridgeRunning = false
    private val prefs by lazy { getSharedPreferences("deeplog", MODE_PRIVATE) }

    // ── Permission launcher ───────────────────────────────────────────────────

    private val permLauncher = registerForActivityResult(
        ActivityResultContracts.RequestMultiplePermissions()
    ) { results ->
        val denied = results.filterValues { !it }.keys
        if (denied.isEmpty()) startBridge() else {
            Toast.makeText(this,
                "Permissions required: ${denied.joinToString { it.substringAfterLast('.') }}",
                Toast.LENGTH_LONG).show()
        }
    }

    // ── Lifecycle ─────────────────────────────────────────────────────────────

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        binding = ActivityMainBinding.inflate(layoutInflater)
        setContentView(binding.root)

        // Restore username
        binding.usernameEdit.setText(prefs.getString("username", ""))

        binding.startStopBtn.setOnClickListener {
            if (bridgeRunning) stopBridge() else checkPermissionsAndStart()
        }

        binding.openWebBtn.setOnClickListener {
            val url = DEEPLOG_URL.let {
                if (it.contains("YOUR_NAME")) {
                    Toast.makeText(this, "Set your DeepLog URL in MainActivity.kt", Toast.LENGTH_LONG).show()
                    return@setOnClickListener
                }
                it
            }
            startActivity(Intent(Intent.ACTION_VIEW, Uri.parse(url))
                .setPackage("com.android.chrome")
                .also { i ->
                    // Fallback to any browser if Chrome not installed
                    if (packageManager.resolveActivity(i, 0) == null)
                        i.setPackage(null)
                })
        }

        updateUI()
    }

    override fun onResume() {
        super.onResume()
        updateUI()
    }

    // ── Start / stop ──────────────────────────────────────────────────────────

    private fun checkPermissionsAndStart() {
        val needed = buildList {
            if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.S) {
                add(Manifest.permission.BLUETOOTH_SCAN)
                add(Manifest.permission.BLUETOOTH_CONNECT)
            } else {
                add(Manifest.permission.ACCESS_FINE_LOCATION)
            }
        }.filter {
            ContextCompat.checkSelfPermission(this, it) != PackageManager.PERMISSION_GRANTED
        }

        // Also check Bluetooth is on
        val bt = getSystemService(BluetoothManager::class.java).adapter
        if (bt == null || !bt.isEnabled) {
            startActivityForResult(Intent(BluetoothAdapter.ACTION_REQUEST_ENABLE), 1)
            return
        }

        if (needed.isEmpty()) startBridge() else permLauncher.launch(needed.toTypedArray())
    }

    private fun startBridge() {
        // Save username
        val username = binding.usernameEdit.text.toString().trim().ifEmpty { "anonymous" }
        prefs.edit().putString("username", username).apply()

        val intent = Intent(this, BridgeService::class.java)
            .putExtra("username", username)

        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O)
            startForegroundService(intent)
        else
            startService(intent)

        bridgeRunning = true
        updateUI()
        Toast.makeText(this, "Bridge started on :8765", Toast.LENGTH_SHORT).show()
    }

    private fun stopBridge() {
        stopService(Intent(this, BridgeService::class.java))
        bridgeRunning = false
        updateUI()
    }

    // ── UI ────────────────────────────────────────────────────────────────────

    private fun updateUI() {
        if (bridgeRunning) {
            binding.startStopBtn.text = "Stop Bridge"
            binding.startStopBtn.setBackgroundColor(
                ContextCompat.getColor(this, android.R.color.holo_red_dark))
            binding.statusDot.setBackgroundResource(R.drawable.dot_green)
            binding.statusText.text = "Running on localhost:8765"
            binding.openWebBtn.isEnabled = true
        } else {
            binding.startStopBtn.text = "Start Bridge"
            binding.startStopBtn.setBackgroundColor(
                ContextCompat.getColor(this, android.R.color.holo_blue_dark))
            binding.statusDot.setBackgroundResource(R.drawable.dot_grey)
            binding.statusText.text = "Stopped"
            binding.openWebBtn.isEnabled = false
        }
    }
}
