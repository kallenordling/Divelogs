package fi.deeplog.bridge

import android.Manifest
import android.annotation.SuppressLint
import android.bluetooth.BluetoothAdapter
import android.bluetooth.BluetoothManager
import android.content.*
import android.content.pm.PackageManager
import android.net.Uri
import android.os.*
import android.webkit.*
import android.widget.Toast
import androidx.activity.result.contract.ActivityResultContracts
import androidx.appcompat.app.AppCompatActivity
import androidx.core.content.ContextCompat

class MainActivity : AppCompatActivity() {

    private lateinit var webView: WebView

    private val permLauncher = registerForActivityResult(
        ActivityResultContracts.RequestMultiplePermissions()
    ) { results ->
        val denied = results.filterValues { !it }.keys
        if (denied.isEmpty()) startBridge()
        else Toast.makeText(this,
            "Bluetooth permissions required for dive computer download",
            Toast.LENGTH_LONG).show()
    }

    @SuppressLint("SetJavaScriptEnabled")
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        webView = WebView(this)
        setContentView(webView)

        webView.settings.apply {
            javaScriptEnabled                = true
            domStorageEnabled                = true
            allowFileAccessFromFileURLs      = true
            allowUniversalAccessFromFileURLs = true
            databaseEnabled                  = true
            cacheMode                        = WebSettings.LOAD_DEFAULT
        }

        webView.addJavascriptInterface(AndroidBridge(), "AndroidBridge")

        webView.webViewClient = object : WebViewClient() {
            override fun shouldOverrideUrlLoading(
                view: WebView, request: WebResourceRequest
            ): Boolean {
                val url = request.url.toString()
                return if (url.startsWith("file://") || url.startsWith("http://127.0.0.1")) {
                    false
                } else {
                    startActivity(Intent(Intent.ACTION_VIEW, Uri.parse(url)))
                    true
                }
            }
        }

        webView.webChromeClient = object : WebChromeClient() {
            override fun onGeolocationPermissionsShowPrompt(
                origin: String, callback: GeolocationPermissions.Callback
            ) = callback.invoke(origin, true, false)
        }

        webView.loadUrl("file:///android_asset/index.html")
        checkPermissionsAndStart()
    }

    override fun onBackPressed() {
        if (webView.canGoBack()) webView.goBack() else super.onBackPressed()
    }

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

        val bt = getSystemService(BluetoothManager::class.java)?.adapter
        if (bt != null && !bt.isEnabled) {
            @Suppress("DEPRECATION")
            startActivityForResult(Intent(BluetoothAdapter.ACTION_REQUEST_ENABLE), 1)
        }

        if (needed.isEmpty()) startBridge() else permLauncher.launch(needed.toTypedArray())
    }

    private fun startBridge() {
        val intent = Intent(this, BridgeService::class.java)
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O)
            startForegroundService(intent)
        else
            startService(intent)
    }

    inner class AndroidBridge {

        @JavascriptInterface
        fun isBridgeRunning(): Boolean = BridgeService.instance != null

        @JavascriptInterface
        fun getBridgeStatus(): String =
            BridgeService.instance?.getState() ?: """{"status":"idle","message":"","progress":0}"""

        @JavascriptInterface
        fun startDownload(username: String, address: String) {
            BridgeService.instance?.startDownload(address.ifEmpty { null }, username)
        }

        @JavascriptInterface
        fun resetBridge() { BridgeService.instance?.resetState() }

        @JavascriptInterface
        fun getUsername(): String =
            getSharedPreferences("deeplog", MODE_PRIVATE).getString("username", "") ?: ""

        @JavascriptInterface
        fun saveUsername(name: String) {
            getSharedPreferences("deeplog", MODE_PRIVATE).edit()
                .putString("username", name).apply()
        }
    }
}
