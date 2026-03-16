package fi.deeplog.bridge

import android.Manifest
import android.annotation.SuppressLint
import android.app.AlertDialog
import android.app.Dialog
import android.bluetooth.*
import android.bluetooth.le.*
import android.content.Context
import android.content.pm.PackageManager
import android.graphics.*
import android.graphics.drawable.GradientDrawable
import android.location.Location
import android.location.LocationListener
import android.location.LocationManager
import android.os.*
import android.util.Log
import android.view.*
import android.webkit.WebSettings
import android.webkit.WebView
import android.webkit.WebViewClient
import android.widget.*
import androidx.activity.result.contract.ActivityResultContracts
import androidx.appcompat.app.AppCompatActivity
import androidx.core.content.ContextCompat
import androidx.recyclerview.widget.LinearLayoutManager
import androidx.recyclerview.widget.RecyclerView
import kotlinx.coroutines.*
import org.json.JSONArray
import org.json.JSONObject
import kotlin.math.ceil
import kotlin.math.max
import kotlin.math.min

private const val TAG = "DeepLog"
private const val MAP_URL =
    "https://www.google.com/maps/d/viewer?mid=1GoyVpKrxdGMYhXkX5B6fr5ShrnphJhU&ll=64.24142975867096%2C26.092975614026116&z=5"

// ── Data classes ──────────────────────────────────────────────────────────────

data class FoundDevice(val name: String, val address: String)

data class DiveSite(val name: String, val lat: Double, val lon: Double)

data class DiveEntry(
    val number: Int,
    val date: String,
    val time: String,
    val maxdepth: Double,
    val avgdepth: Double,
    val duration: Int,
    val divemode: String,
    val temp_surface: Double?,
    val temp_min: Double?,
    val temp_max: Double?,
    val atmospheric: Double?,
    val salinity_type: Int?,
    val gasmixes: JSONArray,
    val tanks: JSONArray,
    val samples: JSONArray,
    val isNew: Boolean = false
) {
    val label get() = "Dive #$number — $date  ${"%.1f".format(maxdepth)} m"
}

// ── Depth + temperature profile graph ─────────────────────────────────────────

class DiveProfileView(context: Context) : View(context) {

    var samples: JSONArray = JSONArray()
    var maxDepthHint: Double = 0.0

    private val bg      = Paint().apply { style = Paint.Style.FILL }
    private val grid    = Paint().apply { color = 0x22FFFFFF; strokeWidth = 1.5f; style = Paint.Style.STROKE }
    private val lbl     = Paint().apply { color = 0xAAFFFFFF.toInt(); isAntiAlias = true }
    private val dLine   = Paint().apply { color = 0xFF4FC3F7.toInt(); strokeWidth = 3f; style = Paint.Style.STROKE; isAntiAlias = true; strokeJoin = Paint.Join.ROUND }
    private val tLine   = Paint().apply { color = 0xFFFFB74D.toInt(); strokeWidth = 2f; style = Paint.Style.STROKE; isAntiAlias = true; strokeJoin = Paint.Join.ROUND }
    private val dLegend = Paint().apply { color = 0xFF4FC3F7.toInt(); isAntiAlias = true }
    private val tLegend = Paint().apply { color = 0xFFFFB74D.toInt(); isAntiAlias = true }
    private val fill    = Paint().apply { style = Paint.Style.FILL; isAntiAlias = true }

    override fun onDraw(canvas: Canvas) {
        val w = width.toFloat()
        val h = height.toFloat()
        val density = resources.displayMetrics.density
        val padL = 72f * density / 3f
        val padR = 52f * density / 3f
        val padT = 28f; val padB = 38f
        val gw = w - padL - padR; val gh = h - padT - padB

        bg.color = 0xFF0D1B2A.toInt()
        canvas.drawRect(0f, 0f, w, h, bg)

        if (samples.length() < 2) {
            lbl.textSize = 30f; lbl.textAlign = Paint.Align.CENTER
            canvas.drawText("No profile data", w / 2, h / 2, lbl)
            return
        }

        val times  = FloatArray(samples.length())
        val depths = FloatArray(samples.length())
        val temps  = FloatArray(samples.length())
        var maxT = 0f; var maxD = maxDepthHint.toFloat()
        var minTemp = Float.MAX_VALUE; var maxTemp = -Float.MAX_VALUE

        for (i in 0 until samples.length()) {
            val s = samples.getJSONArray(i)
            times[i]  = s.getInt(0) / 1000f
            depths[i] = s.getDouble(1).toFloat()
            temps[i]  = s.getDouble(2).toFloat()
            if (times[i]  > maxT) maxT = times[i]
            if (depths[i] > maxD) maxD = depths[i]
            if (temps[i] != 0f && temps[i] > maxTemp) maxTemp = temps[i]
            if (temps[i] != 0f && temps[i] < minTemp) minTemp = temps[i]
        }
        if (maxD <= 0f) maxD = 1f; if (maxT <= 0f) maxT = 1f
        val hasTempData = maxTemp > minTemp

        val depthStep = when {
            maxD <= 10f -> 2f; maxD <= 20f -> 5f; maxD <= 50f -> 10f
            maxD <= 100f -> 20f; else -> 50f
        }
        val depthCeil = (ceil((maxD / depthStep).toDouble()) * depthStep).toFloat()

        fun tx(t: Float) = padL + (t / maxT) * gw
        fun ty(d: Float) = padT + (d / depthCeil) * gh
        fun tTemp(c: Float) =
            if (!hasTempData) padT + gh / 2
            else padT + (1f - (c - minTemp) / (maxTemp - minTemp)) * gh * 0.75f + gh * 0.05f

        lbl.textSize = 26f * density / 3f; lbl.textAlign = Paint.Align.RIGHT
        var dg = 0f
        while (dg <= depthCeil + 0.1f) {
            val gy = ty(dg)
            canvas.drawLine(padL, gy, padL + gw, gy, grid)
            canvas.drawText("${dg.toInt()}m", padL - 6f, gy + 9f, lbl)
            dg += depthStep
        }

        val path = Path()
        path.moveTo(tx(times[0]), padT + gh)
        for (i in times.indices) path.lineTo(tx(times[i]), ty(depths[i]))
        path.lineTo(tx(times.last()), padT + gh); path.close()
        fill.shader = LinearGradient(0f, padT, 0f, padT + gh,
            0xAA1E88E5.toInt(), 0x330D47A1.toInt(), Shader.TileMode.CLAMP)
        canvas.drawPath(path, fill)

        val dPath = Path()
        dPath.moveTo(tx(times[0]), ty(depths[0]))
        for (i in 1 until times.size) dPath.lineTo(tx(times[i]), ty(depths[i]))
        canvas.drawPath(dPath, dLine)

        if (hasTempData) {
            val tPath = Path(); tPath.moveTo(tx(times[0]), tTemp(temps[0]))
            for (i in 1 until times.size) if (temps[i] != 0f) tPath.lineTo(tx(times[i]), tTemp(temps[i]))
            canvas.drawPath(tPath, tLine)
            lbl.textAlign = Paint.Align.LEFT
            canvas.drawText("${"%.0f".format(maxTemp)}°C", padL + gw + 4f, tTemp(maxTemp) + 9f, lbl)
            if (minTemp < maxTemp - 0.5f)
                canvas.drawText("${"%.0f".format(minTemp)}°C", padL + gw + 4f, tTemp(minTemp) + 9f, lbl)
        }

        lbl.textAlign = Paint.Align.CENTER
        val stepSec = when {
            maxT <= 600 -> 60f; maxT <= 1800 -> 300f
            maxT <= 3600 -> 600f; else -> 1200f
        }
        var ts = 0f
        while (ts <= maxT + 1f) {
            val lx = tx(ts)
            canvas.drawLine(lx, padT, lx, padT + gh, grid)
            canvas.drawText("${(ts / 60).toInt()}m", lx, padT + gh + padB - 6f, lbl)
            ts += stepSec
        }

        dLegend.textSize = 24f * density / 3f; tLegend.textSize = 24f * density / 3f
        canvas.drawText("▬ Depth", padL + 8f, padT + 20f, dLegend)
        if (hasTempData) canvas.drawText("▬ Temp", padL + 140f * density / 3f, padT + 20f, tLegend)
    }
}

// ── Site temperature profile view ─────────────────────────────────────────────
// Y = depth (inverted), X = time into dive, colour = temperature

class SiteProfileView(context: Context) : View(context) {

    var dives: List<SupabaseClient.SiteDive> = emptyList()
        set(v) { field = v; computeBounds(); invalidate() }

    private var maxDepth = 0.0
    private var maxTimeSec = 0
    private var globalMinTemp = Double.MAX_VALUE
    private var globalMaxTemp = -Double.MAX_VALUE

    private val bgPaint  = Paint().apply { style = Paint.Style.FILL }
    private val gridPaint = Paint().apply { color = 0x22FFFFFF; strokeWidth = 1.5f; style = Paint.Style.STROKE }
    private val lblPaint  = Paint().apply { color = 0xAAFFFFFF.toInt(); isAntiAlias = true; textAlign = Paint.Align.RIGHT }
    private val lblCPaint = Paint().apply { color = 0xAAFFFFFF.toInt(); isAntiAlias = true; textAlign = Paint.Align.CENTER }
    private val dotPaint  = Paint().apply { style = Paint.Style.FILL; isAntiAlias = true; strokeCap = Paint.Cap.ROUND }
    private val legendPaint = Paint().apply { style = Paint.Style.FILL }
    private val legendBorder = Paint().apply { style = Paint.Style.STROKE; color = 0x55FFFFFF; strokeWidth = 1f }

    private fun computeBounds() {
        maxDepth = 0.0; maxTimeSec = 0
        globalMinTemp = Double.MAX_VALUE; globalMaxTemp = -Double.MAX_VALUE
        for (d in dives) {
            if (d.maxdepth > maxDepth) maxDepth = d.maxdepth
            for (i in 0 until d.samples.length()) {
                val s = d.samples.getJSONArray(i)
                val t = s.getInt(0) / 1000
                val temp = s.getDouble(2)
                if (t > maxTimeSec) maxTimeSec = t
                if (temp != 0.0) {
                    if (temp < globalMinTemp) globalMinTemp = temp
                    if (temp > globalMaxTemp) globalMaxTemp = temp
                }
            }
        }
        if (globalMinTemp == Double.MAX_VALUE) { globalMinTemp = 0.0; globalMaxTemp = 30.0 }
        if (globalMaxTemp <= globalMinTemp) globalMaxTemp = globalMinTemp + 1.0
    }

    /** Map a temperature value to a colour: cold=blue → warm=red (HSV hue 240→0) */
    private fun tempColor(temp: Double): Int {
        val frac = ((temp - globalMinTemp) / (globalMaxTemp - globalMinTemp)).coerceIn(0.0, 1.0).toFloat()
        val hue  = 240f * (1f - frac)   // 240 = blue, 0 = red
        return Color.HSVToColor(220, floatArrayOf(hue, 1f, 1f))
    }

    override fun onDraw(canvas: Canvas) {
        val w = width.toFloat()
        val h = height.toFloat()
        val density = resources.displayMetrics.density
        val padL = 60f; val padR = 70f; val padT = 24f; val padB = 36f
        val gw = w - padL - padR; val gh = h - padT - padB

        bgPaint.color = 0xFF0D1B2A.toInt()
        canvas.drawRect(0f, 0f, w, h, bgPaint)

        if (dives.isEmpty() || maxDepth == 0.0) {
            lblPaint.textSize = 30f; lblPaint.textAlign = Paint.Align.CENTER
            canvas.drawText("No dives at this site yet", w / 2, h / 2, lblPaint)
            lblPaint.textAlign = Paint.Align.RIGHT
            return
        }

        val depthStep = when {
            maxDepth <= 10 -> 2.0; maxDepth <= 20 -> 5.0; maxDepth <= 50 -> 10.0
            maxDepth <= 100 -> 20.0; else -> 50.0
        }
        val depthCeil = (ceil(maxDepth / depthStep) * depthStep)

        fun tx(sec: Int)    = padL + (sec.toFloat() / maxTimeSec.toFloat()) * gw
        fun ty(depth: Double) = padT + (depth / depthCeil * gh).toFloat()

        lblPaint.textSize = 26f * density / 3f

        // Grid lines (depth)
        var dg = 0.0
        while (dg <= depthCeil + 0.01) {
            val gy = ty(dg)
            canvas.drawLine(padL, gy, padL + gw, gy, gridPaint)
            canvas.drawText("${dg.toInt()}m", padL - 6f, gy + 9f, lblPaint)
            dg += depthStep
        }

        // Draw each dive's samples as coloured dots
        val dotR = max(2.5f, min(5f, gw / (maxTimeSec.toFloat() / 10f + 1f)))
        for (dive in dives) {
            val n = dive.samples.length()
            for (i in 0 until n) {
                val s    = dive.samples.getJSONArray(i)
                val timeSec  = s.getInt(0) / 1000
                val depth    = s.getDouble(1)
                val temp     = s.getDouble(2)
                val cx = tx(timeSec)
                val cy = ty(depth)
                dotPaint.color = if (temp != 0.0) tempColor(temp) else 0x88FFFFFF.toInt()
                canvas.drawCircle(cx, cy, dotR, dotPaint)
            }
        }

        // Time axis labels
        lblCPaint.textSize = 24f * density / 3f
        val stepSec = when { maxTimeSec <= 600 -> 60; maxTimeSec <= 1800 -> 300; else -> 600 }
        var ts = 0
        while (ts <= maxTimeSec) {
            val lx = tx(ts)
            canvas.drawLine(lx, padT, lx, padT + gh, gridPaint)
            canvas.drawText("${ts / 60}m", lx, padT + gh + padB - 6f, lblCPaint)
            ts += stepSec
        }

        // Colour legend bar (right side)
        val lx0 = padL + gw + 10f; val lw = 14f; val lh = gh * 0.8f; val ly0 = padT + gh * 0.1f
        val steps = 60
        for (i in 0 until steps) {
            val frac = i.toFloat() / steps
            val temp = globalMinTemp + frac * (globalMaxTemp - globalMinTemp)
            legendPaint.color = tempColor(temp)
            val y0 = ly0 + (1f - frac) * lh
            val y1 = ly0 + (1f - (i + 1f) / steps) * lh
            canvas.drawRect(lx0, min(y0, y1), lx0 + lw, max(y0, y1), legendPaint)
        }
        canvas.drawRect(lx0, ly0, lx0 + lw, ly0 + lh, legendBorder)
        val lblR = Paint().apply { color = 0xCCFFFFFF.toInt(); isAntiAlias = true; textAlign = Paint.Align.LEFT; textSize = 22f * density / 3f }
        canvas.drawText("${"%.0f".format(globalMaxTemp)}°C", lx0 + lw + 4f, ly0 + 12f, lblR)
        canvas.drawText("${"%.0f".format(globalMinTemp)}°C", lx0 + lw + 4f, ly0 + lh, lblR)
    }
}

// ── Adapters ──────────────────────────────────────────────────────────────────

class DeviceAdapter(private val onClick: (FoundDevice) -> Unit) :
    RecyclerView.Adapter<DeviceAdapter.VH>() {

    val items = mutableListOf<FoundDevice>()
    var selectedAddress: String? = null

    inner class VH(
        val card: LinearLayout,
        val tvName: TextView,
        val tvAddr: TextView,
        val tvCheck: TextView
    ) : RecyclerView.ViewHolder(card) {
        val bg: GradientDrawable = card.background as GradientDrawable
    }

    override fun onCreateViewHolder(parent: ViewGroup, type: Int): VH {
        val ctx = parent.context
        val card = LinearLayout(ctx).apply {
            orientation = LinearLayout.HORIZONTAL; setPadding(32, 20, 24, 20)
            gravity = android.view.Gravity.CENTER_VERTICAL
            background = GradientDrawable().apply {
                shape = GradientDrawable.RECTANGLE; cornerRadius = 16f; setColor(0xFF1A2840.toInt())
            }
            layoutParams = RecyclerView.LayoutParams(
                RecyclerView.LayoutParams.MATCH_PARENT, RecyclerView.LayoutParams.WRAP_CONTENT
            ).also { it.setMargins(16, 8, 16, 4) }
        }
        val textCol = LinearLayout(ctx).apply {
            orientation = LinearLayout.VERTICAL
            layoutParams = LinearLayout.LayoutParams(0, LinearLayout.LayoutParams.WRAP_CONTENT, 1f)
        }
        textCol.addView(TextView(ctx).apply {
            textSize = 15f; setTextColor(0xFFE8E8E8.toInt()); setTypeface(typeface, Typeface.BOLD)
        })
        textCol.addView(TextView(ctx).apply { textSize = 12f; setTextColor(0xFF7EA8C8.toInt()) })
        card.addView(textCol)
        card.addView(TextView(ctx).apply {
            text = "✓"; textSize = 18f; setTextColor(0xFF4FC3F7.toInt())
            visibility = android.view.View.INVISIBLE
        })
        return VH(card, textCol.getChildAt(0) as TextView, textCol.getChildAt(1) as TextView,
                  card.getChildAt(1) as TextView)
    }

    override fun getItemCount() = items.size
    override fun onBindViewHolder(h: VH, pos: Int) {
        val d = items[pos]
        h.tvName.text = d.name; h.tvAddr.text = d.address
        val selected = d.address == selectedAddress
        h.bg.setColor(if (selected) 0xFF1A3A5A.toInt() else 0xFF1A2840.toInt())
        h.bg.setStroke(if (selected) 2 else 0, 0xFF4FC3F7.toInt())
        h.tvCheck.visibility = if (selected) android.view.View.VISIBLE else android.view.View.INVISIBLE
        h.card.setOnClickListener { onClick(d) }
    }

    fun addOrUpdate(dev: FoundDevice) {
        if (items.none { it.address == dev.address }) { items.add(dev); notifyItemInserted(items.size - 1) }
    }

    fun setSelected(address: String?) {
        selectedAddress = address
        notifyDataSetChanged()
    }
}

class DiveAdapter(private val onClick: (DiveEntry) -> Unit) :
    RecyclerView.Adapter<DiveAdapter.VH>() {

    val allItems = mutableListOf<DiveEntry>()
    val items = mutableListOf<DiveEntry>()  // filtered view

    inner class VH(val card: LinearLayout) : RecyclerView.ViewHolder(card) {
        val tvNum:   TextView = card.getChildAt(0) as TextView
        val tvDate:  TextView = (card.getChildAt(1) as LinearLayout).getChildAt(0) as TextView
        val tvStats: TextView = (card.getChildAt(1) as LinearLayout).getChildAt(1) as TextView
        val tvNew:   TextView = card.getChildAt(2) as TextView
        val bg: GradientDrawable = card.background as GradientDrawable
    }

    override fun onCreateViewHolder(parent: ViewGroup, type: Int): VH {
        val ctx = parent.context
        val card = LinearLayout(ctx).apply {
            orientation = LinearLayout.HORIZONTAL; setPadding(24, 18, 16, 18)
            gravity = Gravity.CENTER_VERTICAL
            background = GradientDrawable().apply {
                shape = GradientDrawable.RECTANGLE; cornerRadius = 16f; setColor(0xFF162030.toInt())
            }
            layoutParams = RecyclerView.LayoutParams(
                RecyclerView.LayoutParams.MATCH_PARENT, RecyclerView.LayoutParams.WRAP_CONTENT
            ).also { it.setMargins(16, 6, 16, 6) }
        }
        val tvNum = TextView(ctx).apply {
            textSize = 28f; setTextColor(0xFF4FC3F7.toInt()); setTypeface(typeface, Typeface.BOLD)
            setPadding(0, 0, 28, 0); gravity = Gravity.CENTER_VERTICAL
        }
        val right = LinearLayout(ctx).apply {
            orientation = LinearLayout.VERTICAL
            layoutParams = LinearLayout.LayoutParams(0, LinearLayout.LayoutParams.WRAP_CONTENT, 1f)
        }
        right.addView(TextView(ctx).apply {
            textSize = 14f; setTextColor(0xFFE0E0E0.toInt()); setTypeface(typeface, Typeface.BOLD)
        })
        right.addView(TextView(ctx).apply {
            textSize = 12f; setTextColor(0xFF80B8D8.toInt()); setPadding(0, 4, 0, 0)
        })
        val tvNew = TextView(ctx).apply {
            text = "NEW"; textSize = 10f; setTypeface(typeface, Typeface.BOLD)
            setTextColor(0xFF0D1820.toInt())
            setPadding(10, 6, 10, 6)
            background = GradientDrawable().apply {
                shape = GradientDrawable.RECTANGLE; cornerRadius = 20f; setColor(0xFF4FC3F7.toInt())
            }
            visibility = View.INVISIBLE
        }
        card.addView(tvNum); card.addView(right); card.addView(tvNew)
        return VH(card)
    }

    override fun getItemCount() = items.size
    override fun onBindViewHolder(h: VH, pos: Int) {
        val d = items[pos]; h.tvNum.text = "#${d.number}"
        h.tvDate.text = "${d.date}  ${d.time}"
        val mins = d.duration / 60; val secs = d.duration % 60
        val tempStr = d.temp_min?.let { "  ·  ${"%.1f".format(it)} °C" } ?: ""
        val modeStr = if (d.divemode != "OC") "  ·  ${d.divemode}" else ""
        h.tvStats.text = "${"%.1f".format(d.maxdepth)} m  ·  ${mins}m ${secs}s$tempStr$modeStr"
        h.tvNew.visibility = if (d.isNew) View.VISIBLE else View.INVISIBLE
        h.bg.setColor(if (d.isNew) 0xFF0E2535.toInt() else 0xFF162030.toInt())
        h.bg.setStroke(if (d.isNew) 2 else 0, 0xFF4FC3F7.toInt())
        h.card.setOnClickListener { onClick(d) }
    }

    fun add(d: DiveEntry) {
        allItems.add(0, d)
        items.add(0, d)
        notifyItemInserted(0)
    }

    @SuppressLint("NotifyDataSetChanged")
    fun filter(query: String) {
        items.clear()
        val q = query.trim().lowercase()
        items.addAll(if (q.isEmpty()) allItems else allItems.filter {
            it.date.contains(q) || it.divemode.lowercase().contains(q) ||
            "${"%.1f".format(it.maxdepth)}".contains(q)
        })
        notifyDataSetChanged()
    }

    @SuppressLint("NotifyDataSetChanged")
    fun setAll(dives: List<DiveEntry>) {
        allItems.clear(); allItems.addAll(dives.reversed())  // reversed → newest at index 0
        items.clear(); items.addAll(allItems)
        notifyDataSetChanged()
    }

    @SuppressLint("NotifyDataSetChanged")
    fun clear() { allItems.clear(); items.clear(); notifyDataSetChanged() }
}

// ── SiteAdapter ───────────────────────────────────────────────────────────────

class SiteAdapter(
    private val sites: MutableList<DiveSite>,
    private val onSelect: (DiveSite) -> Unit,
    private val onDelete: (Int) -> Unit
) : RecyclerView.Adapter<SiteAdapter.VH>() {

    inner class VH(
        val card: LinearLayout,
        val tvName: TextView,
        val tvCoord: TextView,
        val btnDel: TextView
    ) : RecyclerView.ViewHolder(card)

    override fun onCreateViewHolder(parent: ViewGroup, type: Int): VH {
        val ctx = parent.context
        val tvName  = TextView(ctx).apply { textSize = 14f; setTextColor(0xFFE0E0E0.toInt()); setTypeface(typeface, Typeface.BOLD) }
        val tvCoord = TextView(ctx).apply { textSize = 11f; setTextColor(0xFF6090A8.toInt()) }
        val btnDel  = TextView(ctx).apply {
            text = "✕"; textSize = 18f; setTextColor(0xFF666666.toInt())
            setPadding(20, 8, 8, 8); gravity = Gravity.CENTER_VERTICAL
        }
        val inner = LinearLayout(ctx).apply {
            orientation = LinearLayout.VERTICAL
            layoutParams = LinearLayout.LayoutParams(0, LinearLayout.LayoutParams.WRAP_CONTENT, 1f)
            addView(tvName); addView(tvCoord)
        }
        val card = LinearLayout(ctx).apply {
            orientation = LinearLayout.HORIZONTAL; setPadding(24, 16, 16, 16)
            background = GradientDrawable().apply {
                shape = GradientDrawable.RECTANGLE; cornerRadius = 12f; setColor(0xFF1A2840.toInt())
            }
            layoutParams = RecyclerView.LayoutParams(
                RecyclerView.LayoutParams.MATCH_PARENT, RecyclerView.LayoutParams.WRAP_CONTENT
            ).also { it.setMargins(0, 4, 0, 4) }
            addView(inner); addView(btnDel)
        }
        return VH(card, tvName, tvCoord, btnDel)
    }

    override fun getItemCount() = sites.size
    override fun onBindViewHolder(h: VH, pos: Int) {
        val s = sites[pos]
        h.tvName.text  = s.name
        h.tvCoord.text = "${"%.5f".format(s.lat)}°N, ${"%.5f".format(s.lon)}°E"
        h.card.setOnClickListener { onSelect(s) }
        h.btnDel.setOnClickListener { onDelete(pos) }
    }
}

// ── MainActivity ──────────────────────────────────────────────────────────────

class MainActivity : AppCompatActivity() {

    private lateinit var btnScan:          Button
    private lateinit var btnDownload:      Button
    private lateinit var btnSites:         Button
    private lateinit var progressBar:      ProgressBar
    private lateinit var tvStatus:         TextView
    private lateinit var rvDevices:        RecyclerView
    private lateinit var rvDives:          RecyclerView
    private lateinit var etSearch:         android.widget.EditText
    private lateinit var tabDownload:      TextView
    private lateinit var tabDivelog:       TextView
    private lateinit var tabStats:         TextView
    private lateinit var tabIndicator:     View
    private lateinit var contentDownload:  LinearLayout
    private lateinit var contentDivelog:   LinearLayout
    private lateinit var contentStats:     ScrollView
    private lateinit var statsContainer:   LinearLayout

    private val deviceAdapter = DeviceAdapter { onDeviceSelected(it) }
    private val diveAdapter   = DiveAdapter   { showDiveDetail(it) }

    private var bleScanner:     BluetoothLeScanner? = null
    private var selectedDevice: FoundDevice?        = null
    private var bleTransport:   BleTransport?       = null

    // Dive site associations: key = "date_time", value = site name
    private val diveToSite = HashMap<String, String>()

    private val scope = CoroutineScope(Dispatchers.Main + SupervisorJob())

    private val permLauncher = registerForActivityResult(
        ActivityResultContracts.RequestMultiplePermissions()
    ) { granted ->
        if (granted.values.all { it }) startScan() else status("Bluetooth permission denied")
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_main)

        btnScan        = findViewById(R.id.btnScan)
        btnDownload    = findViewById(R.id.btnDownload)
        btnSites       = findViewById(R.id.btnSites)
        progressBar    = findViewById(R.id.progressBar)
        tvStatus       = findViewById(R.id.tvStatus)
        rvDevices      = findViewById(R.id.rvDevices)
        rvDives        = findViewById(R.id.rvDives)
        etSearch       = findViewById(R.id.etSearch)
        tabDownload    = findViewById(R.id.tabDownload)
        tabDivelog     = findViewById(R.id.tabDivelog)
        tabStats       = findViewById(R.id.tabStats)
        tabIndicator   = findViewById(R.id.tabIndicator)
        contentDownload = findViewById(R.id.contentDownload)
        contentDivelog  = findViewById(R.id.contentDivelog)
        contentStats    = findViewById(R.id.contentStats)
        statsContainer  = findViewById(R.id.statsContainer)

        rvDevices.layoutManager = LinearLayoutManager(this)
        rvDevices.adapter = deviceAdapter
        rvDives.layoutManager = LinearLayoutManager(this)
        rvDives.adapter = diveAdapter

        btnScan.setOnClickListener { requestPermissionsAndScan() }
        btnDownload.setOnClickListener { startDownload() }
        btnSites.setOnClickListener { showSitesDialog() }

        tabDownload.setOnClickListener { showTab(0) }
        tabDivelog.setOnClickListener  { showTab(1) }
        tabStats.setOnClickListener    { showTab(2) }
        showTab(0)

        etSearch.addTextChangedListener(object : android.text.TextWatcher {
            override fun beforeTextChanged(s: CharSequence?, st: Int, c: Int, a: Int) {}
            override fun onTextChanged(s: CharSequence?, st: Int, b: Int, c: Int) {}
            override fun afterTextChanged(s: android.text.Editable?) {
                diveAdapter.filter(s?.toString() ?: "")
            }
        })

        loadDiveSiteAssociations()
        System.loadLibrary("deeplog")
        restoreSupabaseSession()

        scope.launch {
            val loggedIn = when {
                !SupabaseClient.isLoggedIn -> false
                SupabaseClient.isTokenExpired() -> {
                    val ok = SupabaseClient.refreshSession()
                    if (ok) persistSupabaseSession() else SupabaseClient.clearSession()
                    ok
                }
                else -> true
            }
            withContext(Dispatchers.Main) {
                if (loggedIn) loadDivesFromCloud()
                else showLoginDialog()
            }
        }
    }

    private suspend fun loadDivesFromCloud() {
        withContext(Dispatchers.Main) { status("Loading dives from cloud…") }
        val rows = SupabaseClient.fetchMyDives()
        withContext(Dispatchers.Main) {
            if (rows.isEmpty()) { status("No dives yet — connect a dive computer to download."); return@withContext }
            val dives = rows.mapIndexedNotNull { i, row -> rowToDiveEntry(row, i + 1) }
            diveAdapter.setAll(dives)
            diveCounter = dives.size
            existingDiveKeys.clear()
            // Only mark dives as existing if they parsed successfully — unparseable rows
            // must NOT block re-downloading (they won't show in the UI either way).
            for (dive in dives) {
                val key = "${dive.date}_${dive.time}"
                existingDiveKeys.add(key)
                val siteName = diveToSite[key] ?: continue
                diveToSite[key] = siteName
            }
            for (row in rows) {
                val siteName = row.optString("site_name", "").takeIf { it.isNotEmpty() } ?: continue
                val key = "${row.optString("date")}_${row.optString("time")}"
                diveToSite[key] = siteName
            }
            status("Loaded ${dives.size} dive(s) from cloud.")
        }
    }

    private fun rowToDiveEntry(row: JSONObject, number: Int): DiveEntry? = try {
        DiveEntry(
            number        = number,
            date          = row.getString("date"),
            time          = row.getString("time"),
            maxdepth      = row.getDouble("maxdepth"),
            avgdepth      = row.optDouble("avgdepth", 0.0),
            duration      = row.getInt("duration"),
            divemode      = row.optString("divemode", "OC"),
            temp_surface  = row.optDouble("temp_surface", Double.NaN).takeIf { !it.isNaN() },
            temp_min      = row.optDouble("temp_min",     Double.NaN).takeIf { !it.isNaN() },
            temp_max      = row.optDouble("temp_max",     Double.NaN).takeIf { !it.isNaN() },
            atmospheric   = row.optDouble("atmospheric",  Double.NaN).takeIf { !it.isNaN() },
            salinity_type = row.optInt("salinity_type", -1).takeIf { it >= 0 },
            gasmixes      = row.optJSONArray("gasmixes") ?: JSONArray(),
            tanks         = row.optJSONArray("tanks")    ?: JSONArray(),
            samples       = row.optJSONArray("samples")  ?: JSONArray()
        )
    } catch (e: Exception) { Log.e(TAG, "rowToDiveEntry: $e"); null }

    // ── Supabase session persistence ──────────────────────────────────────────

    private fun restoreSupabaseSession() {
        val prefs = getSharedPreferences("supabase", MODE_PRIVATE)
        SupabaseClient.accessToken  = prefs.getString("access_token",  null)
        SupabaseClient.refreshToken = prefs.getString("refresh_token", null)
        SupabaseClient.userId       = prefs.getString("user_id",       null)
    }

    private fun persistSupabaseSession() {
        getSharedPreferences("supabase", MODE_PRIVATE).edit()
            .putString("access_token",  SupabaseClient.accessToken)
            .putString("refresh_token", SupabaseClient.refreshToken)
            .putString("user_id",       SupabaseClient.userId)
            .apply()
    }

    // ── Login dialog ──────────────────────────────────────────────────────────

    private fun showLoginDialog() {
        val dp = resources.displayMetrics.density

        val layout = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            setPadding((20 * dp).toInt(), (16 * dp).toInt(), (20 * dp).toInt(), (8 * dp).toInt())
        }

        fun editText(hint: String, password: Boolean) = android.widget.EditText(this).apply {
            this.hint = hint
            textSize = 15f; setTextColor(0xFFEEEEEE.toInt()); setHintTextColor(0xFF666666.toInt())
            setBackgroundColor(0xFF1A2840.toInt()); setPadding(16, 12, 16, 12)
            if (password) inputType = android.text.InputType.TYPE_CLASS_TEXT or android.text.InputType.TYPE_TEXT_VARIATION_PASSWORD
            layoutParams = LinearLayout.LayoutParams(LinearLayout.LayoutParams.MATCH_PARENT,
                LinearLayout.LayoutParams.WRAP_CONTENT).also { it.bottomMargin = (10 * dp).toInt() }
        }

        val etEmail = editText("Email", false)
        val etPass  = editText("Password", true)
        val tvMsg   = TextView(this).apply {
            textSize = 12f; setTextColor(0xFFFF6B6B.toInt()); visibility = View.GONE
        }
        layout.addView(etEmail); layout.addView(etPass); layout.addView(tvMsg)

        val dialog = AlertDialog.Builder(this)
            .setTitle("Sign in to DeepLog")
            .setView(layout)
            .setCancelable(false)
            .setPositiveButton("Sign In", null)
            .setNeutralButton("Sign Up", null)
            .create()

        dialog.show()

        fun attempt(signUp: Boolean) {
            val email = etEmail.text.toString().trim()
            val pass  = etPass.text.toString()
            if (email.isEmpty() || pass.isEmpty()) { tvMsg.text = "Enter email and password"; tvMsg.visibility = View.VISIBLE; return }
            tvMsg.text = if (signUp) "Creating account…" else "Signing in…"
            tvMsg.setTextColor(0xFF80CCFF.toInt()); tvMsg.visibility = View.VISIBLE

            scope.launch {
                val result = if (signUp) SupabaseClient.signUp(email, pass) else SupabaseClient.signIn(email, pass)
                withContext(Dispatchers.Main) {
                    if (result.isSuccess) {
                        persistSupabaseSession()
                        dialog.dismiss()
                        status("Signed in as $email")
                        loadDivesFromCloud()
                    } else {
                        tvMsg.setTextColor(0xFFFF6B6B.toInt())
                        tvMsg.text = result.exceptionOrNull()?.message?.take(120) ?: "Error"
                    }
                }
            }
        }

        dialog.getButton(AlertDialog.BUTTON_POSITIVE).setOnClickListener { attempt(false) }
        dialog.getButton(AlertDialog.BUTTON_NEUTRAL) .setOnClickListener { attempt(true)  }
    }

    private fun showTab(tab: Int) {
        contentDownload.visibility = if (tab == 0) View.VISIBLE else View.GONE
        contentDivelog.visibility  = if (tab == 1) View.VISIBLE else View.GONE
        contentStats.visibility    = if (tab == 2) View.VISIBLE else View.GONE

        val active   = 0xFF4FC3F7.toInt()
        val inactive = 0xFF7A9AAA.toInt()
        tabDownload.setTextColor(if (tab == 0) active else inactive)
        tabDivelog .setTextColor(if (tab == 1) active else inactive)
        tabStats   .setTextColor(if (tab == 2) active else inactive)

        // Slide indicator under active tab
        tabIndicator.post {
            val parent = tabDownload.parent as? LinearLayout ?: return@post
            val total = parent.width.toFloat()
            val w = (total / 3).toInt()
            val params = tabIndicator.layoutParams as FrameLayout.LayoutParams
            params.width       = w
            params.marginStart = tab * w
            tabIndicator.layoutParams = params
        }

        if (tab == 2) refreshStats()
    }

    private fun refreshStats() {
        statsContainer.removeAllViews()
        val dp = resources.displayMetrics.density
        val dives = diveAdapter.allItems
        if (dives.isEmpty()) {
            statsContainer.addView(TextView(this).apply {
                text = "No dives downloaded yet.\nGo to Download tab to sync your dive computer."
                textSize = 14f; setTextColor(0xFF888888.toInt())
                gravity = Gravity.CENTER; setPadding(0, (40 * dp).toInt(), 0, 0)
            })
            return
        }

        val totalSec   = dives.sumOf { it.duration }
        val maxDepth   = dives.maxOf { it.maxdepth }
        val avgDepth   = dives.map { it.maxdepth }.average()
        val longestSec = dives.maxOf { it.duration }
        val avgSec     = dives.map { it.duration }.average().toInt()
        val locations  = diveToSite.values.toSet().size

        fun mins(s: Int): String { val m = s / 60; val sec = s % 60; return "${m}m ${sec}s" }
        fun totalTime(s: Int): String {
            val h = s / 3600; val m = (s % 3600) / 60
            return if (h > 0) "${h}h ${m}m" else "${m}m"
        }

        fun statCard(label: String, value: String, sub: String = "") {
            val card = LinearLayout(this).apply {
                orientation = LinearLayout.VERTICAL
                background = GradientDrawable().apply {
                    shape = GradientDrawable.RECTANGLE; cornerRadius = 16f
                    setColor(0xFF162030.toInt())
                }
                setPadding((20 * dp).toInt(), (16 * dp).toInt(), (20 * dp).toInt(), (16 * dp).toInt())
                layoutParams = LinearLayout.LayoutParams(
                    LinearLayout.LayoutParams.MATCH_PARENT,
                    LinearLayout.LayoutParams.WRAP_CONTENT
                ).also { it.bottomMargin = (10 * dp).toInt() }
            }
            card.addView(TextView(this).apply {
                text = label; textSize = 12f; setTextColor(0xFF7AB8D8.toInt())
            })
            card.addView(TextView(this).apply {
                text = value; textSize = 28f; setTextColor(0xFF4FC3F7.toInt())
                setTypeface(typeface, Typeface.BOLD)
            })
            if (sub.isNotEmpty()) card.addView(TextView(this).apply {
                text = sub; textSize = 12f; setTextColor(0xFF556677.toInt())
            })
            statsContainer.addView(card)
        }

        statsContainer.addView(TextView(this).apply {
            text = "Your Dive Stats"; textSize = 20f; setTextColor(0xFFFFFFFF.toInt())
            setTypeface(typeface, Typeface.BOLD)
            layoutParams = LinearLayout.LayoutParams(
                LinearLayout.LayoutParams.MATCH_PARENT, LinearLayout.LayoutParams.WRAP_CONTENT
            ).also { it.bottomMargin = (18 * dp).toInt() }
        })

        statCard("TOTAL DIVES",         "${dives.size}")
        statCard("TOTAL TIME UNDERWATER", totalTime(totalSec))
        statCard("MAX DEPTH",           "${"%.1f".format(maxDepth)} m")
        statCard("AVERAGE MAX DEPTH",   "${"%.1f".format(avgDepth)} m")
        statCard("LONGEST DIVE",        mins(longestSec))
        statCard("AVERAGE DIVE TIME",   mins(avgSec))
        if (locations > 0)
            statCard("DIVE SITES VISITED",  "$locations", "Based on assigned site tags")
    }

    override fun onDestroy() { super.onDestroy(); scope.cancel(); bleTransport?.close() }

    // ── BLE scan ──────────────────────────────────────────────────────────────

    private fun requestPermissionsAndScan() {
        val needed = if (Build.VERSION.SDK_INT >= 31)
            listOf(Manifest.permission.BLUETOOTH_SCAN, Manifest.permission.BLUETOOTH_CONNECT)
        else listOf(Manifest.permission.ACCESS_FINE_LOCATION)
        val missing = needed.filter {
            ContextCompat.checkSelfPermission(this, it) != PackageManager.PERMISSION_GRANTED
        }
        if (missing.isEmpty()) startScan() else permLauncher.launch(missing.toTypedArray())
    }

    @SuppressLint("MissingPermission")
    private fun startScan() {
        val adapter = (getSystemService(Context.BLUETOOTH_SERVICE) as BluetoothManager).adapter
        if (adapter == null || !adapter.isEnabled) { status("Bluetooth off"); return }
        bleScanner = adapter.bluetoothLeScanner
        deviceAdapter.items.clear(); deviceAdapter.setSelected(null)
        status("Scanning for dive computers…")
        btnScan.isEnabled = false; selectedDevice = null; btnDownload.isEnabled = false
        bleScanner?.startScan(scanCallback)
        scope.launch { delay(10_000); stopScan() }
    }

    @SuppressLint("MissingPermission")
    private fun stopScan() {
        bleScanner?.stopScan(scanCallback); bleScanner = null; btnScan.isEnabled = true
        status(if (deviceAdapter.items.isEmpty()) "No devices found"
               else "Found ${deviceAdapter.items.size} device(s) — tap to select")
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
        selectedDevice = dev; btnDownload.isEnabled = true
        deviceAdapter.setSelected(dev.address)
        status("Selected: ${dev.name} — tap Download")
    }

    // ── Download ──────────────────────────────────────────────────────────────

    @SuppressLint("MissingPermission")
    private fun startDownload() {
        val dev = selectedDevice ?: return
        btnDownload.isEnabled = false; btnScan.isEnabled = false
        progressBar.visibility = View.VISIBLE; progressBar.progress = 0
        newDivesFound = 0; skippedDives = 0; totalDeviceDives = 0
        status("Connecting to ${dev.name}…")

        val btAdapter = (getSystemService(Context.BLUETOOTH_SERVICE) as BluetoothManager).adapter
        val transport = BleTransport(this, btAdapter.getRemoteDevice(dev.address)) { msg ->
            runOnUiThread { status(msg) }
        }
        bleTransport = transport

        // Use fingerprint only when previous download was gap-free (hasGaps defaults true).
        val fpPrefs = getSharedPreferences("fingerprints", MODE_PRIVATE)
        val hasGaps = fpPrefs.getBoolean("gaps_${dev.address}", true)
        val fingerprint = if (hasGaps) null else {
            fpPrefs.getString("fp_${dev.address}", null)
                ?.takeIf { it.length % 2 == 0 }
                ?.let { hex -> ByteArray(hex.length / 2) { i -> hex.substring(i * 2, i * 2 + 2).toInt(16).toByte() } }
        }
        if (hasGaps) Log.i(TAG, "Full sync: gaps flag set for ${dev.address}")
        pendingFingerprintAddress = dev.address

        scope.launch(Dispatchers.IO) {
            try {
                transport.connect()
                DcBridge.download(dev.name, 32, transport, fingerprint)
            } catch (e: Exception) {
                withContext(Dispatchers.Main) { status("Error: ${e.message}") }
            } finally {
                transport.close()
                withContext(Dispatchers.Main) {
                    progressBar.visibility = View.GONE
                    btnScan.isEnabled = true; btnDownload.isEnabled = selectedDevice != null
                    // Save gap status: if we skipped dives this run, gaps remain and next sync must be full.
                    fpPrefs.edit().putBoolean("gaps_${dev.address}", skippedDives > 0).apply()
                    val summary = "$totalDeviceDives dive(s) on device"
                    if (newDivesFound == 0) {
                        status("$summary — all already in log, nothing new.")
                        return@withContext
                    }
                    resortAndRenumber()
                    status("$summary — $newDivesFound new, $skippedDives already in log. Uploading…")
                    if (SupabaseClient.isLoggedIn) uploadNewDives(dev.name)
                    else status("$newDivesFound new dive(s) found. Sign in to sync to cloud.")
                }
            }
        }
    }

    private fun resortAndRenumber() {
        // Sort oldest→newest, assign dive numbers 1..N, display newest first
        val sorted = diveAdapter.allItems
            .sortedWith(compareBy({ it.date }, { it.time }))
            .mapIndexed { i, d -> d.copy(number = i + 1) }
        diveCounter = sorted.size
        diveAdapter.setAll(sorted)   // setAll reverses → newest at top
    }

    private fun uploadNewDives(deviceName: String) {
        scope.launch {
            var ok = 0; var fail = 0
            for (dive in diveAdapter.allItems.filter { it.isNew }) {
                val siteName = getDiveSite(dive)
                val site = if (siteName != null) loadSites().find { it.name == siteName } else null
                val r = SupabaseClient.uploadDive(dive, deviceName, siteName, site?.lat, site?.lon)
                if (r.isSuccess) { ok++; existingDiveKeys.add("${dive.date}_${dive.time}") }
                else fail++
            }
            withContext(Dispatchers.Main) {
                val msg = buildString {
                    append("Sync done: $ok uploaded")
                    if (fail > 0) append(", $fail failed")
                }
                status(msg)
            }
        }
    }

    // ── Google My Maps KML fetch ──────────────────────────────────────────────

    private val KML_URL =
        "https://www.google.com/maps/d/kml?mid=1GoyVpKrxdGMYhXkX5B6fr5ShrnphJhU"

    private suspend fun fetchGoogleMapSites(): List<DiveSite> = withContext(Dispatchers.IO) {
        try {
            val conn = java.net.URL(KML_URL).openConnection() as java.net.HttpURLConnection
            conn.connectTimeout = 10_000; conn.readTimeout = 20_000
            conn.setRequestProperty("User-Agent", "Mozilla/5.0")
            // Response is KMZ (ZIP containing doc.kml)
            val bytes = conn.inputStream.readBytes()
            conn.disconnect()
            val kml = extractKmlFromKmz(bytes)
                ?: return@withContext emptyList<DiveSite>().also { Log.e(TAG, "No KML in KMZ") }
            parseKml(kml)
        } catch (e: Exception) {
            Log.e(TAG, "KML fetch failed: $e"); emptyList()
        }
    }

    private fun extractKmlFromKmz(bytes: ByteArray): String? {
        val zis = java.util.zip.ZipInputStream(bytes.inputStream())
        var entry = zis.nextEntry
        while (entry != null) {
            if (entry.name.endsWith(".kml")) return zis.bufferedReader(Charsets.UTF_8).readText()
            entry = zis.nextEntry
        }
        return null
    }

    // Folders to skip (clubs and accommodation); all others are dive sites
    private val SKIP_FOLDERS = setOf("Sukellusseurat", "Majoitus")

    private fun parseKml(xml: String): List<DiveSite> {
        val sites = mutableListOf<DiveSite>()
        try {
            val folderRe    = Regex("""<Folder>(.*?)</Folder>""", setOf(RegexOption.DOT_MATCHES_ALL))
            val nameRe      = Regex("""<name>(.*?)</name>""")
            val placemarkRe = Regex("""<Placemark>(.*?)</Placemark>""", setOf(RegexOption.DOT_MATCHES_ALL))
            // Dive sites use <coordinates>lon,lat,alt</coordinates>; clubs use "Osoite: lat, lon"
            val coordsRe = Regex("""<coordinates>\s*([0-9.-]+),([0-9.-]+)""")

            for (folder in folderRe.findAll(xml)) {
                val folderText = folder.groupValues[1]
                val folderName = nameRe.find(folderText)?.groupValues?.get(1)?.trim() ?: continue
                if (folderName in SKIP_FOLDERS) {
                    Log.d(TAG, "Skipping folder: $folderName")
                    continue
                }
                Log.d(TAG, "Parsing folder: $folderName")
                for (pm in placemarkRe.findAll(folderText)) {
                    val pmText = pm.groupValues[1]
                    val name   = nameRe.find(pmText)?.groupValues?.get(1)?.trim() ?: continue
                    val m      = coordsRe.find(pmText) ?: continue
                    val lon    = m.groupValues[1].toDoubleOrNull() ?: continue  // coordinates are lon,lat
                    val lat    = m.groupValues[2].toDoubleOrNull() ?: continue
                    sites.add(DiveSite(name, lat, lon))
                }
            }
        } catch (e: Exception) { Log.e(TAG, "KML parse error: $e") }
        Log.i(TAG, "Parsed ${sites.size} dive sites from KML")
        return sites
    }

    // ── Dive site storage ─────────────────────────────────────────────────────

    private fun loadSites(): MutableList<DiveSite> {
        val json = getSharedPreferences("dive_sites", MODE_PRIVATE)
            .getString("sites", "[]") ?: "[]"
        val arr = JSONArray(json)
        return (0 until arr.length()).map {
            val o = arr.getJSONObject(it)
            DiveSite(o.getString("name"), o.getDouble("lat"), o.getDouble("lon"))
        }.toMutableList()
    }

    private fun saveSites(sites: List<DiveSite>) {
        val arr = JSONArray()
        sites.forEach { s -> arr.put(JSONObject().apply { put("name", s.name); put("lat", s.lat); put("lon", s.lon) }) }
        getSharedPreferences("dive_sites", MODE_PRIVATE).edit()
            .putString("sites", arr.toString()).apply()
    }

    private fun loadDiveSiteAssociations() {
        val json = getSharedPreferences("dive_sites", MODE_PRIVATE)
            .getString("dive_assoc", "{}") ?: "{}"
        val obj = JSONObject(json)
        obj.keys().forEach { diveToSite[it] = obj.getString(it) }
    }

    private fun setDiveSite(dive: DiveEntry, siteName: String) {
        val key = "${dive.date}_${dive.time}"
        diveToSite[key] = siteName
        val json = getSharedPreferences("dive_sites", MODE_PRIVATE)
            .getString("dive_assoc", "{}") ?: "{}"
        val obj = JSONObject(json); obj.put(key, siteName)
        getSharedPreferences("dive_sites", MODE_PRIVATE).edit()
            .putString("dive_assoc", obj.toString()).apply()
    }

    private fun getDiveSite(dive: DiveEntry): String? = diveToSite["${dive.date}_${dive.time}"]

    // ── Dive Sites dialog ─────────────────────────────────────────────────────

    private fun showSitesDialog(onSelected: ((DiveSite) -> Unit)? = null) {
        val dp = resources.displayMetrics.density
        val dialog = Dialog(this, android.R.style.Theme_Black_NoTitleBar_Fullscreen)
        val sites = loadSites()

        val root = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            setBackgroundColor(0xFF0D1820.toInt())
        }

        // ── Title bar ──────────────────────────────────────────────────────────
        val titleRow = LinearLayout(this).apply {
            orientation = LinearLayout.HORIZONTAL; gravity = Gravity.CENTER_VERTICAL
            setBackgroundColor(0xFF0D1820.toInt())
            setPadding((12 * dp).toInt(), (8 * dp).toInt(), (8 * dp).toInt(), (8 * dp).toInt())
        }
        titleRow.addView(TextView(this).apply {
            text = "📍 Dive Sites"; textSize = 17f; setTextColor(0xFFFFFFFF.toInt())
            setTypeface(typeface, Typeface.BOLD)
            layoutParams = LinearLayout.LayoutParams(0, LinearLayout.LayoutParams.WRAP_CONTENT, 1f)
        })
        val btnAddSite = Button(this).apply { text = "+ Add Site" }
        titleRow.addView(btnAddSite)
        titleRow.addView(Button(this).apply {
            text = "✕"; setOnClickListener { dialog.dismiss() }
        })
        root.addView(titleRow)

        if (onSelected != null) {
            root.addView(TextView(this).apply {
                text = "Tap a marker or list item to assign it to this dive"
                textSize = 12f; setTextColor(0xFF80C8E8.toInt()); gravity = Gravity.CENTER
                setPadding(0, (4 * dp).toInt(), 0, (4 * dp).toInt())
                setBackgroundColor(0xFF0A1828.toInt())
            })
        }

        // ── OpenStreetMap via Leaflet ──────────────────────────────────────────
        var siteAdapterRef: SiteAdapter? = null  // forward declaration for JS bridge
        var mapWebView: WebView? = null
        var mapReady = false
        val pendingRemote = mutableListOf<DiveSite>()

        fun injectMarker(view: WebView, site: DiveSite, fn: String) {
            val js = "$fn('${site.name.replace("'", "\\'")}',${site.lat},${site.lon});"
            view.evaluateJavascript(js, null)
        }

        // Fetch Google My Maps KML in background
        scope.launch {
            val remote = fetchGoogleMapSites()
            withContext(Dispatchers.Main) {
                pendingRemote.addAll(remote)
                if (mapReady) {
                    mapWebView?.let { wv -> remote.forEach { injectMarker(wv, it, "addRemoteSiteMarker") } }
                }
            }
        }

        val webView = WebView(this).apply {
            layoutParams = LinearLayout.LayoutParams(
                LinearLayout.LayoutParams.MATCH_PARENT, 0, 3f
            )
            settings.apply {
                javaScriptEnabled = true
                domStorageEnabled = true
                loadWithOverviewMode = true
                useWideViewPort = true
            }
            addJavascriptInterface(object : Any() {
                @android.webkit.JavascriptInterface
                fun onSiteSelected(name: String, lat: Double, lon: Double) {
                    runOnUiThread {
                        if (onSelected != null) {
                            onSelected(DiveSite(name, lat, lon)); dialog.dismiss()
                        } else {
                            Toast.makeText(this@MainActivity, name, Toast.LENGTH_SHORT).show()
                        }
                    }
                }
                @android.webkit.JavascriptInterface
                fun onMapTapped(lat: Double, lon: Double) {
                    runOnUiThread {
                        showAddSiteDialog(lat, lon) { newSite ->
                            sites.add(newSite); saveSites(sites)
                            mapWebView?.let { injectMarker(it, newSite, "addSiteMarker") }
                            siteAdapterRef?.notifyItemInserted(sites.size - 1)
                        }
                    }
                }
            }, "Android")

            webViewClient = object : WebViewClient() {
                override fun onPageFinished(view: WebView, url: String) {
                    mapReady = true
                    // Inject locally saved sites (blue markers)
                    sites.forEach { injectMarker(view, it, "addSiteMarker") }
                    // Inject already-fetched remote sites (green markers)
                    pendingRemote.forEach { injectMarker(view, it, "addRemoteSiteMarker") }
                }
            }
            loadUrl("file:///android_asset/map.html")
            mapWebView = this
        }
        root.addView(webView)

        // ── Divider ────────────────────────────────────────────────────────────
        root.addView(View(this).apply {
            setBackgroundColor(0xFF2A3F55.toInt())
            layoutParams = LinearLayout.LayoutParams(LinearLayout.LayoutParams.MATCH_PARENT, (1 * dp).toInt())
        })

        // ── Site list ──────────────────────────────────────────────────────────
        val bottomSection = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            layoutParams = LinearLayout.LayoutParams(LinearLayout.LayoutParams.MATCH_PARENT, 0, 2f)
            setPadding((12 * dp).toInt(), (4 * dp).toInt(), (12 * dp).toInt(), (4 * dp).toInt())
        }
        bottomSection.addView(TextView(this).apply {
            text = "Saved Sites (${sites.size})"; textSize = 13f; setTextColor(0xFF88AABB.toInt())
            setPadding(4, 0, 0, (6 * dp).toInt())
        })

        val siteRv = RecyclerView(this).apply {
            layoutManager = LinearLayoutManager(this@MainActivity)
            layoutParams = LinearLayout.LayoutParams(LinearLayout.LayoutParams.MATCH_PARENT, 0, 1f)
        }
        siteAdapterRef = SiteAdapter(sites,
            onSelect = { site ->
                if (onSelected != null) {
                    onSelected(site); dialog.dismiss()
                } else {
                    mapWebView?.evaluateJavascript("flyToSite(${site.lat},${site.lon});", null)
                    showSiteProfileDialog(site)
                }
            },
            onDelete = { pos ->
                AlertDialog.Builder(this)
                    .setTitle("Delete site?")
                    .setMessage("Remove \"${sites[pos].name}\"?")
                    .setPositiveButton("Delete") { _, _ ->
                        sites.removeAt(pos); saveSites(sites)
                        siteAdapterRef?.notifyItemRemoved(pos)
                    }
                    .setNegativeButton("Cancel", null).show()
            }
        )
        siteRv.adapter = siteAdapterRef
        bottomSection.addView(siteRv)
        root.addView(bottomSection)

        // Wire up "+ Add Site" button — activates tap-on-map mode
        btnAddSite.setOnClickListener {
            mapWebView?.evaluateJavascript("enableAddMode();", null)
            Toast.makeText(this, "Tap the map to place a new site", Toast.LENGTH_SHORT).show()
        }

        dialog.setContentView(root)
        dialog.show()
    }

    // ── Add site dialog ───────────────────────────────────────────────────────

    // ── Site temperature profile dialog ───────────────────────────────────────

    private fun showSiteProfileDialog(site: DiveSite) {
        val dp = resources.displayMetrics.density

        val profileView = SiteProfileView(this).apply {
            layoutParams = LinearLayout.LayoutParams(
                LinearLayout.LayoutParams.MATCH_PARENT, (280 * dp).toInt()
            )
        }

        val tvInfo = TextView(this).apply {
            text = "Loading dives…"; textSize = 12f; setTextColor(0xFF80AACC.toInt())
            setPadding(0, (8 * dp).toInt(), 0, 0)
        }

        val container = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL; setPadding(0, 0, 0, 0)
        }
        container.addView(profileView)
        container.addView(tvInfo)

        val dialog = AlertDialog.Builder(this)
            .setTitle("📍 ${site.name}")
            .setView(container)
            .setPositiveButton("Close", null)
            .create()
        dialog.show()

        scope.launch {
            if (!SupabaseClient.isLoggedIn) {
                withContext(Dispatchers.Main) { tvInfo.text = "Sign in to view site history." }
                return@launch
            }
            val dives = SupabaseClient.fetchSiteDives(site.name)
            withContext(Dispatchers.Main) {
                profileView.dives = dives
                if (dives.isEmpty()) {
                    tvInfo.text = "No dives recorded at this site yet."
                } else {
                    val maxD = dives.maxOf { it.maxdepth }
                    val allTemps = dives.mapNotNull { it.tempMin } + dives.mapNotNull { it.tempMax }
                    val tempRange = if (allTemps.isNotEmpty())
                        "${"%.0f".format(allTemps.min())}–${"%.0f".format(allTemps.max())} °C"
                    else "no temperature data"
                    tvInfo.text = "${dives.size} dive(s)  ·  max depth ${"%.0f".format(maxD)} m  ·  temp $tempRange\n" +
                        "Y = depth  ·  X = time  ·  colour = temperature"
                }
            }
        }
    }

    private fun showAddSiteDialog(
        prefilledLat: Double = Double.NaN,
        prefilledLon: Double = Double.NaN,
        onSaved: (DiveSite) -> Unit
    ) {
        val dp = resources.displayMetrics.density
        var pendingLat = prefilledLat; var pendingLon = prefilledLon

        val layout = LinearLayout(this).apply {
            orientation = LinearLayout.VERTICAL
            setPadding((20 * dp).toInt(), (16 * dp).toInt(), (20 * dp).toInt(), (8 * dp).toInt())
        }

        val etName = EditText(this).apply {
            hint = "Site name (e.g. Kylmäpihlaja)"; textSize = 15f
            setTextColor(0xFFEEEEEE.toInt()); setHintTextColor(0xFF666666.toInt())
            setBackgroundColor(0xFF1A2840.toInt()); setPadding(16, 12, 16, 12)
            layoutParams = LinearLayout.LayoutParams(LinearLayout.LayoutParams.MATCH_PARENT,
                LinearLayout.LayoutParams.WRAP_CONTENT).also { it.bottomMargin = (12 * dp).toInt() }
        }

        val tvCoords = TextView(this).apply {
            text = if (!prefilledLat.isNaN())
                "${"%.6f".format(prefilledLat)}°N, ${"%.6f".format(prefilledLon)}°E  (from map)"
            else "No location — use GPS or tap map"
            textSize = 12f; setTextColor(0xFF88AABB.toInt())
            layoutParams = LinearLayout.LayoutParams(LinearLayout.LayoutParams.MATCH_PARENT,
                LinearLayout.LayoutParams.WRAP_CONTENT).also { it.bottomMargin = (8 * dp).toInt() }
        }

        layout.addView(etName); layout.addView(tvCoords)

        // Show GPS button only when no map location was provided
        if (prefilledLat.isNaN()) {
            layout.addView(Button(this).apply {
                text = "📡 Use Current GPS Location"
                setOnClickListener {
                    text = "Getting location…"; isEnabled = false
                    getLocation { loc ->
                        runOnUiThread {
                            pendingLat = loc.latitude; pendingLon = loc.longitude
                            tvCoords.text = "${"%.6f".format(loc.latitude)}°N, ${"%.6f".format(loc.longitude)}°E"
                            text = "📡 Location acquired ✓"; isEnabled = true
                        }
                    }
                }
            })
        }

        AlertDialog.Builder(this)
            .setTitle("Add Dive Site")
            .setView(layout)
            .setPositiveButton("Save") { _, _ ->
                val name = etName.text.toString().trim()
                if (name.isEmpty()) {
                    Toast.makeText(this, "Please enter a site name", Toast.LENGTH_SHORT).show()
                    return@setPositiveButton
                }
                if (pendingLat.isNaN()) {
                    Toast.makeText(this, "Please get GPS location or tap the map first", Toast.LENGTH_SHORT).show()
                    return@setPositiveButton
                }
                onSaved(DiveSite(name, pendingLat, pendingLon))
            }
            .setNegativeButton("Cancel", null)
            .show()
    }

    // ── GPS location ──────────────────────────────────────────────────────────

    @SuppressLint("MissingPermission")
    private fun getLocation(callback: (Location) -> Unit) {
        if (ContextCompat.checkSelfPermission(this, Manifest.permission.ACCESS_FINE_LOCATION)
            != PackageManager.PERMISSION_GRANTED) {
            Toast.makeText(this, "Location permission required", Toast.LENGTH_SHORT).show()
            return
        }
        val lm = getSystemService(LOCATION_SERVICE) as LocationManager
        // Try last known first
        val last = lm.getLastKnownLocation(LocationManager.GPS_PROVIDER)
            ?: lm.getLastKnownLocation(LocationManager.NETWORK_PROVIDER)
        if (last != null && System.currentTimeMillis() - last.time < 60_000) {
            callback(last); return
        }
        // Request fresh fix
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.R) {
            lm.getCurrentLocation(LocationManager.GPS_PROVIDER, null, mainExecutor) { loc ->
                if (loc != null) callback(loc)
                else Toast.makeText(this, "Could not get GPS fix", Toast.LENGTH_SHORT).show()
            }
        } else {
            @Suppress("DEPRECATION")
            lm.requestSingleUpdate(LocationManager.GPS_PROVIDER,
                object : LocationListener {
                    override fun onLocationChanged(l: Location) { callback(l) }
                }, null)
        }
    }

    // ── Dive detail dialog ────────────────────────────────────────────────────

    private fun showDiveDetail(dive: DiveEntry) {
        val dp = resources.displayMetrics.density

        // Graph
        val graph = DiveProfileView(this).apply {
            samples = dive.samples; maxDepthHint = dive.maxdepth
            layoutParams = LinearLayout.LayoutParams(
                LinearLayout.LayoutParams.MATCH_PARENT, (220 * dp).toInt()
            )
        }

        // Dive site row
        val siteRow = LinearLayout(this).apply {
            orientation = LinearLayout.HORIZONTAL; gravity = Gravity.CENTER_VERTICAL
            setPadding(0, (10 * dp).toInt(), 0, (4 * dp).toInt())
        }
        val tvSite = TextView(this).apply {
            val cur = getDiveSite(dive)
            text = if (cur != null) "📍 $cur" else "📍 No dive site assigned"
            textSize = 13f
            setTextColor(if (cur != null) 0xFF80E8A0.toInt() else 0xFF888888.toInt())
            layoutParams = LinearLayout.LayoutParams(0, LinearLayout.LayoutParams.WRAP_CONTENT, 1f)
        }
        val btnChangeSite = Button(this).apply {
            text = "Change"; textSize = 12f
            setOnClickListener {
                showSitesDialog { site ->
                    setDiveSite(dive, site.name)
                    tvSite.text = "📍 ${site.name}"
                    tvSite.setTextColor(0xFF80E8A0.toInt())
                }
            }
        }
        siteRow.addView(tvSite); siteRow.addView(btnChangeSite)

        // Stats text
        val sb = StringBuilder()
        sb.appendLine("Date:      ${dive.date}  ${dive.time}")
        sb.appendLine("Mode:      ${dive.divemode}")
        sb.appendLine("Max depth: ${"%.1f".format(dive.maxdepth)} m")
        if (dive.avgdepth > 0) sb.appendLine("Avg depth: ${"%.1f".format(dive.avgdepth)} m")
        val mins = dive.duration / 60; val secs = dive.duration % 60
        sb.appendLine("Duration:  $mins min $secs s")
        dive.temp_surface?.let { sb.appendLine("Temp surf: ${"%.1f".format(it)} °C") }
        dive.temp_min    ?.let { sb.appendLine("Temp min:  ${"%.1f".format(it)} °C") }
        dive.temp_max    ?.let { sb.appendLine("Temp max:  ${"%.1f".format(it)} °C") }
        dive.atmospheric ?.let { sb.appendLine("Atm press: ${"%.0f".format(it * 1000)} mbar") }
        dive.salinity_type?.let { sb.appendLine("Salinity:  ${if (it == 0) "Fresh" else "Salt"}") }

        if (dive.gasmixes.length() > 0) {
            sb.appendLine(); sb.appendLine("Gas mixes:")
            for (i in 0 until dive.gasmixes.length()) {
                val g = dive.gasmixes.getJSONObject(i)
                val o2 = g.getDouble("o2"); val he = g.getDouble("he")
                val gasLabel = when {
                    he > 0.5  -> "Trimix ${"%.0f".format(o2)}/${"%.0f".format(he)}"
                    o2 > 21.5 -> "Nitrox ${"%.0f".format(o2)}"
                    o2 < 20.5 -> "Hypoxic ${"%.0f".format(o2)}"
                    else      -> "Air"
                }
                val usageMap = mapOf(0 to "none", 1 to "OC", 2 to "CCR diluent",
                    3 to "CCR O2", 4 to "gauge", 5 to "setpoint", 6 to "Bailout")
                sb.appendLine("  #${i+1}: $gasLabel  (${usageMap[g.getInt("usage")] ?: "?"})")
            }
        }

        if (dive.tanks.length() > 0) {
            sb.appendLine(); sb.appendLine("Tanks:")
            for (i in 0 until dive.tanks.length()) {
                val t   = dive.tanks.getJSONObject(i)
                val vol = t.getDouble("volume"); val wp = t.getDouble("workpressure")
                val beg = t.getDouble("start");  val end = t.getDouble("end")
                val volStr  = if (vol > 0) "${"%.0f".format(vol)} L" else "?"
                val wpStr   = if (wp  > 0) " @ ${"%.0f".format(wp)} bar" else ""
                val presStr = if (beg > 0) "  ${"%.0f".format(beg)}→${"%.0f".format(end)} bar" else ""
                sb.appendLine("  #${i+1}: Gas#${t.getInt("gasmix")+1}  $volStr$wpStr$presStr")
            }
        }

        val tvStats = TextView(this).apply {
            text = sb.toString(); textSize = 11f
            setTextColor(0xFFDDDDDD.toInt()); setPadding(4, 8, 4, 8)
            typeface = Typeface.MONOSPACE
        }

        // Collapsible sample table
        val count = minOf(dive.samples.length(), 500)
        var hasPressure = false; var hasCns = false; var hasDeco = false
        var hasRbt = false; var hasHr = false; var hasGasmix = false
        for (i in 0 until count) {
            val s = dive.samples.getJSONArray(i)
            if (s.optInt(3, -1)    >= 0) hasPressure = true
            if (s.optDouble(5, 0.0) >  0) hasCns = true
            if (s.optInt(6, -1)    >= 0) hasDeco = true
            if (s.optInt(9, 0)     >  0) hasRbt = true
            if (s.optInt(10, 0)    >  0) hasHr = true
            if (s.optInt(13, -1)   >= 0) hasGasmix = true
        }

        val tableSb = StringBuilder()
        val hdr = StringBuilder("  Time   Depth   Temp")
        if (hasPressure) hdr.append("   Press")
        if (hasCns)      hdr.append("    CNS")
        if (hasDeco)     hdr.append("        Deco")
        if (hasRbt)      hdr.append("     RBT")
        if (hasHr)       hdr.append("    HR")
        if (hasGasmix)   hdr.append(" GM")
        tableSb.appendLine(hdr); tableSb.appendLine("-".repeat(hdr.length))

        for (i in 0 until count) {
            val s = dive.samples.getJSONArray(i)
            val t = s.getInt(0) / 1000; val d = s.getDouble(1); val tc = s.getDouble(2)
            val row = StringBuilder("%5ds %5.1fm %4.1f°C".format(t, d, tc))
            if (hasPressure) {
                val tank = s.optInt(3, -1); val pv = s.optDouble(4, 0.0)
                if (tank >= 0) row.append(" %5.0fb".format(pv)) else row.append("        ")
            }
            if (hasCns) {
                val cns = s.optDouble(5, 0.0)
                if (cns > 0) row.append(" %5.1f%%".format(cns)) else row.append("       ")
            }
            if (hasDeco) {
                val dt = s.optInt(6, -1)
                if (dt >= 0) {
                    val dd = s.optDouble(7, 0.0); val dtime = s.optInt(8, 0)
                    val typeStr = when (dt) { 0 -> "NDL"; 1 -> "SS"; 2 -> "DC"; else -> "DS" }
                    row.append(" $typeStr %4.0fm %3ds".format(dd, dtime))
                } else row.append("             ")
            }
            if (hasRbt) { val rbt = s.optInt(9, 0); if (rbt > 0) row.append(" R%3ds".format(rbt)) else row.append("      ") }
            if (hasHr)  { val hr = s.optInt(10, 0); if (hr  > 0) row.append(" %3d♥".format(hr))   else row.append("      ") }
            if (hasGasmix) { val gm = s.optInt(13, -1); if (gm >= 0) row.append(" G${gm+1}") else row.append("    ") }
            tableSb.appendLine(row)
        }
        if (dive.samples.length() > 500) tableSb.appendLine("… (${dive.samples.length()} total samples)")

        val tvTable = TextView(this).apply {
            text = tableSb.toString(); textSize = 9.5f
            setTextColor(0xFFCCCCCC.toInt()); setPadding(4, 4, 4, 8)
            typeface = Typeface.MONOSPACE; visibility = View.GONE
        }

        val tvTableToggle = TextView(this).apply {
            text = "▶  Sample table  (${dive.samples.length()} rows)"
            textSize = 13f; setTextColor(0xFF4FC3F7.toInt())
            setPadding(4, (12 * dp).toInt(), 4, (8 * dp).toInt())
            setOnClickListener {
                if (tvTable.visibility == View.GONE) {
                    tvTable.visibility = View.VISIBLE
                    text = "▼  Sample table  (${dive.samples.length()} rows)"
                } else {
                    tvTable.visibility = View.GONE
                    text = "▶  Sample table  (${dive.samples.length()} rows)"
                }
            }
        }

        val container = LinearLayout(this).apply { orientation = LinearLayout.VERTICAL }
        container.addView(graph)
        container.addView(siteRow)
        container.addView(tvStats)
        container.addView(tvTableToggle)
        container.addView(tvTable)

        AlertDialog.Builder(this)
            .setTitle(dive.label)
            .setView(ScrollView(this).apply { addView(container) })
            .setPositiveButton("Close", null)
            .show()
    }

    // ── JNI callbacks ─────────────────────────────────────────────────────────

    companion object {
        var diveCounter = 0
        val existingDiveKeys = HashSet<String>()  // "date_time" keys already in the log
        var newDivesFound = 0
        var skippedDives  = 0
        var totalDeviceDives = 0
        var pendingFingerprintAddress: String? = null

        @JvmStatic fun onProgress(current: Int, total: Int) {
            instance?.runOnUiThread {
                instance?.progressBar?.let { pb -> pb.max = total; pb.progress = current }
            }
        }
        @JvmStatic fun onStatus(msg: String) {
            instance?.runOnUiThread { instance?.status(msg) }
        }
        @JvmStatic fun onDiveFound(json: String) {
            instance?.runOnUiThread {
                try {
                    val o    = JSONObject(json)
                    val date = o.getString("date")
                    val time = o.getString("time")
                    val key  = "${date}_${time}"

                    totalDeviceDives++

                    // Skip dives already in the log
                    if (key in existingDiveKeys) {
                        skippedDives++
                        instance?.status("Scanning… ${totalDeviceDives} on device, ${skippedDives} already in log")
                        return@runOnUiThread
                    }

                    diveCounter++
                    newDivesFound++
                    val dive = DiveEntry(
                        number        = diveCounter,
                        date          = date,
                        time          = time,
                        maxdepth      = o.getDouble("maxdepth"),
                        avgdepth      = o.optDouble("avgdepth", 0.0),
                        duration      = o.getInt("duration"),
                        divemode      = o.optString("divemode", "OC"),
                        temp_surface  = o.optDouble("temp_surface", Double.NaN).takeIf { !it.isNaN() },
                        temp_min      = o.optDouble("temp_min",     Double.NaN).takeIf { !it.isNaN() },
                        temp_max      = o.optDouble("temp_max",     Double.NaN).takeIf { !it.isNaN() },
                        atmospheric   = o.optDouble("atmospheric",  Double.NaN).takeIf { !it.isNaN() },
                        salinity_type = o.optInt("salinity_type", -1).takeIf { it >= 0 },
                        gasmixes      = o.optJSONArray("gasmixes") ?: JSONArray(),
                        tanks         = o.optJSONArray("tanks")    ?: JSONArray(),
                        samples       = o.getJSONArray("samples"),
                        isNew         = true
                    )
                    instance?.diveAdapter?.add(dive)
                    instance?.status("Found $newDivesFound new dive(s)…")
                } catch (e: Exception) {
                    Log.e(TAG, "onDiveFound parse error: $e")
                }
            }
        }

        @JvmStatic fun onFingerprint(hex: String) {
            if (skippedDives > 0) {
                Log.i(TAG, "Not saving fingerprint — $skippedDives skipped dives indicate gaps")
                return
            }
            val addr = pendingFingerprintAddress ?: return
            instance?.getSharedPreferences("fingerprints", MODE_PRIVATE)?.edit()
                ?.putString("fp_$addr", hex)
                ?.apply()
            Log.i(TAG, "Saved fingerprint for $addr: $hex")
        }

        var instance: MainActivity? = null
    }

    private fun status(msg: String) { Log.i(TAG, msg); tvStatus.text = msg }
    override fun onStart()  { super.onStart();  instance = this }
    override fun onStop()   { super.onStop();   instance = null }
}
