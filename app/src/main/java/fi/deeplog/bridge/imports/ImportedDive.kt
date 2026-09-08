package fi.deeplog.bridge.imports

import org.json.JSONArray
import org.json.JSONObject

/**
 * A dive assembled by an importer.
 *
 * Importers build these and call [toJson]; the result is byte-for-byte the same
 * shape bridge.cpp emits for a device download, so imported dives travel the
 * same ingestion path as downloaded ones and need no special cases downstream.
 */
data class ImportedSample(
    val timeMs: Int,
    val depth: Double = 0.0,
    val temp: Double = 0.0,
    val pressureTank: Int = -1,
    val pressureValue: Double = 0.0,
    val cns: Double = 0.0,
    val decoType: Int = -1,
    val decoDepth: Double = 0.0,
    val decoTime: Int = 0,
    val rbt: Int = 0,
    val heartbeat: Int = 0,
    val bearing: Int = 0,
    val setpoint: Double = 0.0,
    val gasmix: Int = -1,
    val ppo2: List<Pair<Int, Double>> = emptyList()
) {
    fun toJson(): JSONArray = JSONArray().apply {
        put(timeMs); put(depth); put(temp)
        put(pressureTank); put(pressureValue)
        put(cns); put(decoType); put(decoDepth); put(decoTime)
        put(rbt); put(heartbeat); put(bearing); put(setpoint); put(gasmix)
        put(ppo2.size)
        ppo2.forEach { (sensor, value) -> put(sensor); put(value) }
    }
}

/** Percentages, matching the 0-100 scale bridge.cpp writes. */
data class ImportedGas(
    val o2: Double,
    val he: Double = 0.0,
    val n2: Double = 100.0 - o2 - he,
    val usage: Int = 0
) {
    fun toJson(): JSONObject = JSONObject().apply {
        put("o2", o2); put("he", he); put("n2", n2); put("usage", usage)
    }
}

/** Volume in litres, pressures in bar. */
data class ImportedTank(
    val gasmix: Int = -1,
    val volume: Double = 0.0,
    val workpressure: Double = 0.0,
    val start: Double = 0.0,
    val end: Double = 0.0,
    val usage: Int = 0
) {
    fun toJson(): JSONObject = JSONObject().apply {
        put("gasmix", gasmix); put("volume", volume); put("workpressure", workpressure)
        put("start", start); put("end", end); put("usage", usage)
    }
}

data class ImportedDive(
    /** ISO date, "YYYY-MM-DD". */
    val date: String,
    /** "HH:MM", 24-hour. */
    val time: String,
    val maxdepth: Double,
    val duration: Int,
    val avgdepth: Double = 0.0,
    val divemode: String = "OC",
    val tempSurface: Double? = null,
    val tempMin: Double? = null,
    val tempMax: Double? = null,
    val atmospheric: Double? = null,
    val salinityType: Int? = null,
    val gasmixes: List<ImportedGas> = emptyList(),
    val tanks: List<ImportedTank> = emptyList(),
    val samples: List<ImportedSample> = emptyList(),
    /** Carried through to the upload so imported dives keep their site. */
    val siteName: String? = null,
    val siteLat: Double? = null,
    val siteLon: Double? = null,
    /** Free-text note from the source, kept for display. */
    val notes: String? = null
) {
    /** The key MainActivity dedupes on. */
    val key get() = "${date}_$time"

    fun toJson(): JSONObject = JSONObject().apply {
        put("date", date)
        put("time", time)
        put("maxdepth", maxdepth)
        put("avgdepth", avgdepth)
        put("duration", duration)
        put("divemode", divemode)
        tempSurface?.let { put("temp_surface", it) }
        tempMin?.let { put("temp_min", it) }
        tempMax?.let { put("temp_max", it) }
        atmospheric?.let { put("atmospheric", it) }
        salinityType?.let { put("salinity_type", it) }
        siteName?.let { put("site_name", it) }
        siteLat?.let { put("site_lat", it) }
        siteLon?.let { put("site_lon", it) }
        notes?.let { put("notes", it) }
        put("gasmixes", JSONArray().apply { gasmixes.forEach { put(it.toJson()) } })
        put("tanks", JSONArray().apply { tanks.forEach { put(it.toJson()) } })
        put("samples", JSONArray().apply { samples.forEach { put(it.toJson()) } })
    }
}

/** What an importer hands back: the dives it understood, plus what it could not. */
data class ImportResult(
    val dives: List<ImportedDive>,
    val warnings: List<String> = emptyList(),
    /** Human-readable name of the format, for the summary line. */
    val format: String = "Unknown"
)
