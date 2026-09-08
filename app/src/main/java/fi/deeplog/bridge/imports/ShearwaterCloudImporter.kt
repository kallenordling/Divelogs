package fi.deeplog.bridge.imports

import android.database.sqlite.SQLiteDatabase
import android.util.Log
import fi.deeplog.bridge.DcBridge
import org.json.JSONObject
import java.io.File

/**
 * Shearwater Cloud's desktop database (`.db`).
 *
 * Two tables matter: `dive_details` holds the logged metadata, `log_data` holds
 * the bytes the computer sent over the wire, split across three BLOB columns
 * and still carrying the LRE+XOR compression from the download.
 *
 * Those bytes are exactly what libdivecomputer's Shearwater parser reads, so
 * the profile is recovered by handing them to [DcBridge.parseRaw] rather than
 * by decoding the format here. When that fails the dive is still imported from
 * the metadata columns, just without a profile.
 */
class ShearwaterCloudImporter : DiveImporter {

    override val name = "Shearwater Cloud"

    /** Products whose parser understands Petrel Native Format, best guess first. */
    private val candidates = listOf("Petrel 2", "Perdix", "Teric", "Petrel")

    override fun sniff(bytes: ByteArray, filename: String): Boolean {
        if (bytes.size < 16) return false
        if (String(bytes, 0, 15, Charsets.US_ASCII) != "SQLite format 3") return false
        // Distinguishing a Shearwater DB from any other SQLite file needs the
        // table list, which means opening it; the header check plus the table
        // probe in parse() is enough to keep it out of other importers' way.
        return true
    }

    override fun parse(bytes: ByteArray): ImportResult {
        val tmp = File.createTempFile("shearwater", ".db")
        try {
            tmp.writeBytes(bytes)
            return read(tmp)
        } finally {
            tmp.delete()
        }
    }

    private fun read(file: File): ImportResult {
        val warnings = mutableListOf<String>()
        val dives = mutableListOf<ImportedDive>()

        SQLiteDatabase.openDatabase(file.path, null, SQLiteDatabase.OPEN_READONLY).use { db ->
            val tables = db.rawQuery(
                "SELECT name FROM sqlite_master WHERE type='table'", null
            ).use { c ->
                val names = HashSet<String>()
                while (c.moveToNext()) names.add(c.getString(0))
                names
            }
            require("dive_details" in tables) {
                "Not a Shearwater Cloud database (no dive_details table)"
            }
            val hasLogData = "log_data" in tables

            val sql = if (hasLogData) """
                SELECT dd.DiveId, dd.DiveDate, dd.Depth, dd.AverageDepth,
                       dd.DiveLengthTime, dd.SerialNumber, dd.Location, dd.Site,
                       dd.AirTemperature,
                       ld.data_bytes_1, ld.data_bytes_2, ld.data_bytes_3
                FROM dive_details dd
                LEFT JOIN log_data ld ON dd.DiveId = ld.log_id
                ORDER BY dd.DiveDate
            """ else """
                SELECT DiveId, DiveDate, Depth, AverageDepth,
                       DiveLengthTime, SerialNumber, Location, Site, AirTemperature
                FROM dive_details ORDER BY DiveDate
            """

            db.rawQuery(sql.trimIndent(), null).use { c ->
                while (c.moveToNext()) {
                    try {
                        val raw = if (hasLogData) concatBlobs(c, 9, 10, 11) else null
                        val fromBlob = raw?.let { parseWithLibdivecomputer(it) }
                        dives.add(fromBlob ?: fromColumns(c))
                    } catch (e: Exception) {
                        warnings.add("Skipped a dive: ${e.message}")
                    }
                }
            }
            if (dives.none { it.samples.isNotEmpty() } && hasLogData) {
                warnings.add("No profiles could be decoded; imported summary data only")
            }
        }
        return ImportResult(dives, warnings, name)
    }

    private fun concatBlobs(c: android.database.Cursor, vararg cols: Int): ByteArray? {
        val parts = cols.toList().mapNotNull { i -> if (c.isNull(i)) null else c.getBlob(i) }
        if (parts.isEmpty()) return null
        val out = ByteArray(parts.sumOf { it.size })
        var at = 0
        for (p in parts) { p.copyInto(out, at); at += p.size }
        return out
    }

    /** Tries each candidate product until one of their parsers accepts the log. */
    private fun parseWithLibdivecomputer(raw: ByteArray): ImportedDive? {
        for (product in candidates) {
            val json = try {
                DcBridge.parseRaw("Shearwater", product, raw, true)
            } catch (e: Throwable) {
                Log.w("ShearwaterImport", "parseRaw($product) failed: $e"); null
            } ?: continue
            return fromDiveJson(JSONObject(json)) ?: continue
        }
        return null
    }

    /** Turns the native parser's JSON back into an [ImportedDive]. */
    private fun fromDiveJson(o: JSONObject): ImportedDive? {
        val date = o.optString("date").takeIf { it.isNotEmpty() } ?: return null
        // The native parser writes 0000-00-00 when the log has no usable date.
        if (date.startsWith("0000")) return null

        val samples = o.optJSONArray("samples")?.let { arr ->
            (0 until arr.length()).map { i ->
                val s = arr.getJSONArray(i)
                ImportedSample(
                    timeMs = s.getInt(0),
                    depth = s.getDouble(1),
                    temp = s.getDouble(2),
                    pressureTank = s.optInt(3, -1),
                    pressureValue = s.optDouble(4, 0.0),
                    cns = s.optDouble(5, 0.0),
                    setpoint = s.optDouble(12, 0.0),
                    gasmix = s.optInt(13, -1)
                )
            }
        }.orEmpty()

        val gases = o.optJSONArray("gasmixes")?.let { arr ->
            (0 until arr.length()).map { i ->
                val g = arr.getJSONObject(i)
                ImportedGas(g.optDouble("o2", 21.0), g.optDouble("he", 0.0))
            }
        }.orEmpty()

        val tanks = o.optJSONArray("tanks")?.let { arr ->
            (0 until arr.length()).map { i ->
                val t = arr.getJSONObject(i)
                ImportedTank(
                    gasmix = t.optInt("gasmix", -1),
                    volume = t.optDouble("volume", 0.0),
                    workpressure = t.optDouble("workpressure", 0.0),
                    start = t.optDouble("start", 0.0),
                    end = t.optDouble("end", 0.0)
                )
            }
        }.orEmpty()

        return ImportedDive(
            date = date,
            time = o.optString("time", "00:00"),
            maxdepth = o.optDouble("maxdepth", 0.0),
            duration = o.optInt("duration", 0),
            avgdepth = o.optDouble("avgdepth", 0.0),
            divemode = o.optString("divemode", "OC"),
            tempSurface = o.optDouble("temp_surface", Double.NaN).takeIf { !it.isNaN() },
            tempMin = o.optDouble("temp_min", Double.NaN).takeIf { !it.isNaN() },
            tempMax = o.optDouble("temp_max", Double.NaN).takeIf { !it.isNaN() },
            gasmixes = gases,
            tanks = tanks,
            samples = samples
        )
    }

    /** Fallback when the blob is missing or unparseable: the logged summary. */
    private fun fromColumns(c: android.database.Cursor): ImportedDive {
        fun str(i: Int) = if (c.isNull(i)) null else c.getString(i)
        fun dbl(i: Int) = if (c.isNull(i)) null else c.getDouble(i)

        val stamp = ImportUtil.stamp(str(1))
            ?: throw IllegalArgumentException("unreadable DiveDate '${str(1)}'")

        return ImportedDive(
            date = stamp.date,
            time = stamp.time,
            maxdepth = dbl(2) ?: 0.0,
            // DiveLengthTime is minutes in Shearwater Cloud.
            duration = ((dbl(4) ?: 0.0) * 60).toInt(),
            avgdepth = dbl(3) ?: 0.0,
            tempSurface = dbl(8),
            siteName = listOfNotNull(str(7), str(6)).firstOrNull { it.isNotBlank() }
        )
    }
}
