package fi.deeplog.bridge

import android.content.Context
import android.util.Log
import org.json.JSONObject
import java.text.Normalizer

/**
 * The bundled catalogue of known dive sites.
 *
 * Around 3,300 entries extracted from OpenStreetMap (`sport=scuba_diving`,
 * `amenity=dive_centre`, `historic=wreck`, `seamark:type=wreck`) and shipped as
 * an asset, so browsing works with no network and nothing to sync.
 *
 * The data is OpenStreetMap's, used under ODbL 1.0, which requires the
 * attribution in [ATTRIBUTION] to be shown wherever it is presented.
 */
object SiteCatalog {

    private const val TAG = "SiteCatalog"
    private const val ASSET = "dive_sites.json"

    const val ATTRIBUTION = "Site data © OpenStreetMap contributors, ODbL 1.0"

    data class Entry(
        val name: String,
        val lat: Double,
        val lon: Double,
        val country: String?,
        val region: String?,
        val description: String?
    ) {
        fun toDiveSite() = DiveSite(name, lat, lon)

        /** "Finland · Wreck", for the second line of a list row. */
        val subtitle: String
            get() = listOfNotNull(country, region, description)
                .distinct()
                .joinToString(" · ")
                .ifEmpty { "%.4f, %.4f".format(lat, lon) }

        /** Lower-cased, accent-stripped haystack, built once per entry. */
        internal val haystack: String =
            fold(listOfNotNull(name, country, region, description).joinToString(" "))
    }

    @Volatile private var cache: List<Entry>? = null

    /** Parses the asset on first use; later calls reuse the result. */
    fun entries(context: Context): List<Entry> {
        cache?.let { return it }
        synchronized(this) {
            cache?.let { return it }
            val loaded = try {
                load(context)
            } catch (e: Exception) {
                Log.e(TAG, "Could not read $ASSET", e)
                emptyList()
            }
            cache = loaded
            return loaded
        }
    }

    private fun load(context: Context): List<Entry> {
        val text = context.assets.open(ASSET).use { it.readBytes() }.toString(Charsets.UTF_8)
        val arr = JSONObject(text).getJSONArray("sites")
        val out = ArrayList<Entry>(arr.length())
        for (i in 0 until arr.length()) {
            val o = arr.getJSONObject(i)
            out.add(
                Entry(
                    name = o.getString("name"),
                    lat = o.getDouble("lat"),
                    lon = o.getDouble("lon"),
                    country = o.optString("country").takeIf { it.isNotEmpty() },
                    region = o.optString("region").takeIf { it.isNotEmpty() },
                    description = o.optString("desc").takeIf { it.isNotEmpty() }
                )
            )
        }
        Log.i(TAG, "Loaded ${out.size} catalogue sites")
        return out
    }

    /**
     * Case- and accent-insensitive search over name, country, region and
     * description, so "Ojamon", "ojamon" and "Finland" all find the quarry.
     *
     * @param limit caps the result list; the catalogue is far too large to put
     *   in a RecyclerView unfiltered.
     */
    fun search(context: Context, query: String, limit: Int = 200): List<Entry> {
        val all = entries(context)
        val q = fold(query.trim())
        if (q.isEmpty()) return all.take(limit)

        // Name matches are what people are usually after, so they come first.
        val byName = mutableListOf<Entry>()
        val other = mutableListOf<Entry>()
        for (e in all) {
            if (!e.haystack.contains(q)) continue
            if (fold(e.name).contains(q)) byName.add(e) else other.add(e)
            if (byName.size >= limit) break
        }
        return (byName + other).take(limit)
    }

    /** Nearest entries to a point, for "what is around here?". */
    fun near(context: Context, lat: Double, lon: Double, limit: Int = 50): List<Entry> =
        entries(context)
            .sortedBy { e ->
                // Planar approximation: plenty for ranking, and it avoids a
                // trig call per entry across the whole catalogue.
                val dLat = e.lat - lat
                val dLon = (e.lon - lon) * Math.cos(Math.toRadians(lat))
                dLat * dLat + dLon * dLon
            }
            .take(limit)

    private fun fold(s: String): String =
        Normalizer.normalize(s.lowercase(), Normalizer.Form.NFD)
            .replace(Regex("\\p{Mn}+"), "")
}
