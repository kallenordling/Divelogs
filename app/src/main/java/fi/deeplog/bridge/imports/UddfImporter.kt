package fi.deeplog.bridge.imports

import fi.deeplog.bridge.imports.ImportUtil.attr
import fi.deeplog.bridge.imports.ImportUtil.childDouble
import fi.deeplog.bridge.imports.ImportUtil.childText
import fi.deeplog.bridge.imports.ImportUtil.children
import fi.deeplog.bridge.imports.ImportUtil.descendants
import fi.deeplog.bridge.imports.ImportUtil.kelvinToCelsius
import org.w3c.dom.Element

/**
 * UDDF (Universal Dive Data Format) 3.x, as written by Subsurface, Shearwater
 * Cloud, MacDive and most desktop logbooks.
 *
 * UDDF is strict SI throughout: metres, seconds, Kelvin, Pascal, cubic metres.
 */
class UddfImporter : DiveImporter {

    override val name = "UDDF"

    override fun sniff(bytes: ByteArray, filename: String): Boolean {
        val head = String(bytes, 0, minOf(bytes.size, 2048), Charsets.UTF_8)
        return head.contains("<uddf", ignoreCase = true)
    }

    override fun parse(bytes: ByteArray): ImportResult {
        val doc = ImportUtil.parseXml(bytes)
        val root = doc.documentElement
        val warnings = mutableListOf<String>()

        // Gas mixes are declared once and referenced by id from dives.
        val mixes = LinkedHashMap<String, ImportedGas>()
        root.descendants("mix").forEach { mix ->
            val id = mix.attr("id") ?: return@forEach
            // Fractions 0..1 in UDDF; the dive JSON wants percent.
            val o2 = mix.childDouble("o2")?.times(100)
            val he = mix.childDouble("he")?.times(100) ?: 0.0
            if (o2 != null) mixes[id] = ImportedGas(o2 = o2, he = he)
        }

        // Dive sites, likewise referenced by id.
        data class Site(val name: String?, val lat: Double?, val lon: Double?)
        val sites = HashMap<String, Site>()
        root.descendants("site").forEach { site ->
            val id = site.attr("id") ?: return@forEach
            val geo = site.children("geography").firstOrNull()
            sites[id] = Site(
                site.childText("name"),
                geo?.childDouble("latitude"),
                geo?.childDouble("longitude")
            )
        }

        val dives = mutableListOf<ImportedDive>()
        for (dive in root.descendants("dive")) {
            try {
                parseDive(dive, mixes, sites.mapValues { (_, s) -> Triple(s.name, s.lat, s.lon) })
                    ?.let { dives.add(it) }
                    ?: warnings.add("Skipped a dive with no usable date")
            } catch (e: Exception) {
                warnings.add("Skipped a dive: ${e.message}")
            }
        }
        return ImportResult(dives, warnings, name)
    }

    private fun parseDive(
        dive: Element,
        mixes: Map<String, ImportedGas>,
        sites: Map<String, Triple<String?, Double?, Double?>>
    ): ImportedDive? {
        val before = dive.children("informationbeforedive").firstOrNull()
        val after = dive.children("informationafterdive").firstOrNull()

        val stamp = ImportUtil.stamp(before?.childText("datetime") ?: dive.childText("datetime"))
            ?: return null

        // Only the mixes this dive actually references, in reference order, so
        // waypoint switchmix refs can be turned into indices.
        val usedMixIds = LinkedHashSet<String>()
        dive.descendants("switchmix").forEach { it.attr("ref")?.let(usedMixIds::add) }
        dive.descendants("tankdata").forEach { td ->
            td.children("link").firstOrNull()?.attr("ref")?.let(usedMixIds::add)
        }
        val mixOrder = usedMixIds.filter { mixes.containsKey(it) }
        val gasList = mixOrder.mapNotNull { mixes[it] }
        val mixIndex = mixOrder.withIndex().associate { (i, id) -> id to i }

        val tanks = dive.descendants("tankdata").map { td ->
            val ref = td.children("link").firstOrNull()?.attr("ref")
            ImportedTank(
                gasmix = ref?.let { mixIndex[it] } ?: -1,
                // m³ -> litres, Pa -> bar
                volume = (td.childDouble("tankvolume") ?: 0.0) * 1000.0,
                start = (td.childDouble("tankpressurebegin") ?: 0.0) / 100_000.0,
                end = (td.childDouble("tankpressureend") ?: 0.0) / 100_000.0
            )
        }

        var currentGas = -1
        var currentTank = if (tanks.isNotEmpty()) 0 else -1
        val samples = dive.descendants("waypoint").map { wp ->
            wp.children("switchmix").firstOrNull()?.attr("ref")?.let { ref ->
                mixIndex[ref]?.let { currentGas = it }
            }
            val pressurePa = wp.childDouble("tankpressure")
            ImportedSample(
                timeMs = ((wp.childDouble("divetime") ?: 0.0) * 1000).toInt(),
                depth = wp.childDouble("depth") ?: 0.0,
                temp = wp.childDouble("temperature")?.let(::kelvinToCelsius) ?: 0.0,
                pressureTank = if (pressurePa != null) currentTank else -1,
                pressureValue = pressurePa?.div(100_000.0) ?: 0.0,
                gasmix = currentGas
            )
        }

        val maxDepth = after?.childDouble("greatestdepth")
            ?: samples.maxOfOrNull { it.depth } ?: 0.0
        val duration = after?.childDouble("diveduration")?.toInt()
            ?: samples.maxOfOrNull { it.timeMs / 1000 } ?: 0

        return ImportedDive(
            date = stamp.date,
            time = stamp.time,
            maxdepth = maxDepth,
            duration = duration,
            avgdepth = after?.childDouble("averagedepth") ?: 0.0,
            tempSurface = before?.childDouble("airtemperature")?.let(::kelvinToCelsius),
            tempMin = after?.childDouble("lowesttemperature")?.let(::kelvinToCelsius),
            tempMax = after?.childDouble("highesttemperature")?.let(::kelvinToCelsius),
            gasmixes = gasList,
            tanks = tanks,
            samples = samples,
            siteName = siteFor(dive, sites)?.first,
            siteLat = siteFor(dive, sites)?.second,
            siteLon = siteFor(dive, sites)?.third,
            notes = dive.descendants("notes").firstOrNull()?.textContent?.trim()
                ?.takeIf { it.isNotEmpty() }
        )
    }

    private fun siteFor(
        dive: Element,
        sites: Map<String, Triple<String?, Double?, Double?>>
    ): Triple<String?, Double?, Double?>? =
        dive.descendants("link").firstNotNullOfOrNull { link ->
            link.attr("ref")?.let { sites[it] }
        }
}
