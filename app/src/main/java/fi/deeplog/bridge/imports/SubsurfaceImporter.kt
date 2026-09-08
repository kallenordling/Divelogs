package fi.deeplog.bridge.imports

import fi.deeplog.bridge.imports.ImportUtil.attr
import fi.deeplog.bridge.imports.ImportUtil.children
import fi.deeplog.bridge.imports.ImportUtil.descendants
import fi.deeplog.bridge.imports.ImportUtil.duration
import fi.deeplog.bridge.imports.ImportUtil.number
import org.w3c.dom.Element

/**
 * Subsurface's native XML export (`<divelog>`).
 *
 * Subsurface writes every measurement as a unit-suffixed string ("30.2 m",
 * "232.0 bar", "45:00 min"), always in metric on export regardless of the
 * display units, so [number] and [duration] are enough to read them.
 */
class SubsurfaceImporter : DiveImporter {

    override val name = "Subsurface XML"

    override fun sniff(bytes: ByteArray, filename: String): Boolean {
        val head = String(bytes, 0, minOf(bytes.size, 2048), Charsets.UTF_8)
        return head.contains("<divelog", ignoreCase = true)
    }

    override fun parse(bytes: ByteArray): ImportResult {
        val root = ImportUtil.parseXml(bytes).documentElement
        val warnings = mutableListOf<String>()

        // uuid -> (name, lat, lon). gps is "lat lon" in decimal degrees.
        val sites = HashMap<String, Triple<String, Double?, Double?>>()
        root.descendants("site").forEach { site ->
            val uuid = site.attr("uuid") ?: return@forEach
            val gps = site.attr("gps")?.split(Regex("\\s+"))
            sites[uuid] = Triple(
                site.attr("name") ?: "",
                gps?.getOrNull(0)?.toDoubleOrNull(),
                gps?.getOrNull(1)?.toDoubleOrNull()
            )
        }

        val dives = mutableListOf<ImportedDive>()
        for (dive in root.descendants("dive")) {
            try {
                dives.add(parseDive(dive, sites))
            } catch (e: Exception) {
                warnings.add("Skipped dive ${dive.attr("number") ?: "?"}: ${e.message}")
            }
        }
        return ImportResult(dives, warnings, name)
    }

    private fun parseDive(
        dive: Element,
        sites: Map<String, Triple<String, Double?, Double?>>
    ): ImportedDive {
        val date = dive.attr("date") ?: throw IllegalArgumentException("no date")
        val time = (dive.attr("time") ?: "00:00:00").take(5)

        // Subsurface writes a fixed set of <cylinder> slots and leaves the
        // unused ones empty; those are not real cylinders.
        val cylinderEls = dive.children("cylinder").filter { c ->
            c.attr("size") != null || c.attr("o2") != null || c.attr("start") != null
        }
        // One gas per cylinder, in order, so a cylinder's index is its gas index.
        val gases = cylinderEls.map { c ->
            ImportedGas(o2 = number(c.attr("o2")) ?: 21.0, he = number(c.attr("he")) ?: 0.0)
        }
        val tanks = cylinderEls.mapIndexed { i, c ->
            ImportedTank(
                gasmix = i,
                volume = number(c.attr("size")) ?: 0.0,
                workpressure = number(c.attr("workpressure")) ?: 0.0,
                start = number(c.attr("start")) ?: 0.0,
                end = number(c.attr("end")) ?: 0.0
            )
        }

        // A dive can carry several <divecomputer> blocks; the first with
        // samples is the one worth importing.
        val computers = dive.children("divecomputer")
        val dc = computers.firstOrNull { it.children("sample").isNotEmpty() }
            ?: computers.firstOrNull()

        // Samples are differential: Subsurface only writes an attribute when
        // its value changed, so anything absent carries over from the sample
        // before it. Reading them independently would put 0 m and 0 C spikes
        // through the whole profile.
        var lastDepth = 0.0
        var lastTemp = 0.0
        var lastPressure: Double? = null
        var lastPressureTank = -1

        val samples = dc?.children("sample").orEmpty().map { s ->
            // Tank pressures are per-cylinder attributes: pressure0, pressure1...
            val pressureAttr = (0 until maxOf(tanks.size, 1)).firstNotNullOfOrNull { i ->
                s.attr("pressure$i")?.let { i to number(it) }
            } ?: s.attr("pressure")?.let { -1 to number(it) }

            if (pressureAttr?.second != null) {
                lastPressure = pressureAttr.second
                lastPressureTank = if (pressureAttr.first >= 0) pressureAttr.first else 0
            }
            number(s.attr("depth"))?.let { lastDepth = it }
            number(s.attr("temp"))?.let { lastTemp = it }

            ImportedSample(
                timeMs = (duration(s.attr("time")) ?: 0) * 1000,
                depth = lastDepth,
                temp = lastTemp,
                pressureTank = if (lastPressure != null) lastPressureTank else -1,
                pressureValue = lastPressure ?: 0.0,
                cns = number(s.attr("cns")) ?: 0.0,
                // Subsurface writes either an ndl or a stoptime/stopdepth pair.
                decoType = when {
                    s.attr("stoptime") != null -> 2   // deco stop
                    s.attr("ndl") != null -> 0        // no-stop
                    else -> -1
                },
                decoDepth = number(s.attr("stopdepth")) ?: 0.0,
                decoTime = duration(s.attr("stoptime") ?: s.attr("ndl")) ?: 0,
                heartbeat = number(s.attr("heartbeat"))?.toInt() ?: 0,
                bearing = number(s.attr("bearing"))?.toInt() ?: 0,
                setpoint = number(s.attr("po2")) ?: 0.0
            )
        }

        val depth = dc?.children("depth")?.firstOrNull()
        // Water temperature lives on the computer, air temperature on the dive.
        val dcTemp = dc?.children("temperature")?.firstOrNull()
        val diveTemp = dive.children("divetemperature").firstOrNull()

        val site = dive.attr("divesiteid")?.let { sites[it] }

        return ImportedDive(
            date = date,
            time = time,
            maxdepth = number(depth?.attr("max")) ?: samples.maxOfOrNull { it.depth } ?: 0.0,
            duration = duration(dive.attr("duration"))
                ?: samples.maxOfOrNull { it.timeMs / 1000 } ?: 0,
            avgdepth = number(depth?.attr("mean")) ?: 0.0,
            divemode = dc?.attr("dctype")?.let(::divemode) ?: "OC",
            tempSurface = number(diveTemp?.attr("air") ?: dcTemp?.attr("air")),
            tempMin = number(dcTemp?.attr("water") ?: diveTemp?.attr("water")),
            atmospheric = number(dc?.attr("airpressure"))?.div(1000.0),
            // Above ~1015 g/l the water is salt; Subsurface writes g/l.
            salinityType = number(dive.attr("watersalinity"))?.let { if (it > 1015) 1 else 0 },
            gasmixes = gases,
            tanks = tanks,
            samples = samples,
            siteName = site?.first?.takeIf { it.isNotEmpty() },
            siteLat = site?.second,
            siteLon = site?.third,
            notes = dive.children("notes").firstOrNull()?.textContent?.trim()
                ?.takeIf { it.isNotEmpty() }
        )
    }

    private fun divemode(raw: String) = when (raw.lowercase()) {
        "ccr" -> "CCR"
        "pscr", "scr" -> "SCR"
        "freedive", "apnea" -> "Freedive"
        "gauge" -> "Gauge"
        else -> "OC"
    }
}
