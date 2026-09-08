package fi.deeplog.bridge.imports

import fi.deeplog.bridge.imports.ImportUtil.childDouble
import fi.deeplog.bridge.imports.ImportUtil.childInt
import fi.deeplog.bridge.imports.ImportUtil.childText
import fi.deeplog.bridge.imports.ImportUtil.children
import fi.deeplog.bridge.imports.ImportUtil.cuftToLitres
import fi.deeplog.bridge.imports.ImportUtil.descendants
import fi.deeplog.bridge.imports.ImportUtil.fahrenheitToCelsius
import fi.deeplog.bridge.imports.ImportUtil.feetToMetres
import fi.deeplog.bridge.imports.ImportUtil.number
import fi.deeplog.bridge.imports.ImportUtil.psiToBar
import org.w3c.dom.Element

/**
 * MacDive's XML logbook export (`<dives>` with a macdive_logbook.dtd doctype).
 *
 * Unlike Subsurface, MacDive writes bare numbers and declares the unit system
 * once, in a top-level `<units>` element, so every measurement has to be
 * converted according to that.
 */
class MacDiveImporter : DiveImporter {

    override val name = "MacDive XML"

    override fun sniff(bytes: ByteArray, filename: String): Boolean {
        val head = String(bytes, 0, minOf(bytes.size, 4096), Charsets.UTF_8)
        return head.contains("macdive", ignoreCase = true) ||
            (head.contains("<dives>") && head.contains("<schema>"))
    }

    override fun parse(bytes: ByteArray): ImportResult {
        val root = ImportUtil.parseXml(bytes).documentElement
        val imperial = root.childText("units")?.equals("Imperial", ignoreCase = true) == true
        val warnings = mutableListOf<String>()

        fun depth(v: Double?) = v?.let { if (imperial) feetToMetres(it) else it }
        fun temp(v: Double?) = v?.let { if (imperial) fahrenheitToCelsius(it) else it }
        fun press(v: Double?) = v?.let { if (imperial) psiToBar(it) else it }

        val dives = mutableListOf<ImportedDive>()
        for (dive in root.children("dive")) {
            try {
                val stamp = ImportUtil.stamp(dive.childText("date"))
                    ?: throw IllegalArgumentException("no date")

                val gasEls = dive.descendants("gas")
                val gases = gasEls.map {
                    ImportedGas(
                        o2 = it.childDouble("oxygen") ?: 21.0,
                        he = it.childDouble("helium") ?: 0.0
                    )
                }
                val tanks = gasEls.mapIndexed { i, g ->
                    val work = press(g.childDouble("workingPressure")) ?: 0.0
                    val size = g.childDouble("tankSize") ?: 0.0
                    ImportedTank(
                        gasmix = i,
                        // Imperial tank sizes are a cu ft gas rating, not a
                        // water volume, so they need the working pressure.
                        volume = if (imperial) cuftToLitres(size, work) else size,
                        workpressure = work,
                        start = press(g.childDouble("pressureStart")) ?: 0.0,
                        end = press(g.childDouble("pressureEnd")) ?: 0.0
                    )
                }

                val samples = dive.descendants("sample").map { s ->
                    val p = press(s.childDouble("pressure"))
                    ImportedSample(
                        timeMs = (s.childInt("time") ?: 0) * 1000,
                        depth = depth(s.childDouble("depth")) ?: 0.0,
                        temp = temp(s.childDouble("temperature")) ?: 0.0,
                        pressureTank = if (p != null && tanks.isNotEmpty()) 0 else -1,
                        pressureValue = p ?: 0.0
                    )
                }

                val site = dive.children("site").firstOrNull()
                val siteName = listOfNotNull(
                    site?.childText("name"),
                    site?.childText("location")
                ).firstOrNull()

                dives.add(
                    ImportedDive(
                        date = stamp.date,
                        time = stamp.time,
                        maxdepth = depth(dive.childDouble("maxDepth"))
                            ?: samples.maxOfOrNull { it.depth } ?: 0.0,
                        duration = dive.childInt("duration")
                            ?: samples.maxOfOrNull { it.timeMs / 1000 } ?: 0,
                        avgdepth = depth(dive.childDouble("averageDepth")) ?: 0.0,
                        tempMax = temp(dive.childDouble("tempHigh")),
                        tempMin = temp(dive.childDouble("tempLow")),
                        salinityType = when (site?.childText("waterType")?.lowercase()) {
                            "saltwater", "salt" -> 1
                            "freshwater", "fresh" -> 0
                            else -> null
                        },
                        gasmixes = gases,
                        tanks = tanks,
                        samples = samples,
                        siteName = siteName,
                        siteLat = site?.childDouble("latitude"),
                        siteLon = site?.childDouble("longitude"),
                        notes = dive.childText("notes")
                    )
                )
            } catch (e: Exception) {
                warnings.add("Skipped a dive: ${e.message}")
            }
        }
        return ImportResult(dives, warnings, name)
    }
}
