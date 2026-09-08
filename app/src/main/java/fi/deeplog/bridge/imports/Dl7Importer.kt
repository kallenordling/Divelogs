package fi.deeplog.bridge.imports

/**
 * DAN DL7 (.zxu / .zxl), the interchange format written by DiverLog+ and the
 * Aqualung/Oceanic desktop apps.
 *
 * The file is a sequence of `|`-delimited segments:
 *   FSH  file header
 *   ZRH  record header, carries the unit system
 *   ZAR{ }  optional vendor block (Aqualung writes XML-ish tags in here that
 *           hold the tanks, the gas and the dive site — nothing else does)
 *   ZDH  dive header: start time and air temperature
 *   ZDP{ }  the profile, one row per sample
 *   ZDT  dive trailer: max depth and end time
 *
 * A file may hold several ZDH/ZDP/ZDT groups, one per dive.
 */
class Dl7Importer : DiveImporter {

    override val name = "DAN DL7"

    override fun sniff(bytes: ByteArray, filename: String): Boolean {
        val head = String(bytes, 0, minOf(bytes.size, 512), Charsets.UTF_8)
        return head.startsWith("FSH|") || head.contains("\nZRH|")
    }

    override fun parse(bytes: ByteArray): ImportResult {
        val lines = String(bytes, Charsets.UTF_8).split(Regex("\r?\n"))
        val warnings = mutableListOf<String>()
        val dives = mutableListOf<ImportedDive>()

        // Units default to metric; ZRH says otherwise for imperial exports.
        var feet = false
        var fahrenheit = false

        var vendor: VendorBlock? = null
        var header: List<String>? = null
        var samples = mutableListOf<ImportedSample>()
        var inProfile = false
        var inVendor = false
        val vendorLines = mutableListOf<String>()

        fun flush(trailer: List<String>?) {
            val h = header ?: return
            val stamp = ImportUtil.stamp(h.getOrNull(5))
            if (stamp == null) {
                warnings.add("Skipped a dive with an unreadable start time")
            } else {
                dives.add(buildDive(stamp, h, trailer, samples.toList(), vendor, feet, fahrenheit))
            }
            header = null
            samples = mutableListOf()
        }

        for (raw in lines) {
            val line = raw.trim()
            if (line.isEmpty()) continue

            when {
                inVendor -> {
                    if (line == "}") {
                        inVendor = false
                        vendor = parseVendorBlock(vendorLines)
                        vendorLines.clear()
                    } else vendorLines.add(line)
                }

                inProfile -> {
                    if (line.startsWith("ZDP}")) { inProfile = false; continue }
                    parseProfileRow(line, feet, fahrenheit)?.let { samples.add(it) }
                }

                line.startsWith("ZRH|") -> {
                    val f = line.split('|')
                    // The unit tokens sit in fixed columns in the spec but
                    // vendors shuffle them, so match on the token values.
                    feet = f.any { it.equals("FT", true) }
                    fahrenheit = f.any { it.equals("F", true) }
                }

                line.startsWith("ZAR{") -> { inVendor = true; vendorLines.clear() }
                line.startsWith("ZDP{") -> inProfile = true
                line.startsWith("ZDH|") -> { flush(null); header = line.split('|') }
                line.startsWith("ZDT|") -> flush(line.split('|'))
            }
        }
        flush(null)  // a file whose last dive has no ZDT trailer

        return ImportResult(dives, warnings, name)
    }

    // ── Profile ───────────────────────────────────────────────────────────────

    /**
     * A profile row is `|time|depth|...|temperature|...`, with time in decimal
     * minutes. Columns beyond depth vary by vendor; temperature is the only
     * other one written consistently enough to trust.
     */
    private fun parseProfileRow(line: String, feet: Boolean, fahrenheit: Boolean): ImportedSample? {
        if (!line.startsWith("|")) return null
        val f = line.split('|')
        val minutes = f.getOrNull(1)?.trim()?.toDoubleOrNull() ?: return null
        val depth = f.getOrNull(2)?.trim()?.toDoubleOrNull() ?: return null
        val temp = f.getOrNull(8)?.trim()?.toDoubleOrNull()
        return ImportedSample(
            timeMs = (minutes * 60_000).toInt(),
            depth = if (feet) ImportUtil.feetToMetres(depth) else depth,
            temp = temp?.let { if (fahrenheit) ImportUtil.fahrenheitToCelsius(it) else it } ?: 0.0
        )
    }

    private fun buildDive(
        stamp: ImportUtil.Stamp,
        header: List<String>,
        trailer: List<String>?,
        samples: List<ImportedSample>,
        vendor: VendorBlock?,
        feet: Boolean,
        fahrenheit: Boolean
    ): ImportedDive {
        val airTemp = header.getOrNull(6)?.trim()?.toDoubleOrNull()
            ?.let { if (fahrenheit) ImportUtil.fahrenheitToCelsius(it) else it }

        val trailerDepth = trailer?.getOrNull(3)?.trim()?.toDoubleOrNull()
            ?.let { if (feet) ImportUtil.feetToMetres(it) else it }

        val duration = vendor?.durationSeconds
            ?: samples.maxOfOrNull { it.timeMs / 1000 }
            ?: 0

        // The profile's own minimum beats the trailer's temperature field,
        // which several exporters leave at a placeholder.
        val profileMin = samples.mapNotNull { it.temp.takeIf { t -> t != 0.0 } }.minOrNull()

        return ImportedDive(
            date = stamp.date,
            time = stamp.time,
            maxdepth = vendor?.maxDepth ?: trailerDepth ?: samples.maxOfOrNull { it.depth } ?: 0.0,
            duration = duration,
            tempSurface = airTemp,
            tempMin = vendor?.minTemp ?: profileMin,
            gasmixes = vendor?.gases.orEmpty(),
            tanks = vendor?.tanks.orEmpty(),
            samples = samples,
            siteName = vendor?.siteName,
            siteLat = vendor?.lat,
            siteLon = vendor?.lon,
            notes = vendor?.title
        )
    }

    // ── ZAR vendor block ──────────────────────────────────────────────────────

    private class VendorBlock(
        val title: String?,
        val siteName: String?,
        val lat: Double?,
        val lon: Double?,
        val minTemp: Double?,
        val maxDepth: Double?,
        val durationSeconds: Int?,
        val gases: List<ImportedGas>,
        val tanks: List<ImportedTank>
    )

    /** `<TAG>value</TAG>` lines, with comma-separated `KEY=value` payloads. */
    private fun parseVendorBlock(lines: List<String>): VendorBlock? {
        val text = lines.joinToString("\n")
        if (!text.contains("<AQUALUNG>")) return null   // only Aqualung's is documented

        fun tag(name: String): String? =
            Regex("<$name>(.*?)</$name>", RegexOption.DOT_MATCHES_ALL)
                .find(text)?.groupValues?.get(1)?.trim()

        // "KEY=value" pairs, where a value may be bracketed: "LOCNAME=[Molokini]".
        fun fields(payload: String?): Map<String, String> {
            if (payload == null) return emptyMap()
            val out = LinkedHashMap<String, String>()
            Regex("([A-Z0-9_/]+)=(\\[[^\\]]*\\]|[^,]*)").findAll(payload).forEach { m ->
                out[m.groupValues[1]] = m.groupValues[2].trim().removeSurrounding("[", "]")
            }
            return out
        }

        val location = fields(tag("LOCATION"))
        val stats = fields(tag("DIVESTATS"))

        val gps = location["GPS"]?.split(',')?.mapNotNull { it.trim().toDoubleOrNull() }

        // Several TANK tags can appear, one per cylinder.
        val gases = mutableListOf<ImportedGas>()
        val tanks = mutableListOf<ImportedTank>()
        Regex("<TANK>(.*?)</TANK>", RegexOption.DOT_MATCHES_ALL).findAll(text).forEach { m ->
            val t = fields(m.groupValues[1])
            val sizeRaw = ImportUtil.number(t["CYLSIZE"]) ?: 0.0
            val cuft = t["CYLSIZE"]?.contains("CU FT", true) == true

            // The vendor block states its own units on WORKINGPRESSURE and
            // CYLSIZE, and START/ENDPRESSURE are bare numbers in that same
            // unit -- not in whatever ZRH declared for the standard segments.
            val tankPsi = t["WORKINGPRESSURE"]?.contains("PSI", true) == true || cuft
            fun p(v: String?) = ImportUtil.number(v)
                ?.let { if (tankPsi) ImportUtil.psiToBar(it) else it } ?: 0.0

            val work = p(t["WORKINGPRESSURE"])

            gases.add(ImportedGas(o2 = t["FO2"]?.toDoubleOrNull() ?: 21.0))
            tanks.add(
                ImportedTank(
                    gasmix = gases.size - 1,
                    volume = if (cuft) ImportUtil.cuftToLitres(sizeRaw, work) else sizeRaw,
                    workpressure = work,
                    start = p(t["STARTPRESSURE"]),
                    end = p(t["ENDPRESSURE"])
                )
            )
        }

        // EDT is elapsed dive time as HHMMSS.
        val duration = stats["EDT"]?.takeIf { it.length == 6 }?.let {
            val h = it.substring(0, 2).toIntOrNull() ?: return@let null
            val mi = it.substring(2, 4).toIntOrNull() ?: return@let null
            val s = it.substring(4, 6).toIntOrNull() ?: return@let null
            h * 3600 + mi * 60 + s
        }

        return VendorBlock(
            title = tag("TITLE"),
            siteName = location["LOCNAME"],
            lat = gps?.getOrNull(0),
            lon = gps?.getOrNull(1),
            minTemp = (stats["MINTEMP"] ?: location["MINTEMP"])?.toDoubleOrNull(),
            maxDepth = stats["MAXDEPTH"]?.toDoubleOrNull(),
            durationSeconds = duration,
            gases = gases,
            tanks = tanks
        )
    }
}
