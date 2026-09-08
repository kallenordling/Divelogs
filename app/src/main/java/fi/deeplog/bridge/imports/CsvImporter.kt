package fi.deeplog.bridge.imports

/**
 * CSV, in the two shapes Subsurface and most logbooks export:
 *
 *  - a **dive list**, one row per dive
 *    (`dive number,date,time,duration [min],maxdepth [m],...`)
 *  - a **profile**, one row per sample, grouped by the dive it belongs to
 *    (`dive number,date,time,sample time (min),sample depth (m),...`)
 *
 * Both carry their units in the header (`[m]`, `[bar]`, `(C)`), so the values
 * are read according to whatever the exporter chose rather than an assumption.
 */
class CsvImporter : DiveImporter {

    override val name = "CSV"

    override fun sniff(bytes: ByteArray, filename: String): Boolean {
        val head = String(bytes, 0, minOf(bytes.size, 4096), Charsets.UTF_8)
        val firstLine = head.lineSequence().firstOrNull()?.lowercase() ?: return false
        val sep = firstLine.count { it == ',' || it == ';' || it == '\t' }
        return sep >= 2 && (firstLine.contains("date") || firstLine.contains("dive"))
    }

    override fun parse(bytes: ByteArray): ImportResult {
        val rows = readRows(String(bytes, Charsets.UTF_8))
        if (rows.size < 2) return ImportResult(emptyList(), listOf("No data rows"), name)

        val header = rows[0].map(::normalise)
        val body = rows.drop(1).filter { r -> r.any { it.isNotBlank() } }

        return if (header.any { it.startsWith("sample ") })
            ImportResult(parseProfileRows(header, body), emptyList(), "$name (profile)")
        else
            parseDiveRows(header, body)
    }

    // ── Header handling ───────────────────────────────────────────────────────

    /**
     * "maxdepth [m]" -> "maxdepth", "sample temperature (C)" -> "sample
     * temperature".
     *
     * A trailing parenthesised *number* is kept: Subsurface numbers repeated
     * cylinder columns that way ("startpressure (2) [bar]"), and dropping it
     * would collapse every cylinder onto the first.
     */
    private fun normalise(h: String): String {
        var s = h.trim().trim('"').lowercase()
        while (true) {
            val m = Regex("\\s*[\\[(]([^\\[(\\])]*)[\\])]\\s*$").find(s) ?: break
            if (m.groupValues[1].trim().toIntOrNull() != null) break   // a column index
            s = s.substring(0, m.range.first)
        }
        return s.replace(Regex("\\s+"), " ").trim()
    }

    private class Cols(val header: List<String>) {
        fun index(vararg names: String): Int =
            names.firstNotNullOfOrNull { n ->
                header.indexOf(n).takeIf { it >= 0 }
            } ?: -1

        /** Index of a column matching a regex, e.g. cylinder size (2). */
        fun indexMatching(re: Regex): Int = header.indexOfFirst { re.matches(it) }
    }

    // ── Dive list ─────────────────────────────────────────────────────────────

    private fun parseDiveRows(header: List<String>, body: List<List<String>>): ImportResult {
        val warnings = mutableListOf<String>()
        val cols = Cols(header)

        val iDate = cols.index("date")
        val iTime = cols.index("time")
        if (iDate < 0) return ImportResult(emptyList(), listOf("No 'date' column"), name)

        val iDur = cols.index("duration")
        val iMax = cols.index("maxdepth", "max depth", "depth")
        val iAvg = cols.index("avgdepth", "avg depth", "mean depth")
        val iMode = cols.index("mode", "divemode")
        val iAir = cols.index("airtemp", "air temp")
        val iWater = cols.index("watertemp", "water temp")
        val iLoc = cols.index("location", "site", "dive site")
        val iGps = cols.index("gps")
        val iNotes = cols.index("notes", "comment")

        // Cylinder columns repeat as "cylinder size (1)", "o2 (1)", ...
        val cylinders = (1..12).mapNotNull { n ->
            val size = cols.indexMatching(Regex("cylinder size \\($n\\)"))
            val o2 = cols.indexMatching(Regex("o2 \\($n\\)"))
            val start = cols.indexMatching(Regex("startpressure \\($n\\)"))
            val end = cols.indexMatching(Regex("endpressure \\($n\\)"))
            val he = cols.indexMatching(Regex("he \\($n\\)"))
            if (size < 0 && o2 < 0 && start < 0) null
            else Quint(size, o2, he, start, end)
        }

        val dives = mutableListOf<ImportedDive>()
        for ((n, row) in body.withIndex()) {
            fun cell(i: Int): String? = row.getOrNull(i)?.trim()?.trim('"')?.takeIf { it.isNotEmpty() }
            fun num(i: Int): Double? = ImportUtil.number(cell(i))

            val stamp = ImportUtil.stamp(
                listOfNotNull(cell(iDate), cell(iTime)).joinToString(" ")
            )
            if (stamp == null) { warnings.add("Row ${n + 2}: unreadable date"); continue }

            val gases = mutableListOf<ImportedGas>()
            val tanks = mutableListOf<ImportedTank>()
            for (c in cylinders) {
                val o2 = num(c.o2)
                val size = num(c.size)
                val start = num(c.start)
                if (o2 == null && size == null && start == null) continue
                gases.add(ImportedGas(o2 = o2 ?: 21.0, he = num(c.he) ?: 0.0))
                tanks.add(
                    ImportedTank(
                        gasmix = gases.size - 1,
                        volume = size ?: 0.0,
                        start = start ?: 0.0,
                        end = num(c.end) ?: 0.0
                    )
                )
            }

            val gps = cell(iGps)?.split(Regex("[\\s,]+"))?.mapNotNull { it.toDoubleOrNull() }

            dives.add(
                ImportedDive(
                    date = stamp.date,
                    time = stamp.time,
                    maxdepth = num(iMax) ?: 0.0,
                    duration = ImportUtil.duration(cell(iDur)) ?: 0,
                    avgdepth = num(iAvg) ?: 0.0,
                    divemode = cell(iMode)?.uppercase() ?: "OC",
                    tempSurface = num(iAir),
                    tempMin = num(iWater),
                    gasmixes = gases,
                    tanks = tanks,
                    siteName = cell(iLoc),
                    siteLat = gps?.getOrNull(0),
                    siteLon = gps?.getOrNull(1),
                    notes = cell(iNotes)
                )
            )
        }
        return ImportResult(dives, warnings, name)
    }

    private class Quint(val size: Int, val o2: Int, val he: Int, val start: Int, val end: Int)

    // ── Profile rows ──────────────────────────────────────────────────────────

    private fun parseProfileRows(header: List<String>, body: List<List<String>>): List<ImportedDive> {
        val cols = Cols(header)
        val iDate = cols.index("date")
        val iTime = cols.index("time")
        val iSTime = cols.index("sample time")
        val iSDepth = cols.index("sample depth")
        val iSTemp = cols.index("sample temperature")
        val iSPress = cols.index("sample pressure")
        val iSHr = cols.index("sample heartrate", "sample heartbeat")

        // Rows are grouped by the dive they belong to.
        val grouped = LinkedHashMap<String, MutableList<ImportedSample>>()
        for (row in body) {
            fun cell(i: Int): String? = row.getOrNull(i)?.trim()?.trim('"')?.takeIf { it.isNotEmpty() }
            val key = "${cell(iDate)} ${cell(iTime)}"
            val pressure = ImportUtil.number(cell(iSPress))
            grouped.getOrPut(key) { mutableListOf() }.add(
                ImportedSample(
                    timeMs = (ImportUtil.duration(cell(iSTime)) ?: 0) * 1000,
                    depth = ImportUtil.number(cell(iSDepth)) ?: 0.0,
                    temp = ImportUtil.number(cell(iSTemp)) ?: 0.0,
                    pressureTank = if (pressure != null) 0 else -1,
                    pressureValue = pressure ?: 0.0,
                    heartbeat = ImportUtil.number(cell(iSHr))?.toInt() ?: 0
                )
            )
        }

        return grouped.mapNotNull { (key, samples) ->
            val stamp = ImportUtil.stamp(key.trim()) ?: return@mapNotNull null
            ImportedDive(
                date = stamp.date,
                time = stamp.time,
                maxdepth = samples.maxOfOrNull { it.depth } ?: 0.0,
                duration = samples.maxOfOrNull { it.timeMs / 1000 } ?: 0,
                samples = samples
            )
        }
    }

    // ── RFC 4180 reader ───────────────────────────────────────────────────────

    private fun readRows(text: String): List<List<String>> {
        val sep = detectSeparator(text)
        val rows = mutableListOf<List<String>>()
        var row = mutableListOf<String>()
        val field = StringBuilder()
        var quoted = false
        var i = 0
        while (i < text.length) {
            val c = text[i]
            when {
                quoted && c == '"' && i + 1 < text.length && text[i + 1] == '"' -> {
                    field.append('"'); i++
                }
                c == '"' -> quoted = !quoted
                !quoted && c == sep -> { row.add(field.toString()); field.setLength(0) }
                !quoted && (c == '\n' || c == '\r') -> {
                    if (field.isNotEmpty() || row.isNotEmpty()) {
                        row.add(field.toString()); field.setLength(0)
                        rows.add(row); row = mutableListOf()
                    }
                    if (c == '\r' && i + 1 < text.length && text[i + 1] == '\n') i++
                }
                else -> field.append(c)
            }
            i++
        }
        if (field.isNotEmpty() || row.isNotEmpty()) { row.add(field.toString()); rows.add(row) }
        return rows
    }

    private fun detectSeparator(text: String): Char {
        val first = text.lineSequence().firstOrNull() ?: return ','
        return listOf(',', ';', '\t').maxByOrNull { sep -> first.count { it == sep } } ?: ','
    }
}
