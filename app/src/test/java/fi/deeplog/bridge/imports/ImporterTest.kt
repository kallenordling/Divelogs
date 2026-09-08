package fi.deeplog.bridge.imports

import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class ImporterTest {

    private fun fixture(name: String): ByteArray =
        javaClass.classLoader!!.getResourceAsStream(name)!!.readBytes()

    // ── MacDive ───────────────────────────────────────────────────────────────

    @Test fun macdiveMetric() {
        val r = MacDiveImporter().parse(fixture("macdive_metric.xml"))
        assertEquals(1, r.dives.size)
        val d = r.dives[0]
        assertEquals("2024-06-01", d.date)
        assertEquals("09:00", d.time)
        assertEquals(25.40, d.maxdepth, 0.001)
        assertEquals(18.00, d.avgdepth, 0.001)
        assertEquals(2400, d.duration)
        assertEquals(26.5, d.tempMax!!, 0.001)
        assertEquals(20.0, d.tempMin!!, 0.001)
        assertEquals("Test Reef", d.siteName)
        assertEquals(1, d.salinityType)          // saltwater
        assertEquals(3, d.samples.size)
        assertEquals(60_000, d.samples[1].timeMs)
        assertEquals(10.0, d.samples[1].depth, 0.001)
        assertEquals(195.0, d.samples[1].pressureValue, 0.001)
        assertEquals(1, d.gasmixes.size)
        assertEquals(32.0, d.gasmixes[0].o2, 0.001)
    }

    @Test fun macdiveImperialConvertsUnits() {
        val r = MacDiveImporter().parse(fixture("macdive_imperial.xml"))
        val d = r.dives[0]
        // 100 ft, 80 F / 70 F, 3000 psi
        assertEquals(30.48, d.maxdepth, 0.01)
        assertEquals(26.667, d.tempMax!!, 0.01)
        assertEquals(21.111, d.tempMin!!, 0.01)
        assertEquals(206.84, d.tanks[0].workpressure, 0.1)
        assertEquals(206.84, d.tanks[0].start, 0.1)
        assertEquals(68.95, d.tanks[0].end, 0.1)
        // AL80: 77.4 cu ft at 3000 psi ≈ 10.6 L water volume
        assertEquals(10.6, d.tanks[0].volume, 0.2)
        assertEquals(30.48, d.samples[1].depth, 0.01)
    }

    // ── DAN DL7 ───────────────────────────────────────────────────────────────

    @Test fun dl7WithAqualungVendorBlock() {
        val r = Dl7Importer().parse(fixture("dl7_sample.zxu"))
        assertEquals(1, r.dives.size)
        val d = r.dives[0]
        assertEquals("2024-06-12", d.date)
        assertEquals("09:30", d.time)
        assertEquals(18.288, d.maxdepth, 0.001)   // DIVESTATS MAXDEPTH
        assertEquals(360, d.duration)             // EDT 000600
        assertEquals(28.0, d.tempSurface!!, 0.001)
        assertEquals(26.5, d.tempMin!!, 0.001)    // DIVESTATS MINTEMP
        assertEquals("Molokini Crater", d.siteName)
        assertEquals(20.877432, d.siteLat!!, 1e-6)
        assertEquals(-156.679867, d.siteLon!!, 1e-6)
        assertEquals("Morning Reef Drift", d.notes)

        // 12 profile rows; time is decimal minutes.
        assertEquals(12, d.samples.size)
        assertEquals(0, d.samples[0].timeMs)
        assertEquals(30_000, d.samples[1].timeMs)
        assertEquals(330_000, d.samples[11].timeMs)
        assertEquals(5.2, d.samples[1].depth, 0.001)
        assertEquals(27.0, d.samples[2].temp, 0.001)

        // One tank from the vendor block: AL80, 80 cu ft at 3000 psi, EAN32.
        assertEquals(1, d.tanks.size)
        assertEquals(32.0, d.gasmixes[0].o2, 0.001)
        assertEquals(206.84, d.tanks[0].workpressure, 0.1)
        assertEquals(206.84, d.tanks[0].start, 0.1)
        assertEquals(124.11, d.tanks[0].end, 0.1)
    }

    // ── UDDF ──────────────────────────────────────────────────────────────────

    @Test fun uddfTrimixDive() {
        val r = UddfImporter().parse(fixture("sample.uddf"))
        assertTrue("expected at least one dive", r.dives.isNotEmpty())
        val d = r.dives[0]
        assertTrue("date looks wrong: ${d.date}", Regex("\\d{4}-\\d{2}-\\d{2}").matches(d.date))
        assertTrue("no samples", d.samples.isNotEmpty())
        assertTrue("max depth not positive", d.maxdepth > 0)
        assertTrue("expected several gases", d.gasmixes.size >= 2)
        // Temperatures come out of Kelvin into a plausible water range.
        val temps = d.samples.map { it.temp }.filter { it != 0.0 }
        assertTrue("temps out of range: ${temps.take(3)}", temps.all { it > -5 && it < 45 })
    }

    // ── CSV ───────────────────────────────────────────────────────────────────

    @Test fun csvDiveList() {
        val r = CsvImporter().parse(fixture("subsurface_dive_list.csv"))
        assertTrue(r.dives.size >= 2)
        val d = r.dives.first { it.date == "2025-09-20" }
        assertEquals("07:44", d.time)
        assertEquals(2.41, d.maxdepth, 0.001)
        assertEquals(1.58, d.avgdepth, 0.001)
        assertEquals(42, d.duration)              // "0:42"
        assertEquals(21.0, d.tempMin!!, 0.001)
        assertEquals("Maclearie Park", d.siteName)
        assertEquals(40.179575, d.siteLat!!, 1e-6)
        assertEquals(-74.037466, d.siteLon!!, 1e-6)
        assertEquals(11.094, d.tanks[0].volume, 0.001)
        assertEquals(196.9, d.tanks[0].start, 0.001)
    }

    @Test fun csvProfile() {
        val r = CsvImporter().parse(fixture("subsurface_profile.csv"))
        assertTrue(r.dives.isNotEmpty())
        val d = r.dives[0]
        assertEquals("2018-05-22", d.date)
        assertEquals("14:23", d.time)
        assertTrue(d.samples.size >= 2)
        assertEquals(10_000, d.samples[0].timeMs)
        assertEquals(3.444, d.samples[1].depth, 0.001)
        assertEquals(25.0, d.samples[0].temp, 0.001)
    }

    // ── Subsurface XML ────────────────────────────────────────────────────────

    @Test fun subsurfaceExport() {
        val r = SubsurfaceImporter().parse(fixture("subsurface_export.ssrf"))
        assertTrue("no dives", r.dives.isNotEmpty())
        val d = r.dives.first { it.date == "2025-09-20" && it.time == "07:44" }
        assertEquals(2.41, d.maxdepth, 0.001)
        assertEquals(1.584, d.avgdepth, 0.001)
        assertEquals(42, d.duration)
        assertEquals(21.111, d.tempSurface!!, 0.001)
        assertEquals(21.0, d.tempMin!!, 0.001)
        assertEquals("Maclearie Park", d.siteName)
        assertEquals(1, d.salinityType)                 // 1030 g/l -> salt

        // Six <cylinder> slots, only the first is real.
        assertEquals(1, d.tanks.size)
        assertEquals(11.094, d.tanks[0].volume, 0.001)
        assertEquals(206.843, d.tanks[0].workpressure, 0.001)
        assertEquals(1, d.gasmixes.size)

        // Samples are differential; depth and temp must carry forward rather
        // than collapsing to zero when the attribute is absent.
        assertTrue(d.samples.size > 5)
        assertEquals(0, d.samples[0].timeMs)
        assertEquals(2_000, d.samples[1].timeMs)   // "0:02 min" is mm:ss
        assertEquals(0.37, d.samples[1].depth, 0.001)
        assertEquals(21.0, d.samples[1].temp, 0.001)    // carried from sample 0
        assertTrue("no zero-temp holes", d.samples.none { it.temp == 0.0 })
        // pressure0 carries forward too, and lands on tank 0.
        assertEquals(196.9, d.samples[0].pressureValue, 0.001)
        assertEquals(0, d.samples[0].pressureTank)
        assertEquals(196.9, d.samples[1].pressureValue, 0.001)
    }

    // ── Garmin FIT ────────────────────────────────────────────────────────────

    @Test fun fitDive() {
        val r = FitImporter().parse(fixture("sample.fit"))
        assertEquals(1, r.dives.size)
        val d = r.dives[0]
        assertTrue("bad date ${d.date}", Regex("\\d{4}-\\d{2}-\\d{2}").matches(d.date))
        assertTrue("max depth not positive: ${d.maxdepth}", d.maxdepth > 0)
        assertTrue("no samples", d.samples.size > 10)
        assertTrue("duration not positive", d.duration > 0)
        // Sample times start at zero and increase.
        assertEquals(0, d.samples[0].timeMs)
        assertTrue(d.samples[1].timeMs > 0)
        assertTrue("depths implausible",
            d.samples.all { it.depth >= 0 && it.depth < 400 })
        assertTrue("temps implausible",
            d.samples.map { it.temp }.filter { it != 0.0 }.all { it > -5 && it < 45 })
    }

    // ── Sniffing ──────────────────────────────────────────────────────────────

    @Test fun sniffPicksTheRightFormat() {
        val cases = listOf(
            "macdive_metric.xml" to "MacDive XML",
            "dl7_sample.zxu" to "DAN DL7",
            "sample.uddf" to "UDDF",
            "subsurface_dive_list.csv" to "CSV",
            "subsurface_export.ssrf" to "Subsurface XML",
            "sample.fit" to "Garmin FIT"
        )
        for ((file, expected) in cases) {
            val picked = ImportRegistry.importerFor(fixture(file), file)
            assertEquals("wrong importer for $file", expected, picked?.name)
        }
    }
}
