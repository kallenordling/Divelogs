package fi.deeplog.bridge

import org.json.JSONObject
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertTrue
import org.junit.Test
import java.io.File

/**
 * Checks the bundled catalogue asset itself. [SiteCatalog] needs an Android
 * Context to read it, so these tests read the same file straight off disk and
 * assert on its contents.
 */
class SiteCatalogDataTest {

    private val doc: JSONObject by lazy {
        JSONObject(File("src/main/assets/dive_sites.json").readText())
    }

    @Test fun everySiteHasNameAndUsableCoordinates() {
        val sites = doc.getJSONArray("sites")
        assertTrue("catalogue is suspiciously small: ${sites.length()}", sites.length() > 3000)

        for (i in 0 until sites.length()) {
            val s = sites.getJSONObject(i)
            val name = s.optString("name")
            assertTrue("entry $i has no name", name.isNotBlank())

            // A site with no coordinates cannot be placed on the map, and the
            // upstream extract had 356 of them.
            assertTrue("$name has no lat", s.has("lat"))
            assertTrue("$name has no lon", s.has("lon"))
            val lat = s.getDouble("lat")
            val lon = s.getDouble("lon")
            assertTrue("$name lat out of range: $lat", lat in -90.0..90.0)
            assertTrue("$name lon out of range: $lon", lon in -180.0..180.0)
            assertTrue("$name sits at null island", lat != 0.0 || lon != 0.0)
        }
    }

    @Test fun finnishSitesArePresentAndInFinland() {
        val sites = doc.getJSONArray("sites")
        val finnish = (0 until sites.length())
            .map { sites.getJSONObject(it) }
            .filter { it.optString("country") == "Finland" }

        assertTrue("expected Finnish sites, found ${finnish.size}", finnish.size >= 13)

        for (s in finnish) {
            val lat = s.getDouble("lat")
            val lon = s.getDouble("lon")
            // Finland's bounding box, Åland included.
            assertTrue("${s.getString("name")} is not in Finland: $lat,$lon",
                lat in 59.4..70.2 && lon in 19.0..31.7)
        }

        val names = finnish.map { it.getString("name") }
        // Well-known Finnish sites: a flooded quarry, a quarry lake and the
        // Kuru wreck in Näsijärvi.
        assertTrue("missing Ojamon kaivoslampi in $names", "Ojamon kaivoslampi" in names)
        assertTrue("missing Vetokannas in $names", "Vetokannas" in names)
        assertTrue("missing Kuru in $names", "Kuru" in names)
    }

    @Test fun noDuplicateSitesAtTheSamePlace() {
        val sites = doc.getJSONArray("sites")
        val seen = HashSet<String>()
        for (i in 0 until sites.length()) {
            val s = sites.getJSONObject(i)
            val key = "%s@%.4f,%.4f".format(
                s.getString("name").trim().lowercase(),
                s.getDouble("lat"), s.getDouble("lon")
            )
            assertTrue("duplicate entry: $key", seen.add(key))
        }
    }

    @Test fun attributionIsRecorded() {
        val meta = doc.getJSONObject("metadata")
        // ODbL requires the source to travel with the data.
        assertTrue(meta.getString("source").contains("OpenStreetMap"))
        assertTrue(meta.getString("license").contains("ODbL"))
        assertNotNull(meta.getJSONArray("tags"))
        assertEquals(doc.getJSONArray("sites").length(), meta.getInt("count"))
    }
}
