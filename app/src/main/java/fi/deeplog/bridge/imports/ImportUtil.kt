package fi.deeplog.bridge.imports

import org.w3c.dom.Document
import org.w3c.dom.Element
import org.w3c.dom.Node
import java.io.ByteArrayInputStream
import java.util.Locale
import javax.xml.parsers.DocumentBuilderFactory

/** Parsing helpers shared by the import formats. */
object ImportUtil {

    // ── XML ───────────────────────────────────────────────────────────────────

    fun parseXml(bytes: ByteArray): Document {
        val f = DocumentBuilderFactory.newInstance().apply {
            isNamespaceAware = false
            // Imported files come from outside the app. MacDive's doctype
            // points at a URL on its own website, so without this the parser
            // reaches out over the network and fails when it 403s.
            setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false)
            setFeature("http://xml.org/sax/features/external-general-entities", false)
            setFeature("http://xml.org/sax/features/external-parameter-entities", false)
            isExpandEntityReferences = false
            isValidating = false
        }
        val builder = f.newDocumentBuilder()
        // Belt and braces: resolve any remaining external reference to nothing.
        builder.setEntityResolver { _, _ -> org.xml.sax.InputSource(java.io.StringReader("")) }
        return builder.parse(ByteArrayInputStream(bytes))
    }

    fun Element.children(tag: String): List<Element> {
        val out = mutableListOf<Element>()
        var n: Node? = firstChild
        while (n != null) {
            if (n is Element && n.tagName.equals(tag, ignoreCase = true)) out.add(n)
            n = n.nextSibling
        }
        return out
    }

    /** Every descendant with this tag name, at any depth. */
    fun Element.descendants(tag: String): List<Element> {
        val nl = getElementsByTagName(tag)
        return (0 until nl.length).mapNotNull { nl.item(it) as? Element }
    }

    /** Text of the first direct child with this tag, or null. */
    fun Element.childText(tag: String): String? =
        children(tag).firstOrNull()?.textContent?.trim()?.takeIf { it.isNotEmpty() }

    fun Element.childDouble(tag: String): Double? = childText(tag)?.toDoubleOrNull()
    fun Element.childInt(tag: String): Int? = childText(tag)?.trim()?.toDoubleOrNull()?.toInt()

    fun Element.attr(name: String): String? =
        getAttribute(name).trim().takeIf { it.isNotEmpty() }

    // ── Numbers ───────────────────────────────────────────────────────────────

    /**
     * First number in a unit-suffixed string, e.g. "30.2 m" -> 30.2, "1%" -> 1.0.
     * Subsurface writes every measurement this way.
     */
    fun number(s: String?): Double? {
        if (s.isNullOrBlank()) return null
        val m = Regex("-?\\d+(?:\\.\\d+)?").find(s) ?: return null
        return m.value.toDoubleOrNull()
    }

    /**
     * Subsurface durations: "45:00 min" (mm:ss) or "1:05:00" (h:mm:ss) or plain
     * seconds. Returns seconds.
     */
    fun duration(s: String?): Int? {
        if (s.isNullOrBlank()) return null
        val core = s.trim().substringBefore(' ')
        val parts = core.split(':')
        return when (parts.size) {
            1 -> parts[0].toDoubleOrNull()?.toInt()
            2 -> {
                val min = parts[0].toDoubleOrNull() ?: return null
                val sec = parts[1].toDoubleOrNull() ?: return null
                (min * 60 + sec).toInt()
            }
            3 -> {
                val h = parts[0].toDoubleOrNull() ?: return null
                val m = parts[1].toDoubleOrNull() ?: return null
                val sec = parts[2].toDoubleOrNull() ?: return null
                (h * 3600 + m * 60 + sec).toInt()
            }
            else -> null
        }
    }

    // ── Units ─────────────────────────────────────────────────────────────────

    fun kelvinToCelsius(k: Double) = k - 273.15
    fun fahrenheitToCelsius(f: Double) = (f - 32.0) * 5.0 / 9.0
    fun feetToMetres(ft: Double) = ft * 0.3048
    fun psiToBar(psi: Double) = psi * 0.0689475729
    fun cuftToLitres(cuft: Double, workPressureBar: Double): Double =
        // A cylinder's "cu ft" rating is its free-gas capacity at working
        // pressure, so water volume = cuft * 28.3168 / (workpressure in bar).
        if (workPressureBar > 0) cuft * 28.3168466 / workPressureBar else 0.0

    // ── Date / time ───────────────────────────────────────────────────────────

    /** "YYYY-MM-DD" and "HH:MM" as the dive JSON wants them. */
    data class Stamp(val date: String, val time: String)

    /**
     * Accepts ISO 8601 ("2024-06-01T09:00:00Z", with or without offset),
     * "2024-06-01 09:00:00", and compact "20240601090000".
     */
    fun stamp(raw: String?): Stamp? {
        if (raw.isNullOrBlank()) return null
        val s = raw.trim()

        Regex("^(\\d{4})(\\d{2})(\\d{2})(\\d{2})(\\d{2})").find(s)?.let { m ->
            val (y, mo, d, h, mi) = m.destructured
            return Stamp("$y-$mo-$d", "$h:$mi")
        }
        Regex("^(\\d{4})-(\\d{2})-(\\d{2})[T ](\\d{2}):(\\d{2})").find(s)?.let { m ->
            val (y, mo, d, h, mi) = m.destructured
            return Stamp("$y-$mo-$d", "$h:$mi")
        }
        // Date only.
        Regex("^(\\d{4})-(\\d{2})-(\\d{2})").find(s)?.let { m ->
            val (y, mo, d) = m.destructured
            return Stamp("$y-$mo-$d", "00:00")
        }
        return null
    }

    /** Epoch seconds (UTC) to a local-time stamp. */
    fun stampFromEpoch(epochSeconds: Long): Stamp {
        val c = java.util.Calendar.getInstance()
        c.timeInMillis = epochSeconds * 1000L
        return Stamp(
            String.format(Locale.US, "%04d-%02d-%02d",
                c.get(java.util.Calendar.YEAR),
                c.get(java.util.Calendar.MONTH) + 1,
                c.get(java.util.Calendar.DAY_OF_MONTH)),
            String.format(Locale.US, "%02d:%02d",
                c.get(java.util.Calendar.HOUR_OF_DAY),
                c.get(java.util.Calendar.MINUTE))
        )
    }
}
