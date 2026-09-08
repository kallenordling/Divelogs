package fi.deeplog.bridge.imports

import java.nio.ByteBuffer
import java.nio.ByteOrder

/**
 * Garmin FIT activity files, as written by Descent computers and served by
 * Garmin Connect.
 *
 * A FIT file is a header, then a stream of records. Each record carries a
 * one-byte header saying whether it *defines* a local message type (which
 * fields it has, their sizes and base types) or *is* one of those messages.
 * Only the definitions tell you how to read the data, so the decoder walks the
 * whole stream keeping a table of the 16 local types.
 *
 * The dive-specific global messages are:
 *   20  record        per-sample depth, temperature, heart rate
 *  259  dive_gas      the gases configured for the dive
 *  268  dive_summary  max/avg depth and bottom time
 *   18  session       start time and elapsed time
 */
class FitImporter : DiveImporter {

    override val name = "Garmin FIT"

    override fun sniff(bytes: ByteArray, filename: String): Boolean {
        if (bytes.size < 12) return false
        val headerSize = bytes[0].toInt() and 0xFF
        return headerSize >= 12 &&
            bytes[8] == '.'.code.toByte() && bytes[9] == 'F'.code.toByte() &&
            bytes[10] == 'I'.code.toByte() && bytes[11] == 'T'.code.toByte()
    }

    override fun parse(bytes: ByteArray): ImportResult =
        ImportResult(decode(bytes), emptyList(), name)

    // ── Decoder ───────────────────────────────────────────────────────────────

    /** FIT counts seconds from 1989-12-31 UTC, not the Unix epoch. */
    private val fitEpochOffset = 631_065_600L

    private class FieldDef(val num: Int, val size: Int, val baseType: Int)
    private class MsgDef(val globalNum: Int, val littleEndian: Boolean, val fields: List<FieldDef>)

    private fun decode(bytes: ByteArray): List<ImportedDive> {
        val headerSize = bytes[0].toInt() and 0xFF
        val buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
        val dataSize = buf.getInt(4).toLong() and 0xFFFFFFFFL
        var pos = headerSize
        val end = minOf(bytes.size.toLong(), headerSize + dataSize).toInt()

        val defs = HashMap<Int, MsgDef>()
        val samples = mutableListOf<ImportedSample>()
        val gases = mutableListOf<ImportedGas>()

        var startTime: Long? = null           // FIT seconds
        var elapsedSeconds: Int? = null
        var maxDepth: Double? = null
        var avgDepth: Double? = null
        var bottomTime: Int? = null
        var firstSampleTime: Long? = null
        var lat: Double? = null
        var lon: Double? = null
        var minTemp: Double? = null
        var maxTemp: Double? = null

        while (pos < end) {
            val header = bytes[pos].toInt() and 0xFF
            pos++

            // Compressed-timestamp headers carry data for an already-defined
            // local type; the time offset they encode is not needed here
            // because samples are timestamped from the record's own field.
            val isDefinition = (header and 0x80) == 0 && (header and 0x40) != 0
            val localType = if ((header and 0x80) != 0) (header shr 5) and 0x03 else header and 0x0F

            if (isDefinition) {
                if (pos + 5 > end) break
                val littleEndian = bytes[pos + 1].toInt() == 0
                val order = if (littleEndian) ByteOrder.LITTLE_ENDIAN else ByteOrder.BIG_ENDIAN
                val globalNum = ByteBuffer.wrap(bytes, pos + 2, 2).order(order).short.toInt() and 0xFFFF
                val nFields = bytes[pos + 4].toInt() and 0xFF
                pos += 5

                val fields = ArrayList<FieldDef>(nFields)
                for (i in 0 until nFields) {
                    if (pos + 3 > end) return emptyList()
                    fields.add(FieldDef(
                        bytes[pos].toInt() and 0xFF,
                        bytes[pos + 1].toInt() and 0xFF,
                        bytes[pos + 2].toInt() and 0x1F
                    ))
                    pos += 3
                }
                // Developer fields add bytes to each message but carry nothing
                // this importer reads; their sizes still have to be skipped.
                var devFields = emptyList<FieldDef>()
                if ((header and 0x20) != 0) {
                    if (pos >= end) break
                    val nDev = bytes[pos].toInt() and 0xFF
                    pos++
                    devFields = (0 until nDev).mapNotNull {
                        if (pos + 3 > end) null
                        else FieldDef(-1, bytes[pos + 1].toInt() and 0xFF, 13).also { pos += 3 }
                    }
                }
                defs[localType] = MsgDef(globalNum, littleEndian, fields + devFields)
                continue
            }

            val def = defs[localType] ?: break     // data before its definition: give up
            val order = if (def.littleEndian) ByteOrder.LITTLE_ENDIAN else ByteOrder.BIG_ENDIAN
            val values = HashMap<Int, Long>()
            for (f in def.fields) {
                if (pos + f.size > end) return emptyList()
                if (f.num >= 0) readNumeric(bytes, pos, f, order)?.let { values[f.num] = it }
                pos += f.size
            }

            when (def.globalNum) {
                18 -> {  // session
                    values[2]?.let { startTime = it }
                    values[7]?.let { elapsedSeconds = (it / 1000).toInt() }
                    values[29]?.let { lat = semicirclesToDegrees(it) }
                    values[30]?.let { lon = semicirclesToDegrees(it) }
                }
                259 -> {  // dive_gas
                    val o2 = values[1]?.toDouble()
                    if (o2 != null) gases.add(ImportedGas(o2, values[0]?.toDouble() ?: 0.0))
                }
                268 -> {  // dive_summary
                    values[2]?.let { avgDepth = it / 1000.0 }
                    values[3]?.let { maxDepth = it / 1000.0 }
                    values[11]?.let { bottomTime = (it / 1000).toInt() }
                }
                20 -> {  // record
                    // A record without a timestamp cannot be placed on the
                    // profile, so it is dropped.
                    val ts = values[253]
                    if (ts != null) {
                    if (firstSampleTime == null) firstSampleTime = ts
                    val depth = values[92]?.div(1000.0)
                    val temp = values[13]?.toDouble()
                    if (temp != null) {
                        minTemp = minOf(minTemp ?: temp, temp)
                        maxTemp = maxOf(maxTemp ?: temp, temp)
                    }
                    samples.add(
                        ImportedSample(
                            timeMs = ((ts - firstSampleTime!!) * 1000).toInt(),
                            depth = depth ?: 0.0,
                            temp = temp ?: 0.0,
                            heartbeat = values[3]?.toInt() ?: 0,
                            cns = values[96]?.toDouble() ?: 0.0
                        )
                    )
                    if (lat == null) values[0]?.let { lat = semicirclesToDegrees(it) }
                    if (lon == null) values[1]?.let { lon = semicirclesToDegrees(it) }
                    }
                }
            }
        }

        // A file with no depth anywhere is some other Garmin activity, not a dive.
        val hasDepth = samples.any { it.depth > 0 } || (maxDepth ?: 0.0) > 0
        if (!hasDepth) return emptyList()

        val epoch = (startTime ?: firstSampleTime ?: return emptyList()) + fitEpochOffset
        val stamp = ImportUtil.stampFromEpoch(epoch)

        return listOf(
            ImportedDive(
                date = stamp.date,
                time = stamp.time,
                maxdepth = maxDepth ?: samples.maxOfOrNull { it.depth } ?: 0.0,
                duration = bottomTime ?: elapsedSeconds
                    ?: samples.maxOfOrNull { it.timeMs / 1000 } ?: 0,
                avgdepth = avgDepth ?: 0.0,
                tempMin = minTemp,
                tempMax = maxTemp,
                gasmixes = gases,
                samples = samples,
                siteLat = lat,
                siteLon = lon
            )
        )
    }

    private fun semicirclesToDegrees(v: Long) = v * (180.0 / 2147483648.0)

    /**
     * Reads one field as a number, honouring FIT's per-base-type "invalid"
     * sentinel (all bits set), which marks a field the device did not record.
     */
    private fun readNumeric(bytes: ByteArray, at: Int, f: FieldDef, order: ByteOrder): Long? {
        val b = ByteBuffer.wrap(bytes, at, f.size).order(order)
        return when (f.baseType) {
            0, 2, 10 -> (b.get().toLong() and 0xFF).takeIf { it != 0xFFL }        // enum, uint8
            1 -> b.get().toLong().takeIf { it != 0x7FL }                          // sint8
            3 -> b.short.toLong().takeIf { it != 0x7FFFL }                        // sint16
            4, 11 -> (b.short.toLong() and 0xFFFF).takeIf { it != 0xFFFFL }       // uint16
            5 -> b.int.toLong().takeIf { it != 0x7FFFFFFFL }                      // sint32
            6, 12 -> (b.int.toLong() and 0xFFFFFFFFL).takeIf { it != 0xFFFFFFFFL } // uint32
            8 -> b.float.takeIf { !it.isNaN() }?.toLong()                         // float32
            9 -> b.double.takeIf { !it.isNaN() }?.toLong()                        // float64
            13 -> if (f.size == 1) (b.get().toLong() and 0xFF).takeIf { it != 0xFFL } else null
            else -> null   // strings and the rest are not read here
        }
    }
}
