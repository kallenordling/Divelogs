package fi.deeplog.bridge.imports

/** One import format. */
interface DiveImporter {
    /** Shown in the import summary. */
    val name: String

    /** Cheap check on the file's head; full parsing happens in [parse]. */
    fun sniff(bytes: ByteArray, filename: String): Boolean

    /** @throws Exception when the file turns out not to be this format after all. */
    fun parse(bytes: ByteArray): ImportResult
}
