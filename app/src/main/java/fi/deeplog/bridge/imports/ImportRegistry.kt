package fi.deeplog.bridge.imports

/** Picks the importer for a file and runs it. */
object ImportRegistry {

    /**
     * Order matters: the specific formats get first refusal, and CSV comes
     * last because its sniff is the loosest.
     */
    val importers: List<DiveImporter> = listOf(
        UddfImporter(),
        SubsurfaceImporter(),
        MacDiveImporter(),
        ShearwaterCloudImporter(),
        FitImporter(),
        Dl7Importer(),
        CsvImporter()
    )

    fun importerFor(bytes: ByteArray, filename: String): DiveImporter? =
        importers.firstOrNull { it.sniff(bytes, filename) }

    /** @throws IllegalArgumentException when nothing recognises the file. */
    fun parse(bytes: ByteArray, filename: String): ImportResult {
        val importer = importerFor(bytes, filename)
            ?: throw IllegalArgumentException("Unrecognised file format: $filename")
        return importer.parse(bytes)
    }

    /** Extensions worth offering in the file picker. */
    val extensions = listOf(
        "uddf", "xml", "ssrf", "csv", "txt", "zxu", "zxl", "db", "sqlite", "fit"
    )
}
