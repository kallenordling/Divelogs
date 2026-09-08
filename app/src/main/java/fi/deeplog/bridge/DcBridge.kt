package fi.deeplog.bridge

// JNI interface to bridge.cpp (libdivecomputer native layer).
object DcBridge {

    // Called from C++ when BLE data arrives from the device.
    @JvmStatic external fun onBleData(data: ByteArray)

    // Starts a blocking download. Returns a JSON array of dives.
    // deviceName   — advertised BLE name (used for descriptor matching)
    // transport    — DC_TRANSPORT_BLE = 5
    // bleTransport — object with write(ByteArray):Boolean and ioctl(Int, ByteArray) methods
    // fingerprint  — bytes of the last-downloaded dive's fingerprint, or null for full download
    @JvmStatic external fun download(deviceName: String, transport: Int, bleTransport: BleTransport, fingerprint: ByteArray?): String

    // Resolves an advertised BLE name to "Vendor|Product" via libdivecomputer's
    // own descriptor filters, or null when it is not a supported dive computer.
    @JvmStatic external fun matchDevice(name: String, transport: Int): String?

    // Parses one raw dive log (as stored in a desktop app's database) with the
    // named computer's libdivecomputer parser, returning the same dive JSON a
    // download produces, or null if it cannot be read. Set compressed for
    // Shearwater Cloud blobs, which carry the LRE+XOR layers from the wire.
    @JvmStatic external fun parseRaw(
        vendor: String, product: String, data: ByteArray, compressed: Boolean
    ): String?
}
