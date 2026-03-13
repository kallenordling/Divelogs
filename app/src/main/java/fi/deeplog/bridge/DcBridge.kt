package fi.deeplog.bridge

// JNI interface to bridge.cpp (libdivecomputer native layer).
object DcBridge {

    // Called from C++ when BLE data arrives from the device.
    @JvmStatic external fun onBleData(data: ByteArray)

    // Starts a blocking download. Returns a JSON array of dives.
    // deviceName  — advertised BLE name (used for descriptor matching)
    // transport   — DC_TRANSPORT_BLE = 5
    // bleTransport — object with write(ByteArray):Boolean and ioctl(Int, ByteArray) methods
    @JvmStatic external fun download(deviceName: String, transport: Int, bleTransport: BleTransport): String
}
