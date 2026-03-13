package fi.deeplog.bridge

import android.annotation.SuppressLint
import android.bluetooth.*
import android.content.Context
import android.util.Log
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

private const val TAG = "BleTransport"

// Standard CCCD (client characteristic configuration descriptor) UUID.
private val CCCD_UUID = UUID.fromString("00002902-0000-1000-8000-00805f9b34fb")

// BleTransport manages a single GATT connection.
// write() and ioctl() are called synchronously from the libdivecomputer thread.
// onCharacteristicChanged() feeds data into the C-side queue via DcBridge.onBleData().
@SuppressLint("MissingPermission")
class BleTransport(
    private val context: Context,
    private val device: BluetoothDevice,
    private val onStatus: (String) -> Unit
) {
    private var gatt: BluetoothGatt? = null
    private var writeChar: BluetoothGattCharacteristic? = null

    private val connectLatch = CountDownLatch(1)
    private var connectError: String? = null

    // Called from C++ bridge to send bytes to the device.
    fun write(data: ByteArray): Boolean {
        val ch = writeChar ?: return false
        val g  = gatt     ?: return false
        val latch = CountDownLatch(1)
        var success = false
        pendingWriteLatch = latch
        pendingWriteResult = { ok -> success = ok; latch.countDown() }
        ch.value = data
        if (!g.writeCharacteristic(ch)) {
            pendingWriteLatch = null
            return false
        }
        latch.await(5, TimeUnit.SECONDS)
        return success
    }

    // Called from C++ bridge for BLE ioctls (e.g. characteristic read/write by UUID).
    fun ioctl(request: Int, data: ByteArray) {
        // Most libdivecomputer BLE devices only use standard read/write/notify;
        // characteristic-level ioctls are rare. Log and ignore for now.
        Log.d(TAG, "ioctl req=0x${request.toString(16)} len=${data.size}")
    }

    // ── Connection management ─────────────────────────────────────────────────

    fun connect() {
        Log.i(TAG, "Connecting to ${device.address}")
        gatt = device.connectGatt(context, false, gattCallback, BluetoothDevice.TRANSPORT_LE)
        if (!connectLatch.await(20, TimeUnit.SECONDS)) {
            throw RuntimeException("GATT connect timeout")
        }
        connectError?.let { throw RuntimeException(it) }
    }

    fun close() {
        gatt?.disconnect()
        gatt?.close()
        gatt = null
        writeChar = null
    }

    // ── GATT callbacks ────────────────────────────────────────────────────────

    private var pendingWriteLatch:  CountDownLatch?  = null
    private var pendingWriteResult: ((Boolean) -> Unit)? = null

    private val gattCallback = object : BluetoothGattCallback() {

        override fun onConnectionStateChange(g: BluetoothGatt, status: Int, newState: Int) {
            when (newState) {
                BluetoothProfile.STATE_CONNECTED -> {
                    Log.i(TAG, "Connected, discovering services")
                    g.requestMtu(517)
                    g.discoverServices()
                }
                BluetoothProfile.STATE_DISCONNECTED -> {
                    Log.i(TAG, "Disconnected")
                    if (connectLatch.count > 0) {
                        connectError = "Disconnected (status $status)"
                        connectLatch.countDown()
                    }
                    // Wake up any blocked read so libdivecomputer can return.
                    DcBridge.onBleData(byteArrayOf())
                }
            }
        }

        override fun onServicesDiscovered(g: BluetoothGatt, status: Int) {
            if (status != BluetoothGatt.GATT_SUCCESS) {
                connectError = "Service discovery failed: $status"
                connectLatch.countDown()
                return
            }
            // Pick the first writable characteristic and enable notifications on all notifiable ones.
            for (svc in g.services) {
                for (ch in svc.characteristics) {
                    val props = ch.properties
                    val canWrite = props and (BluetoothGattCharacteristic.PROPERTY_WRITE or
                        BluetoothGattCharacteristic.PROPERTY_WRITE_NO_RESPONSE) != 0
                    val canNotify = props and (BluetoothGattCharacteristic.PROPERTY_NOTIFY or
                        BluetoothGattCharacteristic.PROPERTY_INDICATE) != 0
                    if (canWrite && writeChar == null) writeChar = ch
                    if (canNotify) enableNotifications(g, ch)
                }
            }
            onStatus("Connected to ${device.name ?: device.address}")
            connectLatch.countDown()
        }

        override fun onCharacteristicChanged(
            g: BluetoothGatt,
            ch: BluetoothGattCharacteristic
        ) {
            DcBridge.onBleData(ch.value ?: return)
        }

        // API 33+
        override fun onCharacteristicChanged(
            g: BluetoothGatt,
            ch: BluetoothGattCharacteristic,
            value: ByteArray
        ) {
            DcBridge.onBleData(value)
        }

        override fun onCharacteristicWrite(
            g: BluetoothGatt,
            ch: BluetoothGattCharacteristic,
            status: Int
        ) {
            pendingWriteResult?.invoke(status == BluetoothGatt.GATT_SUCCESS)
            pendingWriteResult = null
        }

        override fun onDescriptorWrite(
            g: BluetoothGatt,
            descriptor: BluetoothGattDescriptor,
            status: Int
        ) {
            Log.d(TAG, "Descriptor write status=$status for ${descriptor.uuid}")
        }
    }

    private fun enableNotifications(g: BluetoothGatt, ch: BluetoothGattCharacteristic) {
        g.setCharacteristicNotification(ch, true)
        val cccd = ch.getDescriptor(CCCD_UUID) ?: return
        val value = if (ch.properties and BluetoothGattCharacteristic.PROPERTY_INDICATE != 0)
            BluetoothGattDescriptor.ENABLE_INDICATION_VALUE
        else
            BluetoothGattDescriptor.ENABLE_NOTIFICATION_VALUE
        cccd.value = value
        g.writeDescriptor(cccd)
    }
}
