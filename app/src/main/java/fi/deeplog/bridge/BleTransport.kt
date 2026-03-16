package fi.deeplog.bridge

import android.annotation.SuppressLint
import android.bluetooth.*
import android.content.Context
import android.util.Log
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

private const val TAG = "BleTransport"

private val CCCD_UUID = UUID.fromString("00002902-0000-1000-8000-00805f9b34fb")

@SuppressLint("MissingPermission")
class BleTransport(
    private val context: Context,
    private val device: BluetoothDevice,
    private val onStatus: (String) -> Unit
) {
    private var gatt: BluetoothGatt? = null
    private var writeChar: BluetoothGattCharacteristic? = null
    private var writeNoResponse: Boolean = false

    private val connectLatch = CountDownLatch(1)
    private var connectError: String? = null

    // Queue of pending CCCD writes — Android BLE allows only one op at a time.
    private val cccdQueue = LinkedBlockingQueue<Pair<BluetoothGattCharacteristic, ByteArray>>()
    private var cccdPending = false

    // Latch for each confirmed write (WRITE_DEFAULT only).
    private var pendingWriteResult: ((Boolean) -> Unit)? = null

    // ── Public API (called from C++ on download thread) ───────────────────────

    fun write(data: ByteArray): Boolean {
        val ch = writeChar ?: run { Log.e(TAG, "write: no writeChar"); return false }
        val g  = gatt     ?: run { Log.e(TAG, "write: no gatt");      return false }

        ch.value = data

        return if (writeNoResponse) {
            ch.writeType = BluetoothGattCharacteristic.WRITE_TYPE_NO_RESPONSE
            val ok = g.writeCharacteristic(ch)
            Log.v(TAG, "write NR ${data.size}B ok=$ok")
            ok
        } else {
            ch.writeType = BluetoothGattCharacteristic.WRITE_TYPE_DEFAULT
            val latch = CountDownLatch(1)
            var success = false
            pendingWriteResult = { ok -> success = ok; latch.countDown() }
            if (!g.writeCharacteristic(ch)) {
                pendingWriteResult = null
                return false
            }
            latch.await(5, TimeUnit.SECONDS)
            success
        }
    }

    fun ioctl(request: Int, data: ByteArray) {
        Log.d(TAG, "ioctl req=0x${request.toString(16)} len=${data.size}")
    }

    // ── Connection management ─────────────────────────────────────────────────

    fun connect() {
        Log.i(TAG, "Connecting to ${device.address}")
        gatt = device.connectGatt(context, false, gattCallback, BluetoothDevice.TRANSPORT_LE)
        if (!connectLatch.await(20, TimeUnit.SECONDS)) throw RuntimeException("GATT connect timeout")
        connectError?.let { throw RuntimeException(it) }
    }

    fun close() {
        gatt?.disconnect()
        gatt?.close()
        gatt = null
        writeChar = null
    }

    // ── GATT callbacks ────────────────────────────────────────────────────────

    private val gattCallback = object : BluetoothGattCallback() {

        override fun onConnectionStateChange(g: BluetoothGatt, status: Int, newState: Int) {
            when (newState) {
                BluetoothProfile.STATE_CONNECTED -> {
                    Log.i(TAG, "Connected, requesting MTU")
                    g.requestMtu(517)
                }
                BluetoothProfile.STATE_DISCONNECTED -> {
                    Log.i(TAG, "Disconnected status=$status")
                    if (connectLatch.count > 0) {
                        connectError = "Disconnected (status $status)"
                        connectLatch.countDown()
                    }
                    DcBridge.onBleData(byteArrayOf()) // unblock pending read
                }
            }
        }

        override fun onMtuChanged(g: BluetoothGatt, mtu: Int, status: Int) {
            Log.i(TAG, "MTU=$mtu, discovering services")
            g.discoverServices()
        }

        override fun onServicesDiscovered(g: BluetoothGatt, status: Int) {
            if (status != BluetoothGatt.GATT_SUCCESS) {
                connectError = "Service discovery failed: $status"
                connectLatch.countDown()
                return
            }

            // Log all services/characteristics for debugging.
            for (svc in g.services) {
                Log.i(TAG, "Service: ${svc.uuid}")
                for (ch in svc.characteristics) {
                    Log.i(TAG, "  Char: ${ch.uuid} props=0x${ch.properties.toString(16)}")
                }
            }

            // Pick write characteristic; queue up all CCCD enables.
            // Skip standard BLE housekeeping services — they have writable
            // characteristics (e.g. Device Name) that are not data channels.
            val SKIP_SERVICES = setOf(
                "00001800-0000-1000-8000-00805f9b34fb", // Generic Access
                "00001801-0000-1000-8000-00805f9b34fb", // Generic Attribute
                "0000180a-0000-1000-8000-00805f9b34fb", // Device Information
                "0000180f-0000-1000-8000-00805f9b34fb"  // Battery
            )

            for (svc in g.services) {
                if (svc.uuid.toString().lowercase() in SKIP_SERVICES) continue
                for (ch in svc.characteristics) {
                    val props = ch.properties
                    val isWrite   = props and BluetoothGattCharacteristic.PROPERTY_WRITE != 0
                    val isWriteNR = props and BluetoothGattCharacteristic.PROPERTY_WRITE_NO_RESPONSE != 0
                    val canNotify = props and (BluetoothGattCharacteristic.PROPERTY_NOTIFY or
                        BluetoothGattCharacteristic.PROPERTY_INDICATE) != 0

                    if ((isWrite || isWriteNR) && writeChar == null) {
                        writeChar = ch
                        // Prefer WRITE_NO_RESPONSE when available — required by Shearwater.
                        writeNoResponse = isWriteNR
                        Log.i(TAG, "writeChar=${ch.uuid} noResponse=$writeNoResponse")
                    }
                    if (canNotify) {
                        val isIndicate = props and BluetoothGattCharacteristic.PROPERTY_INDICATE != 0
                        val cccdVal = if (isIndicate)
                            BluetoothGattDescriptor.ENABLE_INDICATION_VALUE
                        else
                            BluetoothGattDescriptor.ENABLE_NOTIFICATION_VALUE
                        cccdQueue.add(Pair(ch, cccdVal))
                    }
                }
            }

            // Kick off sequential CCCD writes.
            drainCccdQueue(g)
        }

        override fun onDescriptorWrite(g: BluetoothGatt, descriptor: BluetoothGattDescriptor, status: Int) {
            Log.d(TAG, "CCCD written for ${descriptor.characteristic.uuid} status=$status")
            cccdPending = false
            drainCccdQueue(g)
        }

        override fun onCharacteristicChanged(g: BluetoothGatt, ch: BluetoothGattCharacteristic) {
            DcBridge.onBleData(ch.value ?: return)
        }

        // API 33+
        override fun onCharacteristicChanged(g: BluetoothGatt, ch: BluetoothGattCharacteristic, value: ByteArray) {
            DcBridge.onBleData(value)
        }

        override fun onCharacteristicWrite(g: BluetoothGatt, ch: BluetoothGattCharacteristic, status: Int) {
            pendingWriteResult?.invoke(status == BluetoothGatt.GATT_SUCCESS)
            pendingWriteResult = null
        }
    }

    // Write one CCCD at a time; signal connect complete when queue is empty.
    private fun drainCccdQueue(g: BluetoothGatt) {
        if (cccdPending) return
        val (ch, value) = cccdQueue.poll() ?: run {
            // All CCCDs written — connection is ready.
            onStatus("Connected to ${device.name ?: device.address}")
            if (connectLatch.count > 0) connectLatch.countDown()
            return
        }
        g.setCharacteristicNotification(ch, true)
        val cccd = ch.getDescriptor(CCCD_UUID) ?: run {
            // No CCCD descriptor — skip and continue.
            drainCccdQueue(g)
            return
        }
        cccd.value = value
        cccdPending = g.writeDescriptor(cccd)
        if (!cccdPending) drainCccdQueue(g)
    }
}
