package fi.deeplog.bridge

import android.util.Log
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.json.JSONArray
import org.json.JSONObject
import java.net.HttpURLConnection
import java.net.URL
import java.net.URLEncoder

private const val STAG = "Supabase"

object SupabaseClient {

    const val BASE = "https://bdquivweiecffyopsevs.supabase.co"
    const val ANON = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9" +
        ".eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJkcXVpdndlaWVjZmZ5b3BzZXZzIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzIyMTM2MjYsImV4cCI6MjA4Nzc4OTYyNn0" +
        ".wXLAcj5NeyVnO2nTB5ZzNWwe_hFtPkZYHBKkMT2FmAo"

    var accessToken:  String? = null
    var refreshToken: String? = null
    var userId:       String? = null

    val isLoggedIn get() = accessToken != null

    fun isTokenExpired(): Boolean {
        val token = accessToken ?: return true
        return try {
            val payload = token.split(".")[1]
            val padded  = payload.padEnd((payload.length + 3) / 4 * 4, '=')
            val decoded = android.util.Base64.decode(padded,
                android.util.Base64.URL_SAFE or android.util.Base64.NO_PADDING)
            val exp = JSONObject(String(decoded)).getLong("exp")
            System.currentTimeMillis() / 1000 > exp - 60   // treat as expired 1 min early
        } catch (e: Exception) { true }
    }

    // ── Authentication ────────────────────────────────────────────────────────

    suspend fun signIn(email: String, password: String): Result<Unit> = withContext(Dispatchers.IO) {
        runCatching {
            val body = JSONObject().apply { put("email", email); put("password", password) }
            val res = postJson("/auth/v1/token?grant_type=password", body.toString(), useAuth = false)
            storeSession(res)
        }.onFailure { Log.e(STAG, "signIn: $it") }
    }

    suspend fun signUp(email: String, password: String): Result<Unit> = withContext(Dispatchers.IO) {
        val r = runCatching {
            val body = JSONObject().apply { put("email", email); put("password", password) }
            postJson("/auth/v1/signup", body.toString(), useAuth = false)
        }.onFailure { Log.e(STAG, "signUp: $it") }
        // Sign in after successful sign-up to get a session
        if (r.isSuccess) signIn(email, password) else r.map {}
    }

    suspend fun refreshSession(): Boolean = withContext(Dispatchers.IO) {
        val rt = refreshToken ?: return@withContext false
        runCatching {
            val body = JSONObject().apply { put("refresh_token", rt) }
            val res = postJson("/auth/v1/token?grant_type=refresh_token", body.toString(), useAuth = false)
            storeSession(res)
        }.isSuccess
    }

    private fun storeSession(res: JSONObject) {
        accessToken  = res.getString("access_token")
        refreshToken = res.optString("refresh_token").takeIf { it.isNotEmpty() }
        userId = res.getJSONObject("user").getString("id")
    }

    fun clearSession() { accessToken = null; refreshToken = null; userId = null }

    // ── Dive upload ───────────────────────────────────────────────────────────

    suspend fun uploadDive(
        dive: DiveEntry,
        deviceName: String,
        siteName: String?,
        siteLat: Double?,
        siteLon: Double?
    ): Result<Unit> = withContext(Dispatchers.IO) {
        runCatching {
            val compactSamples = JSONArray()
            for (i in 0 until dive.samples.length()) {
                val s = dive.samples.getJSONArray(i)
                compactSamples.put(JSONArray().apply {
                    put(s.getInt(0))     // time_ms
                    put(s.getDouble(1))  // depth
                    put(s.getDouble(2))  // temperature
                })
            }
            val obj = JSONObject().apply {
                put("user_id",     userId)
                put("device_name", deviceName)
                put("date",        dive.date)
                put("time",        dive.time)
                put("maxdepth",    dive.maxdepth)
                put("avgdepth",    dive.avgdepth)
                put("duration",    dive.duration)
                put("divemode",    dive.divemode)
                dive.temp_surface ?.let { put("temp_surface",  it) }
                dive.temp_min     ?.let { put("temp_min",      it) }
                dive.temp_max     ?.let { put("temp_max",      it) }
                dive.atmospheric  ?.let { put("atmospheric",   it) }
                dive.salinity_type?.let { put("salinity_type", it) }
                put("gasmixes",  dive.gasmixes)
                put("tanks",     dive.tanks)
                siteName?.let { put("site_name", it) }
                siteLat ?.let { put("site_lat",  it) }
                siteLon ?.let { put("site_lon",  it) }
                put("samples", compactSamples)
            }
            postRest("/rest/v1/dives", obj.toString(),
                extraHeaders = mapOf("Prefer" to "resolution=ignore-duplicates,return=minimal"))
        }.onFailure { Log.e(STAG, "uploadDive: $it") }
    }

    // ── All user dives ────────────────────────────────────────────────────────

    suspend fun fetchMyDives(): List<JSONObject> = withContext(Dispatchers.IO) {
        runCatching {
            val arr = getArray("/rest/v1/dives?select=*&order=date.asc,time.asc")
            (0 until arr.length()).map { arr.getJSONObject(it) }
        }.getOrElse { e -> Log.e(STAG, "fetchMyDives: $e"); emptyList() }
    }

    // ── Site dives fetch ──────────────────────────────────────────────────────

    data class SiteDive(
        val date: String,
        val maxdepth: Double,
        val tempMin: Double?,
        val tempMax: Double?,
        val samples: JSONArray   // [[time_ms, depth, temp], ...]
    )

    suspend fun fetchSiteDives(siteName: String): List<SiteDive> = withContext(Dispatchers.IO) {
        runCatching {
            val enc = URLEncoder.encode(siteName, "UTF-8")
            val arr = getArray(
                "/rest/v1/dives?site_name=eq.$enc" +
                "&select=date,maxdepth,temp_min,temp_max,samples" +
                "&order=date.asc"
            )
            (0 until arr.length()).map { i ->
                val o = arr.getJSONObject(i)
                SiteDive(
                    date     = o.optString("date", "?"),
                    maxdepth = o.optDouble("maxdepth", 0.0),
                    tempMin  = o.optDouble("temp_min",  Double.NaN).takeIf { !it.isNaN() },
                    tempMax  = o.optDouble("temp_max",  Double.NaN).takeIf { !it.isNaN() },
                    samples  = o.optJSONArray("samples") ?: JSONArray()
                )
            }
        }.getOrElse { e -> Log.e(STAG, "fetchSiteDives: $e"); emptyList() }
    }

    // ── HTTP helpers ──────────────────────────────────────────────────────────

    private fun postJson(path: String, body: String, useAuth: Boolean): JSONObject {
        val conn = open(path, "POST", useAuth)
        conn.doOutput = true
        conn.outputStream.write(body.toByteArray())
        return JSONObject(readResponse(conn))
    }

    private fun postRest(path: String, body: String, extraHeaders: Map<String, String> = emptyMap()) {
        val conn = open(path, "POST", useAuth = true)
        extraHeaders.forEach { (k, v) -> conn.setRequestProperty(k, v) }
        conn.doOutput = true
        conn.outputStream.write(body.toByteArray())
        readResponse(conn)  // throws on error
    }

    private fun getArray(path: String): JSONArray {
        val conn = open(path, "GET", useAuth = true)
        return JSONArray(readResponse(conn))
    }

    private fun open(path: String, method: String, useAuth: Boolean): HttpURLConnection {
        return (URL("$BASE$path").openConnection() as HttpURLConnection).apply {
            requestMethod = method
            setRequestProperty("Content-Type", "application/json")
            setRequestProperty("apikey", ANON)
            if (useAuth) setRequestProperty("Authorization", "Bearer ${accessToken ?: ANON}")
            connectTimeout = 15_000; readTimeout = 30_000
        }
    }

    private fun readResponse(conn: HttpURLConnection): String {
        val code = conn.responseCode
        val text = try {
            if (code < 300) conn.inputStream.bufferedReader().readText()
            else conn.errorStream?.bufferedReader()?.readText() ?: ""
        } finally { conn.disconnect() }
        if (code >= 300) throw Exception("HTTP $code: $text")
        return text
    }
}
