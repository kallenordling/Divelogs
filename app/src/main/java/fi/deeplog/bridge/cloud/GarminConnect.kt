package fi.deeplog.bridge.cloud

import android.util.Log
import fi.deeplog.bridge.imports.FitImporter
import fi.deeplog.bridge.imports.ImportResult
import fi.deeplog.bridge.imports.ImportedDive
import org.json.JSONArray
import org.json.JSONObject
import java.io.ByteArrayInputStream
import java.util.zip.ZipInputStream

/**
 * Downloads dives from a Garmin Connect account.
 *
 * Garmin has no public dive API, so this walks the same path their own apps do:
 *
 *  1. sign in through the SSO form, which yields a one-shot service ticket
 *  2. trade the ticket for an OAuth 1.0a token pair
 *  3. exchange that pair for an OAuth 2.0 bearer token
 *  4. list diving activities, and download each one's FIT file
 *
 * The FIT files are then read by [FitImporter], the same code path a `.fit`
 * file dragged in from disk takes.
 *
 * This is an undocumented interface that Garmin can change without notice, and
 * using it may sit outside their terms of service. It is here to get at your
 * own dives in your own account.
 */
class GarminConnect(private val http: Http = Http()) {

    companion object {
        private const val TAG = "GarminConnect"
        private const val SSO = "https://sso.garmin.com/sso"
        private const val API = "https://connectapi.garmin.com"

        /**
         * Garmin's OAuth1 consumer credentials are not secret — they ship in
         * every Garmin app — and are published here for the same reason the
         * `garth` library reads them from this URL rather than embedding them:
         * Garmin rotates them, and a hard-coded pair would silently stop
         * working.
         */
        private const val CONSUMER_URL = "https://thegarth.s3.amazonaws.com/oauth_consumer.json"
    }

    /** Raised with a message worth showing the user. */
    class GarminException(message: String) : Exception(message)

    private var bearer: String? = null

    // ── Authentication ────────────────────────────────────────────────────────

    fun login(email: String, password: String) {
        val ticket = signIn(email, password)
        val (token, secret) = preauthorize(ticket)
        bearer = exchange(token, secret)
    }

    private fun signIn(email: String, password: String): String {
        val embed = "$SSO/embed?clientId=GarminConnect&locale=en&" +
            "service=https%3A%2F%2Fconnect.garmin.com%2Fmodern"
        http.get(embed)   // establishes the session cookies the form checks

        val signinUrl = "$SSO/signin?clientId=GarminConnect&locale=en&" +
            "service=https%3A%2F%2Fconnect.garmin.com%2Fmodern&" +
            "gauthHost=https%3A%2F%2Fsso.garmin.com%2Fsso&" +
            "webhost=https%3A%2F%2Fconnect.garmin.com%2Fmodern"

        val form = http.get(signinUrl)
        if (!form.ok) throw GarminException("Garmin sign-in page returned ${form.code}")

        val csrf = Regex("name=\"_csrf\"\\s+value=\"([^\"]+)\"").find(form.text)?.groupValues?.get(1)
            ?: throw GarminException("Could not read the sign-in form (Garmin may have changed it)")

        val res = http.postForm(
            signinUrl,
            mapOf("username" to email, "password" to password, "embed" to "true", "_csrf" to csrf),
            mapOf("Referer" to signinUrl)
        )

        val body = res.text
        Regex("embed\\?ticket=([^\"]+)\"").find(body)?.let { return it.groupValues[1] }
        Regex("ticket=(ST-[^\"&]+)").find(body)?.let { return it.groupValues[1] }

        if (body.contains("mfa", ignoreCase = true) || body.contains("verification code", true)) {
            throw GarminException(
                "This Garmin account uses two-factor authentication, which this " +
                    "importer cannot complete. Export the dives from Garmin Connect " +
                    "and import the .fit files instead."
            )
        }
        throw GarminException("Garmin rejected the sign-in — check the email and password")
    }

    private fun consumer(): Pair<String, String> {
        val res = http.get(CONSUMER_URL)
        if (!res.ok) throw GarminException("Could not fetch Garmin's OAuth keys (${res.code})")
        val o = JSONObject(res.text)
        return o.getString("consumer_key") to o.getString("consumer_secret")
    }

    private fun preauthorize(ticket: String): Pair<String, String> {
        val (key, secret) = consumer()
        val url = "$API/oauth-service/oauth/preauthorized"
        val params = mapOf(
            "ticket" to ticket,
            "login-url" to "https://sso.garmin.com/sso/embed",
            "accepts-mfa-tokens" to "true"
        )
        val query = params.entries.joinToString("&") { "${OAuth1.encode(it.key)}=${OAuth1.encode(it.value)}" }
        val auth = OAuth1.header("GET", url, key, secret, params = params)

        val res = http.get("$url?$query", mapOf("Authorization" to auth))
        if (!res.ok) throw GarminException("Garmin token request failed (${res.code})")

        // The response is a form-encoded body, not JSON.
        val fields = res.text.split('&').mapNotNull {
            val k = it.substringBefore('='); val v = it.substringAfter('=', "")
            if (k.isEmpty()) null else k to java.net.URLDecoder.decode(v, "UTF-8")
        }.toMap()

        val token = fields["oauth_token"] ?: throw GarminException("Garmin returned no OAuth token")
        val tokenSecret = fields["oauth_token_secret"] ?: ""
        return token to tokenSecret
    }

    private fun exchange(token: String, tokenSecret: String): String {
        val (key, secret) = consumer()
        val url = "$API/oauth-service/oauth/exchange/user/2.0"
        val auth = OAuth1.header("POST", url, key, secret, token, tokenSecret)

        val res = http.request(
            "POST", url,
            mapOf(
                "Authorization" to auth,
                "Content-Type" to "application/x-www-form-urlencoded"
            ),
            ByteArray(0)
        )
        if (!res.ok) throw GarminException("Garmin token exchange failed (${res.code})")
        return JSONObject(res.text).optString("access_token").takeIf { it.isNotEmpty() }
            ?: throw GarminException("Garmin returned no access token")
    }

    // ── Dives ─────────────────────────────────────────────────────────────────

    /**
     * @param limit how many diving activities to look back over.
     * @param onProgress called as (done, total) while FIT files download.
     */
    fun downloadDives(limit: Int = 100, onProgress: (Int, Int) -> Unit = { _, _ -> }): ImportResult {
        val token = bearer ?: throw GarminException("Not signed in to Garmin Connect")
        val authHeader = mapOf("Authorization" to "Bearer $token", "Accept" to "application/json")

        val listUrl = "$API/activitylist-service/activities/search/activities" +
            "?activityType=diving&start=0&limit=$limit"
        val res = http.get(listUrl, authHeader)
        if (!res.ok) throw GarminException("Could not list Garmin activities (${res.code})")

        // Some Connect endpoints wrap the array in an object; accept both.
        val body = res.text.trim()
        val array = if (body.startsWith("[")) JSONArray(body)
                    else JSONObject(body).optJSONArray("activityList") ?: JSONArray()

        val ids = (0 until array.length()).mapNotNull {
            array.optJSONObject(it)?.optLong("activityId")?.takeIf { id -> id > 0 }
        }
        if (ids.isEmpty()) return ImportResult(emptyList(), listOf("No diving activities found"), "Garmin Connect")

        val fit = FitImporter()
        val dives = mutableListOf<ImportedDive>()
        val warnings = mutableListOf<String>()

        for ((i, id) in ids.withIndex()) {
            onProgress(i, ids.size)
            try {
                val file = http.get("$API/download-service/files/activity/$id", authHeader)
                if (!file.ok) { warnings.add("Activity $id: download failed (${file.code})"); continue }
                val bytes = unzipFit(file.body) ?: file.body
                if (!fit.sniff(bytes, "$id.fit")) {
                    warnings.add("Activity $id: not a FIT file"); continue
                }
                val parsed = fit.parse(bytes).dives
                if (parsed.isEmpty()) warnings.add("Activity $id: no dive data")
                dives += parsed
            } catch (e: Exception) {
                Log.w(TAG, "activity $id failed", e)
                warnings.add("Activity $id: ${e.message}")
            }
        }
        onProgress(ids.size, ids.size)
        return ImportResult(dives, warnings, "Garmin Connect")
    }

    /** Garmin serves activity files as a ZIP holding a single .fit. */
    private fun unzipFit(bytes: ByteArray): ByteArray? {
        if (bytes.size < 4 || bytes[0] != 'P'.code.toByte() || bytes[1] != 'K'.code.toByte()) return null
        ZipInputStream(ByteArrayInputStream(bytes)).use { zip ->
            var entry = zip.nextEntry
            while (entry != null) {
                if (entry.name.endsWith(".fit", ignoreCase = true)) return zip.readBytes()
                entry = zip.nextEntry
            }
        }
        return null
    }
}
