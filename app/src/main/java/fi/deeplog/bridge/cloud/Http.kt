package fi.deeplog.bridge.cloud

import java.io.ByteArrayOutputStream
import java.net.HttpURLConnection
import java.net.URL

/** A cookie-keeping HTTP client, which the Garmin SSO flow needs. */
class Http {

    private val cookies = LinkedHashMap<String, String>()

    class Response(val code: Int, val body: ByteArray, val headers: Map<String, List<String>>) {
        val text: String get() = String(body, Charsets.UTF_8)
        val ok: Boolean get() = code in 200..299
    }

    fun request(
        method: String,
        url: String,
        headers: Map<String, String> = emptyMap(),
        body: ByteArray? = null,
        followRedirects: Boolean = true
    ): Response {
        val conn = (URL(url).openConnection() as HttpURLConnection).apply {
            requestMethod = method
            instanceFollowRedirects = false     // handled here so cookies survive
            connectTimeout = 20_000
            readTimeout = 60_000
            // Garmin's SSO sits behind a bot filter that rejects the default
            // Java user agent outright.
            setRequestProperty("User-Agent",
                "Mozilla/5.0 (Linux; Android 14) AppleWebKit/537.36 " +
                    "(KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36")
            setRequestProperty("Accept-Language", "en-US,en;q=0.9")
            if (cookies.isNotEmpty()) {
                setRequestProperty("Cookie", cookies.entries.joinToString("; ") { "${it.key}=${it.value}" })
            }
            headers.forEach { (k, v) -> setRequestProperty(k, v) }
            if (body != null) {
                doOutput = true
                outputStream.use { it.write(body) }
            }
        }

        val code = conn.responseCode
        val head = conn.headerFields ?: emptyMap()
        head["Set-Cookie"]?.forEach { raw ->
            val pair = raw.substringBefore(';')
            val name = pair.substringBefore('=').trim()
            val value = pair.substringAfter('=', "").trim()
            if (name.isNotEmpty()) cookies[name] = value
        }

        val stream = if (code in 200..399) conn.inputStream else conn.errorStream
        val bytes = stream?.use { input ->
            ByteArrayOutputStream().also { out -> input.copyTo(out) }.toByteArray()
        } ?: ByteArray(0)
        conn.disconnect()

        if (followRedirects && code in 300..399) {
            val location = head["Location"]?.firstOrNull()
            if (location != null) {
                val next = if (location.startsWith("http")) location
                           else URL(URL(url), location).toString()
                return request("GET", next, headers, null, true)
            }
        }
        return Response(code, bytes, head)
    }

    fun get(url: String, headers: Map<String, String> = emptyMap()) =
        request("GET", url, headers)

    fun postForm(url: String, form: Map<String, String>, headers: Map<String, String> = emptyMap()) =
        request(
            "POST", url,
            headers + mapOf("Content-Type" to "application/x-www-form-urlencoded"),
            form.entries.joinToString("&") { "${OAuth1.encode(it.key)}=${OAuth1.encode(it.value)}" }
                .toByteArray(),
            followRedirects = false   // the SSO ticket arrives in the 302 body
        )
}
