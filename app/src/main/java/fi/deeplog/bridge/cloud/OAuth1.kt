package fi.deeplog.bridge.cloud

import android.util.Base64
import java.net.URLEncoder
import java.security.SecureRandom
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

/**
 * OAuth 1.0a request signing (HMAC-SHA1), which Garmin's token endpoints still
 * require before they will hand out an OAuth2 bearer token.
 */
object OAuth1 {

    /** Percent-encoding per RFC 5849 §3.6, which is stricter than URLEncoder's. */
    fun encode(s: String): String =
        URLEncoder.encode(s, "UTF-8")
            .replace("+", "%20")
            .replace("*", "%2A")
            .replace("%7E", "~")

    /**
     * @param params query parameters already present on the URL, which must be
     *   included in the signature base string.
     * @return the value for the Authorization header.
     */
    fun header(
        method: String,
        url: String,
        consumerKey: String,
        consumerSecret: String,
        token: String? = null,
        tokenSecret: String? = null,
        params: Map<String, String> = emptyMap()
    ): String {
        val nonce = ByteArray(16).also { SecureRandom().nextBytes(it) }
            .joinToString("") { "%02x".format(it) }
        val timestamp = (System.currentTimeMillis() / 1000).toString()

        val oauth = sortedMapOf(
            "oauth_consumer_key" to consumerKey,
            "oauth_nonce" to nonce,
            "oauth_signature_method" to "HMAC-SHA1",
            "oauth_timestamp" to timestamp,
            "oauth_version" to "1.0"
        )
        token?.let { oauth["oauth_token"] = it }

        // The base string sorts oauth params and query params together.
        val all = (oauth + params).toSortedMap()
        val paramString = all.entries.joinToString("&") { "${encode(it.key)}=${encode(it.value)}" }
        val base = "${method.uppercase()}&${encode(url)}&${encode(paramString)}"

        val signingKey = "${encode(consumerSecret)}&${encode(tokenSecret ?: "")}"
        val mac = Mac.getInstance("HmacSHA1").apply {
            init(SecretKeySpec(signingKey.toByteArray(), "HmacSHA1"))
        }
        val signature = Base64.encodeToString(mac.doFinal(base.toByteArray()), Base64.NO_WRAP)

        val headerParams = oauth + mapOf("oauth_signature" to signature)
        return "OAuth " + headerParams.entries.joinToString(", ") {
            "${encode(it.key)}=\"${encode(it.value)}\""
        }
    }
}
