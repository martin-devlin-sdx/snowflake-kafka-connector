package com.snowflake.kafka.connector.internal.streaming.v2.migration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.SnowflakeURL;
import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.MessageDigest;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.interfaces.RSAPrivateCrtKey;
import java.security.spec.RSAPublicKeySpec;
import java.time.Duration;
import java.util.Base64;
import java.util.OptionalLong;
import java.util.UUID;

/**
 * Reads committed offset tokens from SSv1 channels by calling the SSv1 REST API directly. Uses
 * {@code POST /v1/streaming/channels/open/} to reopen the channel and read its last committed
 * offset.
 *
 * <p>JWT authentication is generated from the connector's private key using only JDK standard
 * library APIs (RS256).
 */
public class Ssv1OffsetReader implements Closeable {

  private static final KCLogger LOGGER = new KCLogger(Ssv1OffsetReader.class.getName());
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Duration HTTP_TIMEOUT = Duration.ofSeconds(30);

  private static final String OPEN_CHANNEL_ENDPOINT = "/v1/streaming/channels/open/";
  private static final String BEARER_PREFIX = "Bearer ";
  private static final String AUTH_TOKEN_TYPE_HEADER = "X-Snowflake-Authorization-Token-Type";
  private static final String KEYPAIR_JWT_TOKEN_TYPE = "KEYPAIR_JWT";

  /** SSv1 response status codes where we know the channel doesn't exist (safe to return empty). */
  private static final long ERR_CHANNEL_DOES_NOT_EXIST_OR_IS_NOT_AUTHORIZED = 25;

  private static final long ERR_CHANNEL_NO_LONGER_EXISTS = 19;
  private static final long RESPONSE_SUCCESS = 0;

  private final String baseUrl;
  private final String role;
  private final String database;
  private final String schema;
  private final PrivateKey privateKey;

  // Pre-computed JWT components
  private final String jwtIssuer;
  private final String jwtSubject;
  private final String jwtHeaderEncoded;

  private final HttpClient httpClient;

  // Cached JWT and its expiry
  private volatile String cachedJwt;
  private volatile long cachedJwtExpiresAtMillis;

  public Ssv1OffsetReader(
      SnowflakeURL url,
      String user,
      String role,
      PrivateKey privateKey,
      String database,
      String schema) {
    this(
        url,
        user,
        role,
        privateKey,
        database,
        schema,
        HttpClient.newBuilder().connectTimeout(HTTP_TIMEOUT).build());
  }

  /** Package-private constructor for unit testing with a custom HttpClient. */
  Ssv1OffsetReader(
      SnowflakeURL url,
      String user,
      String role,
      PrivateKey privateKey,
      String database,
      String schema,
      HttpClient httpClient) {
    this.baseUrl = url.getScheme() + "://" + url.hostWithPort();
    this.role = role;
    this.database = database;
    this.schema = schema;
    this.privateKey = privateKey;

    String normalizedAccount = normalizeAccount(url.getAccount());
    String normalizedUser = user.toUpperCase();

    String publicKeyFingerprint = computePublicKeyFingerprint(privateKey);
    this.jwtIssuer = normalizedAccount + "." + normalizedUser + "." + publicKeyFingerprint;
    this.jwtSubject = normalizedAccount + "." + normalizedUser;
    this.jwtHeaderEncoded =
        Base64.getUrlEncoder()
            .withoutPadding()
            .encodeToString("{\"alg\":\"RS256\",\"typ\":\"JWT\"}".getBytes(StandardCharsets.UTF_8));

    this.httpClient = httpClient;
  }

  /**
   * Reads the committed offset token from an SSv1 channel by calling the openChannel REST API.
   *
   * @param tableName the Snowflake table name
   * @param channelName the SSv1 channel name (typically connectorName_topic_partition)
   * @return the committed offset as a long, or empty if the channel doesn't exist or has no
   *     committed data
   * @throws Ssv1OffsetReadException for transient or unexpected errors (network failures, auth
   *     errors, unexpected status codes). The caller must NOT silently proceed when this is thrown.
   */
  public OptionalLong readCommittedOffset(String tableName, String channelName) {
    LOGGER.info("Reading SSv1 committed offset for table={}, channel={}", tableName, channelName);

    String requestId = UUID.randomUUID().toString();
    String requestBody = buildOpenChannelRequestBody(requestId, tableName, channelName);
    String jwt = getOrRefreshJwt();

    URI uri = URI.create(baseUrl + OPEN_CHANNEL_ENDPOINT + "?requestId=" + requestId);
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(uri)
            .timeout(HTTP_TIMEOUT)
            .header("Authorization", BEARER_PREFIX + jwt)
            .header(AUTH_TOKEN_TYPE_HEADER, KEYPAIR_JWT_TOKEN_TYPE)
            .header("Accept", "application/json")
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(requestBody))
            .build();

    HttpResponse<String> response;
    try {
      response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    } catch (IOException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      throw new Ssv1OffsetReadException(
          "Network error reading SSv1 offset for channel " + channelName, e);
    }

    return parseResponse(response.body(), channelName);
  }

  @Override
  public void close() {
    // HttpClient has no explicit close in JDK 11
  }

  private OptionalLong parseResponse(String responseBody, String channelName) {
    JsonNode root;
    try {
      root = MAPPER.readTree(responseBody);
    } catch (IOException e) {
      throw new Ssv1OffsetReadException(
          "Failed to parse SSv1 openChannel response for channel " + channelName, e);
    }

    long statusCode = root.path("status_code").asLong(-1);

    if (statusCode == RESPONSE_SUCCESS) {
      JsonNode offsetTokenNode = root.path("offset_token");
      if (offsetTokenNode.isNull() || offsetTokenNode.isMissingNode()) {
        LOGGER.info("SSv1 channel {} exists but has no committed offset token", channelName);
        return OptionalLong.empty();
      }
      String offsetToken = offsetTokenNode.asText();
      try {
        long offset = Long.parseLong(offsetToken);
        LOGGER.info("SSv1 channel {} has committed offset {}", channelName, offset);
        return OptionalLong.of(offset);
      } catch (NumberFormatException e) {
        throw new Ssv1OffsetReadException(
            "SSv1 channel " + channelName + " has non-numeric offset token: " + offsetToken, e);
      }
    }

    if (statusCode == ERR_CHANNEL_DOES_NOT_EXIST_OR_IS_NOT_AUTHORIZED
        || statusCode == ERR_CHANNEL_NO_LONGER_EXISTS) {
      LOGGER.info(
          "SSv1 channel {} does not exist (status_code={}), no offset to migrate",
          channelName,
          statusCode);
      return OptionalLong.empty();
    }

    String message = root.path("message").asText("(no message)");
    throw new Ssv1OffsetReadException(
        "SSv1 openChannel for "
            + channelName
            + " returned unexpected status_code="
            + statusCode
            + ", message="
            + message);
  }

  private String buildOpenChannelRequestBody(
      String requestId, String tableName, String channelName) {
    try {
      return MAPPER
          .createObjectNode()
          .put("request_id", requestId)
          .put("role", role)
          .put("channel", channelName)
          .put("table", tableName)
          .put("database", database)
          .put("schema", schema)
          .put("write_mode", "CLOUD_STORAGE")
          .put("is_iceberg", false)
          .toString();
    } catch (Exception e) {
      throw new Ssv1OffsetReadException("Failed to build SSv1 openChannel request body", e);
    }
  }

  // --- JWT generation ---

  private synchronized String getOrRefreshJwt() {
    long nowMillis = System.currentTimeMillis();
    // Refresh if no cached token or within 60 seconds of expiry
    if (cachedJwt == null || nowMillis >= cachedJwtExpiresAtMillis - 60_000) {
      long iatSeconds = nowMillis / 1000;
      long expSeconds = iatSeconds + 59 * 60; // 59 minutes, matching SSv1 SDK JWTManager
      cachedJwt = generateJwt(iatSeconds, expSeconds);
      cachedJwtExpiresAtMillis = expSeconds * 1000;
    }
    return cachedJwt;
  }

  private String generateJwt(long iatSeconds, long expSeconds) {
    String claimsJson =
        "{\"iss\":\""
            + jwtIssuer
            + "\",\"sub\":\""
            + jwtSubject
            + "\",\"iat\":"
            + iatSeconds
            + ",\"exp\":"
            + expSeconds
            + "}";

    Base64.Encoder encoder = Base64.getUrlEncoder().withoutPadding();
    String claimsEncoded = encoder.encodeToString(claimsJson.getBytes(StandardCharsets.UTF_8));
    String signingInput = jwtHeaderEncoded + "." + claimsEncoded;

    try {
      Signature sig = Signature.getInstance("SHA256withRSA");
      sig.initSign(privateKey);
      sig.update(signingInput.getBytes(StandardCharsets.UTF_8));
      String signatureEncoded = encoder.encodeToString(sig.sign());
      return signingInput + "." + signatureEncoded;
    } catch (Exception e) {
      throw new Ssv1OffsetReadException("Failed to generate JWT for SSv1 REST API", e);
    }
  }

  /**
   * Computes the public key fingerprint in the format expected by Snowflake JWT: {@code
   * SHA256:<base64(sha256(DER-encoded-public-key))>}.
   */
  private static String computePublicKeyFingerprint(PrivateKey privateKey) {
    try {
      PublicKey publicKey = derivePublicKey(privateKey);
      byte[] publicKeyDer = publicKey.getEncoded();
      MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
      byte[] digest = sha256.digest(publicKeyDer);
      return "SHA256:" + Base64.getEncoder().encodeToString(digest);
    } catch (Exception e) {
      throw new Ssv1OffsetReadException("Failed to compute public key fingerprint", e);
    }
  }

  private static PublicKey derivePublicKey(PrivateKey privateKey) throws Exception {
    if (privateKey instanceof RSAPrivateCrtKey) {
      RSAPrivateCrtKey rsaKey = (RSAPrivateCrtKey) privateKey;
      RSAPublicKeySpec publicKeySpec =
          new RSAPublicKeySpec(rsaKey.getModulus(), rsaKey.getPublicExponent());
      return KeyFactory.getInstance("RSA").generatePublic(publicKeySpec);
    }
    throw new IllegalArgumentException(
        "Cannot derive public key from private key of type " + privateKey.getAlgorithm());
  }

  /** Normalize account: strip region/cloud suffix after first dot, uppercase. */
  private static String normalizeAccount(String account) {
    int dotIndex = account.indexOf('.');
    String normalized = dotIndex > 0 ? account.substring(0, dotIndex) : account;
    return normalized.toUpperCase();
  }
}
