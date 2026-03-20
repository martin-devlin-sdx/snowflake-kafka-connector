package com.snowflake.kafka.connector.internal.streaming.v2.migration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.snowflake.kafka.connector.internal.SnowflakeURL;
import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.util.OptionalLong;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class Ssv1OffsetReaderTest {

  private static PrivateKey testPrivateKey;
  private static final SnowflakeURL TEST_URL =
      new SnowflakeURL("https://testaccount.snowflakecomputing.com:443");

  @BeforeAll
  static void generateTestKey() throws Exception {
    KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
    keyGen.initialize(2048);
    KeyPair keyPair = keyGen.generateKeyPair();
    testPrivateKey = keyPair.getPrivate();
  }

  @SuppressWarnings("unchecked")
  private Ssv1OffsetReader createReaderWithMockedResponse(String responseBody) throws Exception {
    HttpClient mockHttpClient = mock(HttpClient.class);
    HttpResponse<String> mockResponse = mock(HttpResponse.class);
    when(mockResponse.body()).thenReturn(responseBody);
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    return new Ssv1OffsetReader(
        TEST_URL, "testuser", "testrole", testPrivateKey, "testdb", "testschema", mockHttpClient);
  }

  @SuppressWarnings("unchecked")
  private Ssv1OffsetReader createReaderWithNetworkError() throws Exception {
    HttpClient mockHttpClient = mock(HttpClient.class);
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenThrow(new IOException("Connection refused"));

    return new Ssv1OffsetReader(
        TEST_URL, "testuser", "testrole", testPrivateKey, "testdb", "testschema", mockHttpClient);
  }

  @Test
  void readCommittedOffset_successWithOffset() throws Exception {
    String response = "{\"status_code\": 0, \"offset_token\": \"42\", \"message\": \"Success\"}";
    Ssv1OffsetReader reader = createReaderWithMockedResponse(response);

    OptionalLong result = reader.readCommittedOffset("test_table", "test_channel");

    assertTrue(result.isPresent());
    assertEquals(42L, result.getAsLong());
  }

  @Test
  void readCommittedOffset_successWithNullOffset() throws Exception {
    String response = "{\"status_code\": 0, \"offset_token\": null, \"message\": \"Success\"}";
    Ssv1OffsetReader reader = createReaderWithMockedResponse(response);

    OptionalLong result = reader.readCommittedOffset("test_table", "test_channel");

    assertTrue(result.isEmpty());
  }

  @Test
  void readCommittedOffset_channelDoesNotExist_statusCode25() throws Exception {
    String response =
        "{\"status_code\": 25, \"message\": \"Channel does not exist or is not authorized\"}";
    Ssv1OffsetReader reader = createReaderWithMockedResponse(response);

    OptionalLong result = reader.readCommittedOffset("test_table", "test_channel");

    assertTrue(result.isEmpty());
  }

  @Test
  void readCommittedOffset_channelNoLongerExists_statusCode19() throws Exception {
    String response = "{\"status_code\": 19, \"message\": \"Channel no longer exists\"}";
    Ssv1OffsetReader reader = createReaderWithMockedResponse(response);

    OptionalLong result = reader.readCommittedOffset("test_table", "test_channel");

    assertTrue(result.isEmpty());
  }

  @Test
  void readCommittedOffset_unexpectedStatusCode_throws() throws Exception {
    String response = "{\"status_code\": 99, \"message\": \"Unknown error\"}";
    Ssv1OffsetReader reader = createReaderWithMockedResponse(response);

    Ssv1OffsetReadException exception =
        assertThrows(
            Ssv1OffsetReadException.class,
            () -> reader.readCommittedOffset("test_table", "test_channel"));

    assertTrue(exception.getMessage().contains("status_code=99"));
  }

  @Test
  void readCommittedOffset_networkError_throws() throws Exception {
    Ssv1OffsetReader reader = createReaderWithNetworkError();

    Ssv1OffsetReadException exception =
        assertThrows(
            Ssv1OffsetReadException.class,
            () -> reader.readCommittedOffset("test_table", "test_channel"));

    assertTrue(exception.getMessage().contains("Network error"));
    assertTrue(exception.getCause() instanceof IOException);
  }

  @Test
  void readCommittedOffset_nonNumericOffset_throws() throws Exception {
    String response =
        "{\"status_code\": 0, \"offset_token\": \"not_a_number\", \"message\": \"Success\"}";
    Ssv1OffsetReader reader = createReaderWithMockedResponse(response);

    Ssv1OffsetReadException exception =
        assertThrows(
            Ssv1OffsetReadException.class,
            () -> reader.readCommittedOffset("test_table", "test_channel"));

    assertTrue(exception.getMessage().contains("non-numeric offset token"));
  }

  @Test
  void readCommittedOffset_malformedJson_throws() throws Exception {
    Ssv1OffsetReader reader = createReaderWithMockedResponse("not json at all");

    assertThrows(
        Ssv1OffsetReadException.class,
        () -> reader.readCommittedOffset("test_table", "test_channel"));
  }

  @Test
  void readCommittedOffset_missingOffsetTokenField_returnsEmpty() throws Exception {
    String response = "{\"status_code\": 0, \"message\": \"Success\"}";
    Ssv1OffsetReader reader = createReaderWithMockedResponse(response);

    OptionalLong result = reader.readCommittedOffset("test_table", "test_channel");

    assertTrue(result.isEmpty());
  }
}
