package io.imply.druid.example.indexer;

import com.google.common.annotations.VisibleForTesting;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.data.input.ByteBufferInputRowParser;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.Parser;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.List;
import java.util.Objects;

public class ExampleByteBufferInputRowParser implements ByteBufferInputRowParser
{
  public static final String TYPE_NAME = "exampleParser";

  private final ParseSpec parseSpec;
  private Parser<String, Object> parser;
  private MapInputRowParser mapParser;

  private final Base64.Decoder base64Decoder = Base64.getDecoder();

  @JsonCreator
  public ExampleByteBufferInputRowParser(
      @JsonProperty("parseSpec") ParseSpec parseSpec
  )
  {
    this.parseSpec = parseSpec;
  }

  @JsonProperty
  @Override
  public ParseSpec getParseSpec()
  {
    return parseSpec;
  }

  @Override
  public ByteBufferInputRowParser withParseSpec(ParseSpec parseSpec)
  {
    return new ExampleByteBufferInputRowParser(parseSpec);
  }

  @Override
  public List<InputRow> parseBatch(ByteBuffer input)
  {
    if (parser == null) {
      parser = parseSpec.makeParser();
      mapParser = new MapInputRowParser(parseSpec);
    }
    String stringInput = decodeRot13Base64(input);
    return mapParser.parseBatch(parser.parseToMap(stringInput));
  }

  public String decodeRot13Base64(ByteBuffer input)
  {
    String rot13encodedBase64 = StringUtils.fromUtf8(input);
    String base64 = rot13(rot13encodedBase64);
    return StringUtils.fromUtf8(base64Decoder.decode(base64));
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ExampleByteBufferInputRowParser that = (ExampleByteBufferInputRowParser) o;
    return parseSpec.equals(that.parseSpec);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(parseSpec);
  }

  @Override
  public String toString()
  {
    return "ExampleByteBufferInputRowParser{" +
           "parseSpec=" + parseSpec +
           '}';
  }

  @VisibleForTesting
  static String rot13(String rot13String)
  {
    char[] rotated = new char[rot13String.length()];
    for (int i = 0; i < rot13String.length(); i++) {
      char c = rot13String.charAt(i);
      if (c >= 'a' && c <= 'm') {
        c += 13;
      } else if (c >= 'A' && c <= 'M') {
        c += 13;
      } else if (c >= 'n' && c <= 'z') {
        c -= 13;
      } else if (c >= 'N' && c <= 'Z') {
        c -= 13;
      }
      rotated[i] = c;
    }
    return new String(rotated);
  }
}
