package dev.luxor.server.algo;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

class FNV1aTest {

  @Test
  void testIterate() {
    final ByteBuffer input = ByteBuffer.wrap(new byte[] {'d', 'e', 'a', 'd', 'b', 'e', 'e', 'f'});
    final long state = new FNV1a().iterate(input, 0, input.limit()).state();

    assertEquals(0x34b63b485f5df51dL, state);
  }
}
