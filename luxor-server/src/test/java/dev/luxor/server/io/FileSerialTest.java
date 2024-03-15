package dev.luxor.server.io;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class FileSerialTest {

  @Test
  void testAcquireRelease() throws IOException {
    final Path temp = Files.createTempFile("FileSerialTest-", ".tmp");

    final FileSerial serial = FileSerial.find(temp);
    assertNotNull(serial, String.format("Expect an FileSerial to be found for path %s", temp));
    assertEquals(serial.referenceCount(), 1, "Expect FileSerial to be referenced exactly once.");

    final FileSerial same = FileSerial.find(temp);
    assertEquals(serial.referenceCount(), 2, "Expect FileSerial to be referenced exactly twice.");
    assertSame(serial, same, "Expect same FileSerial for identical paths.");

    same.dereference();
    serial.dereference();
    assertEquals(same.referenceCount(), 0, "Expect no more references to FileSerial.");
  }
}
