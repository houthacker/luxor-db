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
    assertEquals(1, serial.referenceCount(), "Expect FileSerial to be referenced exactly once.");

    final FileSerial same = FileSerial.find(temp);
    assertEquals(2, serial.referenceCount(), "Expect FileSerial to be referenced exactly twice.");
    assertSame(serial, same, "Expect same FileSerial for identical paths.");

    same.dereference();
    serial.dereference();
    assertEquals(0, same.referenceCount(), "Expect no more references to FileSerial.");
  }
}
