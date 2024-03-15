package dev.luxor.server.shared;

/**
 * A utility class for argument validations that throw on invalid values.
 *
 * @author houthacker
 */
public class Ensure {

  private Ensure() {
    throw new UnsupportedOperationException(
        "Ensure is a utility class; don't create instances of it.");
  }

  /**
   * Ensures the value of {@code subject} is at least {@code atLeast}.
   *
   * @param subject The subject to compare.
   * @param atLeast The minimum value for {@code subject}.
   * @return {@code subject}.
   * @throws IllegalArgumentException if {@code subject < atLeast}.
   */
  public static long ensureAtLeast(final long subject, final long atLeast) {
    if (subject < atLeast) {
      throw new IllegalArgumentException(String.format("%d must be at least %d", subject, atLeast));
    }

    return subject;
  }

  /**
   * Ensures the value of {@code subject} is at least {@code atLeast}.
   *
   * @param subject The subject to compare.
   * @param atLeast The minimum value for {@code subject}.
   * @return {@code subject}.
   * @throws IllegalArgumentException if {@code subject < atLeast}.
   */
  public static int ensureAtLeast(final int subject, final int atLeast) {
    if (subject < atLeast) {
      throw new IllegalArgumentException(String.format("%d must be at least %d", subject, atLeast));
    }

    return subject;
  }

  /**
   * Ensures {@code subject} is at least zero.
   *
   * @param subject The subject value.
   * @return {@code subject}.
   * @throws IllegalArgumentException if {@code subject < 0}.
   */
  public static long ensureAtLeastZero(final long subject) {
    return ensureAtLeast(subject, 0L);
  }

  /**
   * Ensures {@code subject} is at least zero.
   *
   * @param subject The subject value.
   * @return {@code subject}.
   * @throws IllegalArgumentException if {@code subject < 0}.
   */
  public static int ensureAtLeastZero(final int subject) {
    return ensureAtLeast(subject, 0);
  }

  /**
   * Ensures {@code subject} is at least one.
   *
   * @param subject The subject value.
   * @return {@code subject}.
   * @throws IllegalArgumentException if {@code subject < 1}.
   */
  public static long ensureAtLeastOne(final long subject) {
    return ensureAtLeast(subject, 1L);
  }

  /**
   * Ensures {@code subject} is at least one.
   *
   * @param subject The subject value.
   * @return {@code subject}.
   * @throws IllegalArgumentException if {@code subject < 1}.
   */
  public static int ensureAtLeastOne(final int subject) {
    return ensureAtLeast(subject, 1);
  }
}
