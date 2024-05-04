/**
 * A utility class for encoding different types of data into specific
 * Redis protocol formats.
 */
class Encoder {
  /**
   * Encodes a simple string in Redis protocol format.
   * @param {string} string - The string to encode.
   * @returns {string} Encoded simple string.
   */
  static createSimpleString(string) {
    return `+${string}\r\n`;
  }

  /**
   * Encodes a bulk string or a null value in Redis protocol format.
   * @param {string} string - The string to encode.
   * @param {boolean} [isNull=false] - Flag to determine if the output should be a null bulk string.
   * @returns {string} Encoded bulk string or a null representation.
   */
  static createBulkString(string, isNull = false) {
    if (!isNull) {
      return `$${string.length}\r\n${string}\r\n`;
    }
    return `$-1\r\n`;
  }

  /**
   * Encodes an array of encoded strings in Redis protocol format.
   * @param {string[]} arr - The array of strings to encode.
   * @returns {string} Encoded array.
   */
  static createArray(arr) {
    return `*${arr.length}\r\n${arr.join("")}`;
  }

  /**
   * Encodes an integer in Redis protocol format.
   * @param {number} num - The number to encode.
   * @returns {string} Encoded integer.
   */
  static createInteger(num) {
    return `:${num}\r\n`;
  }

  /**
   * Encodes a simple error message in Redis protocol format.
   * @param {string} message - The error message to encode.
   * @returns {string} Encoded error message.
   */
  static createSimpleError(message) {
    return `-${message}\r\n`;
  }
}

module.exports = Encoder;
