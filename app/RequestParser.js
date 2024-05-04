const assert = require("assert");

/**
 * A class for parsing Redis-like protocol messages from a buffer.
 */
class RequestParser {
  /**
   * Custom error class for handling incomplete request errors.
   */
  static PartialRequestError = class PartialRequestError extends Error {
    constructor() {
      super("Index out of bound while parsing request");
      this.name = "Partial Request";
    }
  };

  /**
   * Constructs a new RequestParser instance.
   * @param {string} buffer - The buffer containing the request data.
   */
  constructor(buffer) {
    this.request = buffer;
    this.cursor = 0; // Tracks the current position in the buffer.
    this.currentRequest = ""; // Holds the current request being parsed.
  }

  /**
   * Parses the buffer and extracts the arguments from the Redis-like request.
   * @returns {Array<string>} An array of arguments extracted from the request.
   */
  parse() {
    const startCursor = this.cursor; // Remember the starting position for rollback.
    this.args = []; // Initialize arguments list.
    try {
      assert.equal(this.curr(), "*", "Start of request should be *"); // Check for the correct start symbol.
      this.cursor++;
      const numOfArgs = this.readNum(); // Read the number of arguments.
      for (let i = 0; i < numOfArgs; i++) {
        this.args.push(this.readBulkString()); // Read each bulk string argument.
      }
    } catch (err) {
      this.cursor = startCursor; // Rollback to the start position on error.
      this.args = [];
      this.currentRequest = "";
    } finally {
      this.currentRequest = this.request.slice(startCursor, this.cursor); // Update the current request.
      return this.args;
    }
  }

  /**
   * Reads a number from the buffer until a carriage return.
   * @returns {number} The parsed number.
   */
  readNum() {
    let num = 0;
    while (this.curr() !== "\r") {
      num = num * 10 + (this.curr() - "0"); // Convert character to digit and accumulate.
      this.cursor++;
    }
    this.cursor += 2; // Skip the carriage return and newline.
    return num;
  }

  /**
   * Reads a bulk string from the buffer.
   * @returns {string} The bulk string.
   */
  readBulkString() {
    assert.equal(this.curr(), "$", "Start of bulk string should be $");
    this.cursor++;
    const lenOfString = this.readNum(); // Read the length of the bulk string.
    const string = this.getString(lenOfString); // Extract the string.
    return string;
  }

  /**
   * Retrieves a string from the buffer of a specified length.
   * @param {number} lenOfString - The length of the string to extract.
   * @returns {string} The extracted string.
   * @throws {PartialRequestError} If the buffer does not contain enough characters.
   */
  getString(lenOfString) {
    if (this.request.length < this.cursor + lenOfString) {
      throw new RequestParser.PartialRequestError(); // Throw error if the buffer is too short.
    }
    const ret = this.request.slice(this.cursor, this.cursor + lenOfString);
    this.cursor += lenOfString + 2; // Move past the string and the subsequent carriage return and newline.
    return ret;
  }

  /**
   * Retrieves the remaining part of the buffer after the current cursor position.
   * @returns {string} The remaining buffer.
   */
  getRemainingRequest() {
    return this.request.slice(this.cursor);
  }

  /**
   * Gets the current character in the buffer at the cursor's position.
   * @returns {string} The current character.
   * @throws {PartialRequestError} If the cursor is out of bounds.
   */
  curr() {
    if (this.cursor < 0 || this.cursor >= this.request.length) {
      throw new RequestParser.PartialRequestError(); // Validate cursor position.
    }
    return this.request[this.cursor];
  }
}

module.exports = RequestParser;
