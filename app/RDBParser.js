const HashTable = require("./HashTable");

/**
 * Represents a Redis RDB Parser.
 * Parses the Redis RDB (Redis Database) file format and extracts data from it.
 */
class RDBParser {
  static CONSTANTS = {
    MAGIC_REDIS_STRING: 5,
    RDB_VERSION: 4,
  };

  /**
   * Represents the available OPCodes.
   * @type {Object}
   */
  static OPCodes = {
    AUX: 0xfa,
    RESIZEDB: 0xfb,
    EXPIRETIMEMS: 0xfc,
    EXPIRETIME: 0xfd,
    SELECTDB: 0xfe,
    EOF: 0xff,
  };

  constructor(buffer) {
    this.buffer = buffer;
    this.cursor = 0;
    this.dataStore = new HashTable();

    this.auxData = {};
  }

  /**
   * Parses the Redis RDB file.
   */
  parse() {
    let redisMagicString = this.readStringOfLen(
      RDBParser.CONSTANTS.MAGIC_REDIS_STRING
    );
    let rdbVersion = this.readStringOfLen(RDBParser.CONSTANTS.RDB_VERSION);

    while (true) {
      const opCode = this.readByte();
      switch (opCode) {
        case RDBParser.OPCodes.AUX:
          this.readAUX();
          break;

        case RDBParser.OPCodes.RESIZEDB:
          this.readResizeDB();
          break;

        case RDBParser.OPCodes.EXPIRETIMEMS:
          this.readExpireTimeMS();
          break;

        case RDBParser.OPCodes.EXPIRETIME:
          this.readExpireTime();
          break;

        case RDBParser.OPCodes.SELECTDB:
          this.readSelectDB();
          break;

        case RDBParser.OPCodes.EOF:
          this.readEOF();
          return;
        default:
          this.readKeyWithoutExpiry(opCode);
          break;
      }
    }
  }

  /**
   * Reads the AUX data from the RDB file and stores it in the `auxData` object.
   * @returns {void}
   */
  readAUX() {
    let key = this.readStringEncoding();
    let value = this.readStringEncoding();
    this.auxData[key] = value;
  }

  /**
   * Reads and parses the resize database command from the RDB file.
   */
  readResizeDB() {
    let hashTableSize = this.readLengthEncoding().value;
    let expireHashTableSize = this.readLengthEncoding().value;
  }

  /**
   * Reads the expire time in milliseconds from the RDB file and inserts the key-value pair with timestamp into the data store.
   */
  readExpireTimeMS() {
    let timestamp = this.read8Bytes();
    let valueType = this.readValueType();
    let key = this.readStringEncoding();
    let value = this.readValue(valueType);

    this.dataStore.insertKeyWithTimeStamp(key, value, timestamp);
  }

  /**
   * Reads the expire time, key, and value from the RDB file and inserts the key-value pair into the data store with the specified timestamp.
   */
  readExpireTime() {
    let timestamp = this.read4Bytes() * 1000;
    let valueType = this.readValueType();
    let key = this.readStringEncoding();
    let value = this.readValue(valueType);

    this.dataStore.insertKeyWithTimeStamp(key, value, timestamp);
  }

  /**
   * Reads and parses the SELECTDB command from the RDB file.
   * @returns {void}
   */
  readSelectDB() {
    let { type, value } = this.readLengthEncoding();
  }

  readEOF() {}

  /**
   * Reads a key without expiry from the RDB file and inserts it into the data store.
   * @param {string} valueType - The type of the value associated with the key.
   * @returns {void}
   */
  readKeyWithoutExpiry(valueType) {
    let key = this.readStringEncoding();
    let value = this.readValue(valueType);
    this.dataStore.insert(key, value);
  }

  /**
   * Reads and returns the string encoding from the input.
   * 
   * @returns {string} The string encoding value.
   * @throws {Error} If there is an error while reading the string encoding.
   */
  readStringEncoding() {
    let { type, value } = this.readLengthEncoding();

    if (type === "length") {
      let length = value;
      return this.readStringOfLen(length);
    }

    if (value === 0) {
      return `${this.readByte()}`;
    } else if (value === 1) {
      return `${this.read2Bytes()}`;
    } else if (value === 2) {
      return `${this.read4Bytes()}`;
    }

    throw new Error("Error while reading string encoding");
  }

  /**
   * Reads the length encoding from the input stream.
   * 
   * @returns {Object} An object containing the type and value of the length encoding.
   * @throws {Error} If an error occurs while reading the length encoding.
   */
  readLengthEncoding() {
    let firstByte = this.readByte();
    let twoBits = firstByte >> 6;

    let value = 0;
    let type = "length";
    if (twoBits === 0b00) {
      value = firstByte & 0b00111111;
    } else if (twoBits === 0b01) {
      let secondByte = this.readByte();
      value = ((firstByte & 0b00111111) << 8) | secondByte;
    } else if (twoBits === 0b10) {
      value = this.read4Bytes();
    } else if (twoBits === 0b11) {
      type = "format";
      value = firstByte & 0b00111111;
    } else {
      throw new Error(
        `Error while reading length encoding, got first byte as : ${firstByte}`
      );
    }
    return { type, value };
  }

  /**
   * Reads the value type from the input.
   * @returns {number} The value type.
   */
  readValueType() {
    return this.readByte();
  }

  /**
   * Reads and returns the value based on the given value type.
   *
   * @param {number} valueType - The type of the value.
   * @returns {string} - The parsed value.
   * @throws {Error} - If the value type is not handled.
   */
  readValue(valueType) {
    if (valueType == 0) {
      return this.readStringEncoding();
    }
    throw new Error(`Value Type not handled: ${valueType}`);
  }

  /**
   * Reads a byte from the buffer and advances the cursor.
   * @returns {number} The byte read from the buffer.
   */
  readByte() {
    return this.buffer[this.cursor++];
  }

  /**
   * Reads 2 bytes from the buffer and advances the cursor by 2.
   * 
   * @returns {number} The 2 bytes read from the buffer.
   */
  read2Bytes() {
    let bytes = this.buffer.readUInt16LE(this.cursor);
    this.cursor += 2;
    return bytes;
  }

  /**
   * Reads 4 bytes from the buffer and returns the value as an unsigned 32-bit integer.
   * Advances the cursor by 4 bytes.
   *
   * @returns {number} The value read from the buffer as an unsigned 32-bit integer.
   */
  read4Bytes() {
    let bytes = this.buffer.readUInt32LE(this.cursor);
    this.cursor += 4;
    return bytes;
  }

  /**
   * Reads 8 bytes from the buffer and advances the cursor by 8.
   * 
   * @returns {BigInt} The 8 bytes read from the buffer.
   */
  read8Bytes() {
    let bytes = this.buffer.readBigUint64LE(this.cursor);
    this.cursor += 8;
    return bytes;
  }

  /**
   * Reads a string of specified length from the buffer and advances the cursor.
   *
   * @param {number} len - The length of the string to read.
   * @returns {string} - The string read from the buffer.
   */
  readStringOfLen(len) {
    let string = String.fromCharCode(
      ...this.buffer.subarray(this.cursor, this.cursor + len)
    );
    this.cursor += len;
    return string;
  }
}

module.exports = RDBParser;
