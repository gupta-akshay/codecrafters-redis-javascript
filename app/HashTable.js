/**
 * A simple hash table implementation with support for expiry timestamps.
 */
class HashTable {
   /**
   * Constructs a new HashTable instance.
   */
  constructor() {
    this.map = new Map();
  }

  /**
   * Inserts a value into the hash table without an explicit expiry.
   * Defaults to 24 hours.
   * @param {string} key - The key under which to store the value.
   * @param {any} value - The value to store.
   */
  insert(key, value) {
    this.insertWithExpiry(key, value, 1000 * 60 * 60 * 24);
  }

  /**
   * Inserts a value with an expiry time.
   * @param {string} key - The key under which to store the value.
   * @param {any} value - The value to store.
   * @param {number} expiry - The expiry duration in milliseconds.
   */
  insertWithExpiry(key, value, expiry) {
    const expiryTimestamp = parseInt(expiry, 10) + Date.now();
    this.insertKeyWithTimeStamp(key, value, expiryTimestamp);
  }

  /**
   * Inserts a value with a specific expiry timestamp.
   * @param {string} key - The key under which to store the value.
   * @param {any} value - The value to store.
   * @param {number} timestamp - The specific timestamp at which the value should expire.
   */
  insertKeyWithTimeStamp(key, value, timestamp) {
    this.map.set(key, { value, expiry: timestamp, type: "string" });
  }

  /**
   * Retrieves a value by its key if it hasn't expired.
   * @param {string} key - The key whose value is to be retrieved.
   * @returns {any|null} The value if found and not expired, otherwise null.
   */
  get(key) {
    if (this.has(key)) {
      return this.map.get(key).value;
    }
    return null;
  }

  /**
   * Checks if a key exists and hasn't expired.
   * @param {string} key - The key to check.
   * @returns {boolean} True if the key exists and hasn't expired, otherwise false.
   */
  has(key) {
    if (!this.map.has(key)) return false;
    if (this.map.get(key).expiry < Date.now()) {
      this.map.delete(key);
      return false;
    }
    return true;
  }
}

module.exports = HashTable;
