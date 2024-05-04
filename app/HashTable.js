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
   * Inserts a value into the hash table as a stream.
   *
   * @param {string} key - The key to insert the value under.
   * @param {object} value - The value to insert.
   * @returns {string|null} - The ID of the inserted value, or null if insertion is unsuccessful.
   */
  insertStream(key, value) {
    if (!this.map.has(key)) this.map.set(key, { value: [], type: "stream" });
    let existingValue = this.map.get(key);

    let currentId = value.id;

    if (currentId === "*") {
      currentId = `${Date.now()}-*`;
    }

    let currentIdMillis = currentId.split("-")[0];
    let currentIdSequence = currentId.split("-")[1];

    if (currentIdSequence === "*") {
      if (existingValue.value.length === 0) {
        if (currentIdMillis === "0") currentIdSequence = "1";
        else currentIdSequence = "0";
        currentId = currentIdMillis + "-" + currentIdSequence;
      } else {
        let lastEntry = existingValue.value.slice(-1)[0];
        let lastId = lastEntry["id"].split("-");
        let lastIdMilisecond = lastId[0];
        let lastIdSequence = lastId[1];

        if (currentIdMillis < lastIdMilisecond) return null;
        if (currentIdMillis === lastIdMilisecond) {
          currentIdSequence = `${parseInt(lastIdSequence) + 1}`;
          currentId = currentIdMillis + "-" + currentIdSequence;
        } else {
          currentIdSequence = "0";
          currentId = currentIdMillis + "-" + currentIdSequence;
        }
      }
      value["id"] = currentId;
      existingValue.value.push(value);
      this.map.set(key, existingValue);
      return currentId;
    }

    if (existingValue.value.length !== 0) {
      let lastEntry = existingValue.value.slice(-1)[0];
      let lastId = lastEntry["id"];
      let lastIdMilisecond = lastId.split("-")[0];
      let lastIdSequence = lastId.split("-")[1];

      if (currentIdMillis < lastIdMilisecond) return null;
      if (
        currentIdMillis === lastIdMilisecond &&
        currentIdSequence <= lastIdSequence
      )
        return null;
    }

    existingValue.value.push(value);
    this.map.set(key, existingValue);
    return currentId;
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
   * Retrieves the type of the value associated with the given key.
   *
   * @param {any} key - The key to retrieve the type for.
   * @returns {string|null} The type of the value associated with the key, or null if the key does not exist.
   */
  getType(key) {
    if (this.has(key)) {
      return this.map.get(key).type;
    }
    return null;
  }

  /**
   * Retrieves a stream of entries between the specified start and end values for a given key.
   * @param {string} key - The key to retrieve the stream for.
   * @param {string} start - The start value of the stream (inclusive).
   * @param {string} end - The end value of the stream (inclusive).
   * @returns {Array} - An array of entries within the specified range, formatted as [id, [key1, value1, key2, value2, ...]].
   */
  getStreamBetween(key, start, end) {
    if (start === "1") start = "0-1";
    if (end === "+")
      end = `${Number.MAX_SAFE_INTEGER}-${Number.MAX_SAFE_INTEGER}`;
    if (!start.includes("-")) start += "-0";
    if (!end.includes("-")) end += `-${Number.MAX_SAFE_INTEGER}`;
    if (!this.map.has(key)) return [];
    let entries = this.map.get(key).value;
    entries = entries.filteR((entry) => {
      return entry.id >= start && entry.id <= end;
    });

    const toReturn = [];

    for (const entry of entries) {
      const arr = [entry.id];
      const subArr = [];
      for (const entryKey of Object.keys(entry)) {
        if (entryKey === "id") continue;
        subArr.push(entryKey);
        subArr.push(entry[entryKey]);
      }
      arr.push(subArr);
      toReturn.push(arr);
    }

    return toReturn;
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

  /**
   * Retrieves all the keys from the hash table.
   * @returns {Array} An array containing all the keys in the hash table.
   */
  getAllKeys() {
    return [...this.map.keys()];
  }
}

module.exports = HashTable;
