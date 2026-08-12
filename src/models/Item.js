const { ITEM_COLUMNS } = require('./itemColumns');

/**
 * Item domain object - mirrors com.antontech.itemkafka_poc.model.Item.
 * Rather than 80+ hand-written getters/setters/prepared-statement bindings
 * like the Java version, every conversion here is driven by the single
 * ITEM_COLUMNS metadata array (src/models/itemColumns.js).
 */
class Item {
  constructor(data = {}) {
    for (const col of ITEM_COLUMNS) {
      this[col.js] = Object.prototype.hasOwnProperty.call(data, col.js) ? data[col.js] : null;
    }
  }

  /** Builds an Item from a raw DB row (mysql2/mssql both return plain objects keyed by column name). */
  static fromDbRow(row) {
    const data = {};
    for (const col of ITEM_COLUMNS) {
      data[col.js] = row[col.db] !== undefined ? row[col.db] : null;
    }
    return new Item(data);
  }

  /** Builds an Item from a parsed JSON payload (e.g. consumed from Kafka). */
  static fromJson(json) {
    return new Item(json);
  }

  /** Fills in sensible defaults for any null/undefined fields (mirrors ItemFromJsonFunction.applyDefaults). */
  applyDefaults() {
    for (const col of ITEM_COLUMNS) {
      if (this[col.js] === null || this[col.js] === undefined) {
        this[col.js] = typeof col.default === 'function' ? col.default() : col.default;
      }
    }
    if (!this.itemId) {
      this.itemId = require('uuid').v4();
    }
    return this;
  }

  /** Returns an array of values in ITEM_COLUMNS order, suitable for a parameterised INSERT ... ON DUPLICATE KEY UPDATE. */
  toDbValues() {
    return ITEM_COLUMNS.map((col) => {
      const value = this[col.js];
      if (value === undefined) return null;
      if (col.type === 'datetime' && value instanceof Date) {
        return value.toISOString().slice(0, 19).replace('T', ' ');
      }
      return value;
    });
  }
}

module.exports = Item;

