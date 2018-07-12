'use strict'

const mysql = require('mysql')
const util = require('util')

/**
 * MySQL helper
 * @class
 */
class MySQL {
  /**
   * Constructor
   * @param {string} url - database url
   * @param {object} defs - syncing fields definitions
   */
  constructor(url, defs) {
    this.url = url || 'mysql://localhost/test?user=root'
    this.defs = defs
    this.dbName = this.url.split(/\/|\?/)[3]
    this.con = null
    this.createConnection = null
    this.counter = 0
  }

  /**
   * Insert the record
   * @param {object} def - definition of fields
   * @param {object} item - the data of the record to insert
   * @param {boolean} replaceFlag - set true to replace the record
   * @param {function} callback - callback
   */
  insert(def, item, replaceFlag, callback) {
    if (typeof replaceFlag === 'function') {
      callback = replaceFlag
      replaceFlag = false
    }
    const command = replaceFlag ? 'REPLACE' : 'INSERT'
    const fs = def.fields.map(field => '`' + field.distName + '`')
    const vs = def.fields.map(field => field.convert(getFieldVal(field.name, item)))
    const sql = `${command} INTO \`${def.distName}\`` +
      ` (${fs.join(', ')}) VALUES (${vs.join(', ')});`
    const promise = this.query(sql)
      .catch(err => {
        //        util.log(`Failed to insert: ${err} ${sql}`)
        throw err
      })

    if (callback) promise.then(() => callback())

    return promise
  }

  /**
   * Update the record
   * @param {object} def - definition of fields
   * @param {string} id - the id of the record to update
   * @param {object} item - the columns to update
   * @param {object} unsetItems - the columns to drop
   * @param {function} callback - callback
   */
  update(def, id, item, unsetItems, callback) {
    const fields = def.fields.filter(field =>
      !!item && typeof getFieldVal(field.name, item) !== 'undefined' ||
      !!unsetItems && typeof getFieldVal(field.name, unsetItems) !== 'undefined')
    const sets = fields.map(field => {
      const val = field.convert(getFieldVal(field.name, item))
      return `\`${field.distName}\` = ${val}`
    })
    if (!sets.length) return Promise.resolve()

    const setsStr = sets.join(', ')
    const id2 = def.idType === 'number' ? id : `'${id}'`
    const sql = `UPDATE \`${def.distName}\` SET ${setsStr} WHERE ${def.idDistName} = ${id2};`
    const promise = this.query(sql)
      .catch(err => {
        //        util.log(`Failed to update: ${err}  ${sql}`)
        throw err
      })

    if (callback) promise.then(() => callback())
    return promise
  }

  /**
   * Remove the record
   * @param {object} def - definition of fields
   * @param {string} id - the id of the record to remove
   * @param {function} callback - callback
   */
  remove(def, id, callback) {
    const id2 = def.idType === 'number' ? id : `'${id}'`
    const sql = `DELETE FROM \`${def.distName}\` WHERE ${def.idDistName} = ${id2};`
    const promise = this.query(sql)
      .catch(err => {
        //        util.log(`Failed to remove: ${err} ${sql}`)
        throw err
      })

    if (callback) promise.then(() => callback())
    return promise
  }

  /**
   * Create tables
   * @returns {Promise} with no value
   */
  createTable() {
    // TODO: Create mongo_to_mysql table only if not exists
    const sql0 = 'DROP TABLE IF EXISTS mongo_to_mysql; ' +
      'CREATE TABLE mongo_to_mysql (service varchar(20), timestamp BIGINT);'
    const sql1 = `INSERT INTO mongo_to_mysql ` +
      `(service, timestamp) VALUES ("${this.dbName}", 0);`
    const sql2 = this.defs.map(def => {
      const fields = def.fields.map(field =>
        `\`${field.distName}\` ${field.type}${field.primary ? ' PRIMARY KEY' : ''}`)
      return `DROP TABLE IF EXISTS \`${def.distName}\`; ` +
        `CREATE TABLE \`${def.distName}\` (${fields.join(', ')});`
    }).join('')
    return this.query(sql0)
      .then(() => this.query(sql1))
      .then(() => this.query(sql2))
  }

  /**
   * Read timestamp
   * @returns {Promise} with timestamp
   */
  readTimestamp() {
    let q = 'SELECT timestamp FROM mongo_to_mysql' +
      ` WHERE service = '${this.dbName}'`
    return this.query(q)
      .then(results => results[0] && results[0].timestamp || 0)
      .catch(err => {
        //        util.log(`Failed to read timestamp: ${err}`)
        throw err
      })
  }

  /**
   * Update timestamp
   * @param {number} ts - a new timestamp
   */
  updateTimestamp(ts) {
    let q = `UPDATE mongo_to_mysql SET timestamp = ${ts}` +
      ` WHERE service = '${this.dbName}';`
    this.getConnection()
      .then(con => con.query(q))
      .catch(err => {
        //      util.log(`Failed to update timestamp: ${err}`)
        throw err
      })
  }

  connectionOk() {
    return new Promise((resolve) => {
      if (!this.con) return resolve(false)

      this.con
        .then((con) => resolve(!!(con._socket && con._socket.readable && con._socket.writable)))
        .catch(() => resolve(false))
    })
  }

  connect() {
    const connector = new Promise((resolve, reject) => {
      if (this.createConnection !== null) {
        const counter = ++this.counter
        util.log(`waiting for connection... ${counter}: ${this.createConnection}`)
        return this.createConnection.then((otherConnector) => otherConnector)
      }

      this.createConnection = new Promise((resolveCreateConnection) => {
        const params = 'multipleStatements=true'
        const url = this.url + (/\?/.test(this.url) ? '&' : '?') + params
        const con = mysql.createConnection(url)
        const counter = ++this.counter
        util.log(`Connect to MySQL... ${counter}`)
        con.on('close', () => util.log('SQL CONNECTION CLOSED.'))
        con.on('error', err => util.log(`SQL CONNECTION ERROR: ${err}`))

        con.connect( (err) => {
          if (err) {
            util.log(`SQL CONNECT ERROR: ${counter} ${err}`)
            reject(err)
          } else {
            util.log(`SQL CONNECT SUCCEEDED: ${counter}`)
            resolve(con)
          }
          resolveCreateConnection(connector)
          this.createConnection = null
        })
      })
    })
    return connector
  }
  /**
   * Connect to MySQL
   * @returns {connection} MySQL connection
   */
  async getConnection() {
    if (!await this.connectionOk()) this.con = this.connect()
    return this.con
  }

  /**
   * Query method with promise
   * @param {string} sql - SQL string
   * @returns {Promise} with results
   */
  query(sql) {
    return new Promise((resolve, reject) => {
      this.getConnection().then((con) => {
        con.query(sql, (err, results) => {
          if (err) reject(err)
          else resolve(results)
        })
      })
    })
  }
}

function getFieldVal(name, record) {
  return name.split('.').reduce((p, c) => p && p[c], record)
}

module.exports = MySQL