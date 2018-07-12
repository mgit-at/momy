'use strict'

const util = require('util')
const mongodb = require('mongodb').MongoClient
const Timestamp = require('mongodb').Timestamp
const MySQL = require('./mysql.js')
const createDefs = require('./defs.js')

/**
 * Tailer
 * @class
 */
class Tailer {
  /**
   * Constructor
   * @param {object} config - configulation options
   * @param {boolean} cliMode - set false for testing
   */
  constructor(config, cliMode) {
    const opts = {
      prefix: config.prefix || '',
      fieldCase: config.fieldCase || '',
      exclusions: config.exclusions || '',
      inclusions: config.inclusions || ''
    }
    this.cliMode = cliMode === undefined ? true : !!cliMode
    this.url = config.src
    this.url2 = this.url.replace(/\/\w+(\?|$)/, '/local$1')
    this.dbName = this.url.split(/\/|\?/)[3]
    this.defs = createDefs(config.collections, this.dbName, opts)
    this.lastTs = 0
    this.mysql = new MySQL(config.dist, this.defs)
    this.processCounter = 0
  }

  /**
   * Start tailing
   * @param {boolean} forever - set false for testing
   */
  start(forever) {
    forever = forever === undefined ? true : !!forever
    this.mysql.readTimestamp()
      .then(ts => this.updateTimestamp(ts, true))
      .then(() => forever ? this.tailForever() : this.tail())
      .catch(err => this.stop(err))
  }

  /**
   * Import all and start tailing
   * @param {boolean} forever - set false for testing
   */
  importAndStart(forever) {
    forever = forever === undefined ? true : !!forever
    this.mysql.createTable()
      .then(() => this.importAll())
      .then(() => this.updateTimestamp())
      .then(() => forever ? this.tailForever() : this.tail())
      .catch(err => this.stop(err))
  }

  stop(err) {
    if (this.cliMode) {
      if (err) util.log(err)
      util.log('Bye')
      process.exit()
    } else if (this.db) {
      this.db.close()
      this.db = null
    }
  }

  /**
   * Import all
   * @returns {Promise} with no value
   */
  importAll() {
    util.log('Begin to import...')
    let promise = Promise.resolve()
    this.defs.forEach(def => {
      promise = promise.then(() => this.importCollection(def))
    })
    promise.then(() => {
      util.log('Done.')
    })
    return promise
  }

  /**
   * Import collection
   * @param {object} def - definition of fields
   * @returns {Promise} with no value
   */
  importCollection(def) {
    util.log(`Import records in ${def.ns}`)
    return new Promise(resolve =>
      mongodb.connect(this.url)
      .then(db => {
        const stream = db.collection(def.name).find().stream()
        stream
          .on('data', item => {
            stream.pause()
            this.mysql.insert(def, item, () => stream.resume())
          })
          .on('end', () => {
            resolve()
          })
      }))
  }

  /**
   * Check the latest log in Mongo, then catch the timestamp up in MySQL
   * @param {number} ts - unless null then skip updating in MySQL
   * @param {boolean} skipUpdateMySQL - skip update in MySQL
   * @returns {Promise} with no value
   */
  updateTimestamp(ts, skipUpdateMySQL) {
    if (ts) {
      this.lastTs = ts
      if (!skipUpdateMySQL) return this.mysql.updateTimestamp(ts)
      return Promise.resolve()
    }
    return new Promise(resolve =>
      mongodb.connect(this.url2)
      .then(db =>
        db.collection('oplog.rs').find().sort({
          $natural: -1
        }).limit(1)
        .nextObject()
        .then(item => {
          ts = item.ts.toNumber()
          this.lastTs = ts
          if (!skipUpdateMySQL) return this.mysql.updateTimestamp(ts).then(resolve)
          resolve()
        })))
  }

  /**
   * Tail forever
   * @returns {Promise} with no value
   */
  tailForever() {
    return new Promise((resolve, reject) => {
      let counter = 0
      let promise = Promise.resolve()
      const chainPromise = () => {
        promise = promise
          .then(() => {
            const message = counter++
              ?
              'Reconnect to MongoDB...' :
              'Connect to MongoDB...'
            util.log(message)
            return this.tail()
          })
          .catch(err => reject(err))
          .then(chainPromise)
      }
      chainPromise()
    })
  }

  /**
   * Tail the log of Mongo by tailable cursors
   * @returns {Promise} with no value
   */
  tail() {
    const ts = this.lastTs
    const nss = this.defs.map(def => def.ns)
    const filters = {
      ns: {
        $in: nss
      },
      ts: {
        $gt: Timestamp.fromNumber(ts)
      }
    }
    const curOpts = {
      tailable: true,
      awaitdata: true,
      numberOfRetries: 1024,
      tailableRetryInterval: 1000
    }

    util.log(`Begin to watch... (from ${ts})`)
    return new Promise((resolve, reject) =>
      mongodb.connect(this.url2).then(db => {
        const close = (err) => {
          this.db = null
          db.close()
          if (err) return reject(err)
          resolve()
        }
        this.db = db
        const stream = db.collection('oplog.rs').find(filters, curOpts).stream()
        stream
          .on('data', log => {
            if (log.op === 'n' || log.ts.toNumber() === ts) return
            this.process(log).catch((err) => {
              util.log(`Processing error: ${err}`)
              close(null)
            })
          })
          .on('close', () => {
            util.log('Stream closed....')
            close(null)
          })
          .on('error', err => {
            util.log(`Mongodb error: ${err}`)
            close(err)
          })
      }))
  }



  /**
   * Process the log and sync to MySQL
   * @param {object} log - the log retrieved from oplog.rs
   * @returns {undefined}
   */
  async process(log) {
    const def = this.defs.filter(def => log.ns === def.ns)[0]
    if (!def) return

    switch (log.op) {
      case 'i':
        //        util.log(`Insert a new record into ${def.ns}`)
        await this.mysql.insert(def, log.o)
        break
      case 'u':
        if (log.o.$set || log.o.$unset) {
          //          util.log(`Update a record in ${def.ns} (${def.idName}=${log.o2[def.idName]})`)
          await this.mysql.update(def, log.o2[def.idName], log.o.$set, log.o.$unset)
        } else {
          const replaceFlag = true
          //          util.log(`Replace a record in ${def.ns} (${def.idName}=${log.o[def.idName]})`)
          await this.mysql.insert(def, log.o, replaceFlag)
        }
        break
      case 'd':
        //        util.log(`Delete a record in ${def.ns} (${def.idName}=${log.o[def.idName]})`)
        await this.mysql.remove(def, log.o[def.idName])
        break
      default:
        break
    }
    const txnNum = log.ts.toNumber()
    await this.updateTimestamp(txnNum)
    if((++this.processCounter) % 1000 === 0) {
      util.log(`processing item#${this.processCounter}`)
    }
  }
}

module.exports = Tailer