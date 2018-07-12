#!/usr/bin/env node

'use strict'

const Tailer = require('../lib/tailer.js')
const fs = require('fs')

const DEFAULT_CONFIG_PATH = 'momyfile.json'
const refresh = process.argv.some(c => c === '--import')
const forever = process.argv.some(c => c === '--forever')
const finder = (p, c, i, a) => c === '--config' && a[i + 1] ? a[i + 1] : p
const file = process.argv.reduce(finder, DEFAULT_CONFIG_PATH)
const config = JSON.parse(fs.readFileSync(file))
const tailer = new Tailer(config)

if (refresh) return tailer.importAndStart(forever)

tailer.start(forever)
