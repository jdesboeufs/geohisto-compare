const csvParse = require('csv-parse')
const fs = require('fs')
const t = require('through2')
const Promise = require('bluebird')
const iconv = require('iconv-lite')
const { difference } = require('lodash')

function loadGeohistoTowns() {
  const towns = {}

  return new Promise((resolve, reject) => {
    fs.createReadStream(__dirname + '/data/towns_2016-01-01.csv')
      .pipe(csvParse({ columns: true }))
      .pipe(t.obj((entry, enc, cb) => {
        if (entry.name.includes('Arrondissement')) return cb()
        towns[entry.insee_code] = entry.name
        cb()
      }))
      .on('error', reject)
      .on('finish', () => resolve(towns))
  })
}

function loadCOGTowns() {
  const towns = {}

  return new Promise((resolve, reject) => {
    fs.createReadStream(__dirname + '/data/insee_cog_comsimp2016.tsv')
      .pipe(iconv.decodeStream('win1252'))
      .pipe(csvParse({ delimiter: '\t', columns: true }))
      .pipe(t.obj((entry, enc, cb) => {
        const code = entry.DEP + entry.COM
        towns[code] = entry.NCCENR
        cb()
      }))
      .on('error', reject)
      .on('finish', () => resolve(towns))
  })
}

Promise.join(
  loadGeohistoTowns(),
  loadCOGTowns(),

  function (geohistoTowns, cogTowns) {
    const geohistoCodes = Object.keys(geohistoTowns)
    const cogCodes = Object.keys(cogTowns)

    console.log('%d codes in Geohisto', geohistoCodes.length)
    console.log('%d codes in COG', cogCodes.length)

    const codesInGeohistoButNotInCog = difference(geohistoCodes, cogCodes)
    const codesInCogButNotInGeohisto = difference(cogCodes, geohistoCodes)

    codesInGeohistoButNotInCog.forEach(code => {
      console.log('Town %s found in geohisto but not in COG: %s', code, geohistoTowns[code])
    })

    codesInCogButNotInGeohisto.forEach(code => {
      console.log('Town %s found in COG but not in Geohisto: %s', code, cogTowns[code])
    })

    console.log('Finished!')
  }
)
