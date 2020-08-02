const BatchStream = require('batched-stream')
const { performance } = require('perf_hooks')
const { Transform, Writable } = require('stream')

module.exports = async (tenant, sql, columnSet, recordMapper, options = {}) => {

  try {
    // Set options as required
    options.batchSize = parseInt(options.batchSize) || 1000
    options.onConflictExpression = options.onConflictExpression || null

    const query = new tenant.lib.QueryStream(sql)

    const stream = tenant.db.client.query(query)

    return new Promise((resolve, reject) => {
      // We want to process this in batches
      const batch = new BatchStream({size : options.batchSize, objectMode: true, strictMode: false})

      // We use a write stream to insert the batch into the database
      let insertDatabase = new Writable({
        objectMode: true,
        write(records, encoding, callback) {
          (async () => {

            try {
              /*
                If we have a record mapper then do it here prior to inserting the records.
                This way is much quicker than doing it as a transform stream below by
                about 10 seconds for 100,000 records
              */
              if (recordMapper) {
                records = records.map(record => recordMapper(record))
              }

              let query = tenant.lib.pgp.helpers.insert(records, columnSet)

              if (options.onConflictExpression) {
                query += options.onConflictExpression
              }

              const i0 = performance.now()
              await tenant.db.none(query)
                .then(() => {
                  let i1 = performance.now()
                  console.log('Inserted ' + records.length + ' records in ' + ((i1 - i0) / 1000) + ' (seconds).')
                })

            } catch(e) {
              return callback(e)
            }

            callback()
          })()
        }
      })

      // Process the stream
      const t0 = performance.now()
      stream
        // Break it down into batches
        .pipe(batch)
        // Insert those batches into the database
        .pipe(insertDatabase)
        // Once we get here we are done :)
        .on('finish', () => {
          const t1 = performance.now()
          console.log('Finished insert in ' + ((t1 - t0) / 1000) + ' (seconds).')
          resolve()
        })
        .on('error', (error) => {
          reject(error)
        })

    })
  } catch (err) {
    throw err
  }
}