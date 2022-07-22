//  initial cut based on redis article https://redis.io/topics/indexes#representing-and-querying-graphs-using-an-hexastore
const permute = require('heaps-permute')
const keyLookup = { s: 'subject', p: 'predicate', o: 'object'}
const expectedOrders = ['spo','sop','pso','pos','osp','ops']
class Hexastore  {
  constructor({ index, redis, logger = console } ) {
    // index is where you want to store the key in redis
    this.index = index
    // expected to be an instance of an ioredis connector
    this.redis = redis
  }

  // add all permutations of a single triple to redis { subject, predicate, object }
  // e.g. `spo:<subject>:<predicate>:<object>` but also for all values of expectedOrders
  #generateHexastorePermutations (element) {
    const keys = Object.keys(element)
    const permutations = permute(keys)
    const toKeyInitial = ([key]) => key
    const typeKey = (perm) => perm.flatMap(toKeyInitial).join('')
    const synthesizeHexstoreData = (perm) => perm.map(key => element[key]).join(':')
    return permutations.map(perm => `${typeKey(perm)}:${synthesizeHexstoreData(perm)}`)
  }

  // create our string - `spo:<subject>:<predicate>:<object>` from
  // order: 'spo' and element where element is of
  // the form { subject, predicate, object } and can have null values
  #generateQueryRange(order, element) {
    const keys = order.split('').map(initial => keyLookup[initial])
    const rangeVerbs = [order]

    // literally `order` matters, for example if `spo` is passed and you
    // skip providing a subject, but provide a predicate or object
    // it throws an error
    // otherwise it builds our query string as an array to join with later on
    for (let i = 0; i < keys.length; i++) {
      const currentKey = keys[i]
      const priorKey = keys[i-1]
      if (element[currentKey]) {
        if (i !== 0 && !element[priorKey]) throw new Error(`You provided ${currentKey}, but there is no ${priorKey} for a ${order} query, invalid query`)
        rangeVerbs.push(element[currentKey])
      } 
    }
    return rangeVerbs.join(':')
  }

  // takes `spo:<subject>:<predicate>:<object>`
  // return { subject, predicate, object }
  #unmarshallHashString(string) {
    const [order, ...elements] = string.split(':')
    const keys = order.split('').map(initial => keyLookup[initial])
    const entries = keys.map((key, idx) => [key, elements[idx]])
    return Object.fromEntries(entries)
  }

  async add (subject, predicate, object) {
    const element = { subject, predicate, object }

    const pipeline = this.redis.pipeline()
    for (const hash of this.#generateHexastorePermutations(element)) {
      pipeline.zadd(this.index, 0, hash)
    }
    await pipeline.exec()
  }

  async remove (subject, predicate, object) {
    const element = { subject, predicate, object }
    // we can remove an array at a time! yay
    await this.redis.zrem(this.index, this.#generateHexastorePermutations(element))
  }

  // shorthand queries - probably not needed but nice to have
  async queryXXX() {
    return await this.query('spo', {})
  }
  async querySXX(subject) {
    return await this.query('spo', { subject })
  }
  async querySPX(subject, predicate) {
    return await this.query('spo', { subject, predicate })
  }
  async querySPO(subject, predicate, object) {
    return await this.query('spo', { subject, predicate, object })
  }
  async queryXPX(predicate) {
    return await this.query('pso', { predicate })
  }
  async queryXPO(predicate, object) {
    return await this.query('pos', { predicate, object })
  }
  async queryXXO(object) {
    return await this.query('osp', { object })
  }
  async querySXO(subject, object){
    return await this.query('sop', { object, subject })
  }

  // order is 'spo' or 'sop' or 'ops'
  // longform query
  async query(order, { subject, predicate, object }) {
    const element = { subject, predicate, object }

    if (!expectedOrders.includes(order)) throw new Error('Invalid order requested')

    // fyi marshalling here will throw an error if we pass an order with the proper elements passed
    const range = this.#generateQueryRange(order, element)

    const start = `[${range}`
    const end = `[${range}\xff`

    // ZRANGEBYLEX myindex "[sop:loren.sanz@nike.com:NEB-123:" "[sop:loren.sanz@nike.com:NEB-123:\xff"
    const data = await this.redis.zrangebylex(this.index, start, end)
    // split each entry back into its component parts - subject, object, predicate
    return data.map(this.#unmarshallHashString)
   }

}

module.exports = Hexastore
