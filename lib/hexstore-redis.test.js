const test = require('ava')
const Redis = require('ioredis')
const Hexastore = require('./hexstore-redis')

test.beforeEach(t => {
  t.context.redis = new Redis()
})

// requires a live redis connection
// and actually creates and deletes values since we
// can't use ioredis-mock with zranges :sad-panda:
test('hexstore', async t => {
  const index = 'someoneToLove'
  const { redis } = t.context

  // clear out our index!
  await redis.del(index) 

  const hex = new Hexastore({ index, redis })

  const subject = 'techmandu@foo.com'
  const predicate = 'Registrant'
  const object = '12345'
  
  await hex.add('techmandu@foo.com', 'Registrant', '914')
  await hex.add('techmandu@foo.com', 'Registrant', '12345')
  await hex.add('techmandu@foo.com', 'Attendee', '914')
  await hex.add('someoneelse@foo.com', 'Registrant', '12345')

  const retVal = await hex.query('spo', { subject, predicate })

  t.deepEqual(retVal, [{ subject:'techmandu@foo.com', predicate:'Registrant', object }, {subject, predicate, object: '914'}] )
  const registrants = await hex.query('osp', { object: '914'})
  const altRegistrants = await hex.queryXXO('914')
  t.deepEqual(registrants, altRegistrants)
  t.deepEqual(registrants.map(r => r.subject), ['techmandu@foo.com', 'techmandu@foo.com'])
  await hex.remove('techmandu@foo.com', 'Registrant', '914')

  const allEntries = await hex.queryXXX()
  t.log(allEntries)
  t.is(allEntries.length, 3)
  await t.throwsAsync(hex.query('spo', { subject, object }), {
    message: 'You provided object, but there is no predicate for a spo query, invalid query'
  })
})
