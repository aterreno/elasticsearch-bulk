const { Client } = require('@elastic/elasticsearch');
const client = new Client({ node: 'http://localhost:9200' });

const dataset = [
  {
    id: 1,
    text: "If I fall, don't bring me back.",
    user: 'jon',
    date: new Date(),
  },
  {
    id: 2,
    text: 'Witer is coming',
    user: 'ned',
    date: new Date(),
  },
  {
    id: 3,
    text: 'A Lannister always pays his debts.',
    user: 'tyrion',
    date: new Date(),
  },
  {
    id: 4,
    text: 'I am the blood of the dragon.',
    user: 'daenerys',
    date: new Date(),
  },
  {
    id: 5,
    text: "A girl is Arya Stark of Winterfell. And I'm going home.",
    user: 'arya',
    date: new Date(),
  },
];

afterEach(async () => {
  await client.indices.delete({ index: 'tweets' });
});

test('index', async () => {
  const body = dataset.flatMap((doc) => [{ update: { _index: 'tweets', _id: doc.id } }, { doc, doc_as_upsert: true }]);
  await client.bulk({ refresh: true, body });

  const {
    body: {
      hits: { hits },
    },
  } = await client.search({
    index: 'tweets',
    body: {
      query: {
        match_all: {},
      },
    },
  });

  expect(hits.length).toBe(5);
  const res = hits.map((d) => {
    const { id, text, user, date } = d._source;
    return { id, text, user, date: new Date(date) };
  });
  expect(res).toStrictEqual(dataset);
});

test('update', async () => {
  const updateBody = dataset.flatMap((doc) => [
    { update: { _index: 'tweets', _id: doc.id } },
    { doc, doc_as_upsert: true },
  ]);
  await client.bulk({ refresh: true, body: updateBody });

  const updatedDatasource = dataset.map((d) => ({
    id: d.id,
    text: 'updatedText',
    user: 'updatedUser',
    date: new Date(),
  }));

  const updateBody2 = updatedDatasource.flatMap((doc) => [
    { update: { _index: 'tweets', _id: doc.id } },
    { doc, doc_as_upsert: true },
  ]);

  await client.bulk({ refresh: true, body: updateBody2 });

  const {
    body: {
      hits: { hits },
    },
  } = await client.search({
    index: 'tweets',
    body: {
      query: {
        match_all: {},
      },
    },
  });

  expect(hits.length).toBe(5);
  const res = hits.map((d) => {
    const { id, text, user, date } = d._source;
    return { id, text, user, date: new Date(date) };
  });
  expect(res).toStrictEqual(updatedDatasource);
});

test('delete', async () => {
  const body = dataset.flatMap((doc) => [{ update: { _index: 'tweets', _id: doc.id } }, { doc, doc_as_upsert: true }]);
  await client.bulk({ refresh: true, body });

  const bodyDelete = dataset.flatMap((doc) => [{ delete: { _index: 'tweets', _id: doc.id } }]);
  await client.bulk({ refresh: true, body: bodyDelete });

  const {
    body: {
      hits: { hits },
    },
  } = await client.search({
    index: 'tweets',
    body: {
      query: {
        match_all: {},
      },
    },
  });

  expect(hits.length).toBe(0);
  const res = hits.map((d) => {
    const { id, text, user, date } = d._source;
    return { id, text, user, date: new Date(date) };
  });
  expect(res).toStrictEqual([]);
});
