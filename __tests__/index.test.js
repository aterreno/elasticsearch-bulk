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

beforeEach(async () => {
  await client.indices.delete({ index: 'tweets' });
});

test('index', async () => {
  const result = await client.helpers.bulk({
    datasource: dataset,
    onDocument() {
      return {
        index: { _index: 'tweets' },
      };
    },
  });

  expect(result.total).toBe(5);
});

test('update', async () => {
  await client.helpers.bulk({
    datasource: dataset,
    onDocument() {
      return {
        index: { _index: 'tweets' },
      };
    },
  });

  const updatedDatasource = dataset.map((d) => ({
    id: d.id,
    text: 'updatedText',
    user: 'updatedUser',
    date: new Date(),
  }));

  await client.helpers.bulk({
    datasource: updatedDatasource,
    onDocument(doc) {
      return [
        {
          update: {
            _index: 'tweets',
            _id: doc.id,
          },
        },
        {
          doc_as_upsert: true,
        },
      ];
    },
  });

  const result = await client.get({
    index: 'tweets',
    id: 1,
  });

  expect(result.body._source.text).toBe('updatedText');
  expect(result.body._source.user).toBe('updatedUser');
});

test('delete', async () => {
  await client.helpers.bulk({
    datasource: dataset,
    onDocument() {
      return {
        index: { _index: 'tweets' },
      };
    },
  });

  await client.helpers.bulk({
    datasource: dataset,
    onDocument(doc) {
      return {
        delete: { _index: 'tweets', _id: doc.id },
      };
    },
  });

  const { body: result } = await client.exists({
    index: 'tweets',
    id: 1,
  });

  expect(result).toBe(false);
});
