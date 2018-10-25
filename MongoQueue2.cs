using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MongoHelper
{
    public class MongoQueue2
    {
        string m_szDatabase, m_szCollection;
        IMongoDatabase m_db;
        BsonValue m_lastId = BsonMinKey.Value;
        bool m_capped;

        // You can't delete record from a capped collection and you can change its size.
        public MongoQueue2(string connectionString, string database, string collection, bool capped, long queueSize = 1024 * 1024)
        {
            m_szDatabase = database;
            m_szCollection = collection;
            m_capped = capped;

            m_db = new MongoClient(connectionString).GetDatabase(database);
            if (capped)
            {
                var list = m_db.ListCollections(new ListCollectionsOptions() { Filter = Builders<BsonDocument>.Filter.Eq("name", collection) }).ToList();
                if (list.Count == 0)
                {
                    try
                    {
                        var options = new CreateCollectionOptions { Capped = true, MaxSize = queueSize };
                        m_db.CreateCollection(collection, options);
                    }
                    catch
                    {
                        // assume that any exceptions are because the collection already exists ...
                    }
                }
            }
            GetLastId();
        }

        public void SetLastId<T>(object o)
        {
            if (o != null)
            {
                // Retreive the id
                var idProvider = MongoDB.Bson.Serialization.BsonSerializer.LookupSerializer(typeof(T)) as MongoDB.Bson.Serialization.IBsonIdProvider;
                object id;
                Type nominal;
                MongoDB.Bson.Serialization.IIdGenerator generator;
                if (idProvider.GetDocumentId(o, out id, out nominal, out generator))
                    m_lastId = BsonValue.Create(id);
            }
        }

        public void GetLastId()
        {
            SetLastId<BsonDocument>(m_db.GetCollection<BsonDocument>(m_szCollection).Find(bson => true).SortByDescending(bson => bson["_id"]).FirstOrDefault());
        }

        public async Task SendAsync<T>(T doc)
        {
            await m_db.GetCollection<T>(m_szCollection).InsertOneAsync(doc);
        }

        // The callback should return the id for this record
        public async Task ReceiveAsync<T>(Func<T, BsonValue> callback, FilterDefinition<T> query = null, CancellationToken cancellationToken = default(CancellationToken))
        {
            while (true)
            {
                FilterDefinition<T> query2 = Builders<T>.Filter.Gt("_id", m_lastId); // We search event we could have miss since last call
                if (query != null)
                    query2 = Builders<T>.Filter.And(query, query2);

                var options = new FindOptions<T> { CursorType = CursorType.TailableAwait };
                using (var cursor = await m_db.GetCollection<T>(m_szCollection).FindAsync(query2, options, cancellationToken))
                {
                    await cursor.ForEachAsync(document =>
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        m_lastId = callback(document);
                    }, cancellationToken); // <= This cancellationToken is the important one to allow clean exit
                }
                // the tailable cursor died so loop through and restart it
            }
        }
    }
}
