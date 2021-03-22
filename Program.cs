using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MessageQueue
{
    public class Event
    {
        public ObjectId _id;
        public BsonValue taskId;
        public string source;
        public string filter;
        public DateTime dt;
    }

    class Program
    {
        // Example message queue with mongodb. Small multi process chat
        // My advise is to use an event collection only to trigger the event
        // and use another collection with all the infos about the event (message info...)
        // because capped collection have some disavantages (can't delete)
        static void Main(string[] args)
        {
            string cnx = "mongodb://127.0.0.1";
            CancellationTokenSource exitToken = new CancellationTokenSource();

            MongoHelper.MongoQueue2 mq = new MongoHelper.MongoQueue2(cnx, "devapps", "mq", true);
            Task t1 = Task.Run(async () =>
            {
                while (true)
                {
                    // Uncomment to simulate slow treatment
                    // await System.Threading.Tasks.Task.Delay(1000, source.Token);
                    await mq.ReceiveAsync<Event>((Event e) =>
                    {
                        Console.WriteLine($"{e.dt.ToLocalTime()}\t{e.source}\t{e.filter}");
                        return e._id;
                    },
                    Builders<Event>.Filter.Eq(x => x.filter, "mq"),
                    exitToken.Token);
                }
            }, exitToken.Token);

            string line = "";
            while ((line=Console.ReadLine())!="")
            {
                Task.Run(async () => await mq.SendAsync<Event>(new Event() { source = line, dt = DateTime.Now, filter = "mq" }));
            }
            exitToken.Cancel();
            Task.WaitAny(t1);
        }
    }
}
