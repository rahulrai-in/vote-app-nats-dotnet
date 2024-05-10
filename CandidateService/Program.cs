using System.Text.Json;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.KeyValueStore;

await using var connection = new NatsConnection();

var jsContext = new NatsJSContext(connection);
var kvContext = new NatsKVContext(jsContext);

// Create a KV bucket named candidates
var candidateStore = await kvContext.CreateStoreAsync(new NatsKVConfig("candidates"));

// Add seed data to the candidates bucket
await candidateStore.PutAsync("1", "Cat");
await candidateStore.PutAsync("2", "Dog");
await candidateStore.PutAsync("3", "Fish");

// Receiver for candidates.get
await using var candidateSubscription = await connection.SubscribeCoreAsync<string>("candidate.get");
var candidateReader = candidateSubscription.Msgs;
var candidateResponder = Task.Run(async () =>
{
    // Retrieve the candidates from the KV store
    var candidateList = new Dictionary<int, string>();
    await foreach (var key in candidateStore.GetKeysAsync())
    {
        candidateList.Add(Convert.ToInt32(key), (await candidateStore.GetEntryAsync<string>(key)).Value!);
    }

    // Serialize the candidate list to JSON
    var candidateListJson = JsonSerializer.Serialize(candidateList);

    await foreach (var msg in candidateReader.ReadAllAsync())
    {
        Console.WriteLine("Received candidate fetch request");
        await msg.ReplyAsync(candidateListJson);
        Console.WriteLine("Request processed");
    }
});

Console.WriteLine("Candidate Service is ready.");
await candidateResponder;