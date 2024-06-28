using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.KeyValueStore;
using NATS.Client.Serializers.Json;

// Set the ad-hoc JSON serializer registry as the default for the connection.
var serializerRegistry = NatsJsonSerializerRegistry.Default;

var natsConnectionOptions = NatsOpts.Default with { SerializerRegistry = serializerRegistry };

await using var connection = new NatsConnection(natsConnectionOptions);

var jsContext = new NatsJSContext(connection);
var kvContext = new NatsKVContext(jsContext);

// Create a KV bucket named candidates
var candidateStore = await kvContext.CreateStoreAsync(new NatsKVConfig("candidates"));

// Add seed data to the candidates bucket
await candidateStore.PutAsync("1", "Cat");
await candidateStore.PutAsync("2", "Dog");
await candidateStore.PutAsync("3", "Fish");

Console.WriteLine("Candidate Service is ready.");

// Receiver for candidate.get
await foreach (var message in connection.SubscribeAsync<string>("candidate.get"))
{
    Console.WriteLine("Received candidate fetch request");

    // Retrieve the candidates from the KV store
    var candidateList = new Dictionary<int, string>();
    await foreach (var key in candidateStore.GetKeysAsync())
    {
        candidateList.Add(Convert.ToInt32(key), (await candidateStore.GetEntryAsync<string>(key)).Value!);
    }

    // Send the candidate list as a response
    await message.ReplyAsync(candidateList);
    Console.WriteLine("Request processed");
}