using System.Text.Json;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.ObjectStore;

var semaphore = new SemaphoreSlim(1, 1);

await using var connection = new NatsConnection();

var jsContext = new NatsJSContext(connection);
var objectStoreContext = new NatsObjContext(jsContext);

// Create an object store named votes
var voteStore = await objectStoreContext.CreateObjectStoreAsync("votes");

// Create two receivers for vote.save to demonstrate load balancing between multiple instances
await using var voteSubscription1 = await connection.SubscribeCoreAsync<int>("vote.save", "group1");
var voteResponder1 = VoteResponder(voteSubscription1, voteStore, 1);

await using var voteSubscription2 = await connection.SubscribeCoreAsync<int>("vote.save", "group1");
var voteResponder2 = VoteResponder(voteSubscription2, voteStore, 2);

// Receiver for vote.get
await using var voteGetSubscription = await connection.SubscribeCoreAsync<string>("vote.get");
var voteGetReader = voteGetSubscription.Msgs;
var voteGetResponder = Task.Run(async () =>
{
    await foreach (var msg in voteGetReader.ReadAllAsync())
    {
        Console.WriteLine("Received candidate fetch request");
        var candidateVotes = new Dictionary<string, int>();

        // Fetch the votes from the vote store
        await foreach (var item in voteStore.ListAsync())
        {
            candidateVotes.Add($"Candidate-{Convert.ToInt32(item.Name)}",
                BitConverter.ToInt32(await voteStore.GetBytesAsync(item.Name)));
        }

        // Serialize the candidate votes to JSON
        var candidateVotesJson = JsonSerializer.Serialize(candidateVotes);
        await msg.ReplyAsync(candidateVotesJson);
        Console.WriteLine("Request processed");
    }
});

Console.WriteLine("Vote Processor Service is ready.");
await Task.WhenAll(voteResponder1, voteResponder2, voteGetResponder);
return;

Task VoteResponder(INatsSub<int> subscription, INatsObjStore objectStore, int consumerId)
{
    var voteReader = subscription.Msgs;
    var task = Task.Run(async () =>
    {
        await foreach (var msg in voteReader.ReadAllAsync())
        {
            var candidateId = msg.Data;
            Console.WriteLine($"Processor {consumerId}: Storing vote for candidate: {candidateId}");

            try
            {
                // Acquire lock to ensure thread safety when updating the vote count
                await semaphore.WaitAsync();

                // Increment the vote count for the candidate
                var dataBytes = await objectStore.GetBytesAsync(candidateId.ToString());
                var voteCount = BitConverter.ToInt32(dataBytes);
                voteCount++;
                await objectStore.PutAsync(candidateId.ToString(), BitConverter.GetBytes(voteCount));
            }
            catch (NatsObjNotFoundException e)
            {
                // If candidate record does not exist in the store, create it
                await objectStore.PutAsync(candidateId.ToString(), BitConverter.GetBytes(1));
            }
            finally
            {
                semaphore.Release();
            }

            Console.WriteLine($"Processor {consumerId}: Vote saved");
        }
    });

    return task;
}