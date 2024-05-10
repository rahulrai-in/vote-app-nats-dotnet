using System.Text.Json;
using NATS.Client.Core;
using NATS.Client.Services;

await using var connection = new NatsConnection();

// Register as service with NATS
var svc = new NatsSvcContext(connection);
var service = await svc.AddServiceAsync(new("vote", "1.0.0")
{
    Description = "Casts vote for candidate",
});

// Receiver for vote.send
await service.AddEndpointAsync<object>(HandleMessage, "send", "vote.send.*");

Console.WriteLine("Voting Service is ready.");
Console.ReadKey();
return;

async ValueTask HandleMessage(NatsSvcMsg<object> msg)
{
    var candidateId = Convert.ToInt32(msg.Subject.Split('.')[^1]);
    Console.WriteLine($"Received vote for candidate: {candidateId}");

    // Retrieve the candidate ids from the Candidate Service
    var candidateResponse = await connection.RequestAsync<object, string>("candidate.get", null);
    var receivedData = JsonSerializer.Deserialize<Dictionary<int, string>>(candidateResponse.Data ?? string.Empty);
    var candidates = receivedData?.Keys.ToList() ?? []
    ;

    // Validate the candidate ID
    if (!candidates.Contains(candidateId))
    {
        await msg.ReplyAsync("Invalid candidate ID");
    }
    else
    {
        //Publish the vote to the Vote Processor Service
        await connection.PublishAsync("vote.save", candidateId);
        await msg.ReplyAsync("Vote has been cast");
    }

    Console.WriteLine("Vote processed");
}