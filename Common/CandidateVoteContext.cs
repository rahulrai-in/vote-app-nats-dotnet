using System.Text.Json.Serialization;

namespace Common;

[JsonSerializable(typeof(Dictionary<string, int>))]
public partial class CandidateVoteContext : JsonSerializerContext { }