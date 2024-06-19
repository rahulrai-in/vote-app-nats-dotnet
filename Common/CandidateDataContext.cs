using System.Text.Json.Serialization;

namespace Common;

[JsonSerializable(typeof(Dictionary<int, string>))]
public partial class CandidateDataContext : JsonSerializerContext { }