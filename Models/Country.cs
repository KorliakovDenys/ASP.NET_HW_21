using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace ASP.NET_HW_21.Models;

public class Country {
    [BsonId]
    public ObjectId Id { get; set; }

    public string? Name { get; set; }

    public int Population { get; set; }

    public double Area { get; set; }

    public ObjectId CapitalObjectId { get; set; }

    public ObjectId ContinentObjectId { get; set; }
}