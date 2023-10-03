using ASP.NET_HW_21.Models;
using MongoDB.Bson;
using MongoDB.Driver;

const string connectionString = "mongodb://localhost:27017";
const string dbName = "Geography";
const string continentsCollectionName = "Continents";
const string countriesCollectionName = "Countries";
const string citiesCollectionName = "Cities";
const string africa = "Africa";
const string asia = "Asia";
const double minArea = 500000;
const double maxArea = 1000000;
const int minPopulation = 10000000;

var client = new MongoClient(connectionString);

var database = client.GetDatabase(dbName);

var continentsCollection = database.GetCollection<Continent>(continentsCollectionName);
var countriesCollection = database.GetCollection<Country>(countriesCollectionName);
var citiesCollection = database.GetCollection<City>(citiesCollectionName);

// Initialize();

var countries = await countriesCollection.Find(_ => true).ToListAsync();

var countryNames = await countriesCollection.Find(_ => true).Project(c => c.Name).ToListAsync();

var capitalIds = await countriesCollection.Find(_ => true).Project(c => c.CapitalObjectId).ToListAsync();

var capitalNames = await citiesCollection
    .Find(new BsonDocument("_id", new BsonDocument("$in", new BsonArray(capitalIds))))
    .Project(c => c.Name).ToListAsync();

var europeId = continentsCollection.Find(c => c.Name == "Europe").Project(c => c.Id).FirstOrDefault();

var europeanCountryNames = await countriesCollection.Find(c => c.ContinentObjectId == europeId)
    .Project(c => c.Name).ToListAsync();

var countriesWithMinArea = await countriesCollection.Find(c => c.Area >= minArea).Project(c => c.Name).ToListAsync();

var filter = Builders<Country>.Filter.Regex(c => c.Name, new BsonRegularExpression("a|u", "i"));
var countryWithAuNames = await countriesCollection.Find(filter).Project(c => c.Name).ToListAsync();

filter = Builders<Country>.Filter.Regex(c => c.Name, new BsonRegularExpression("^a", "i"));
var countryNamesStartingWithA = await countriesCollection.Find(filter).Project(c => c.Name).ToListAsync();

filter = Builders<Country>.Filter.And(
    Builders<Country>.Filter.Gte(c => c.Area, minArea),
    Builders<Country>.Filter.Lte(c => c.Area, maxArea)
);
var countryInAreaRangeNames = await countriesCollection.Find(filter).Project(c => c.Name).ToListAsync();

filter = Builders<Country>.Filter.Gt(c => c.Population, minPopulation);
var countriesWithPopulationGreaterThanNames = await countriesCollection.Find(filter).Project(c => c.Name).ToListAsync();

var top5CountryNamesByArea = await countriesCollection.Find(_ => true).SortByDescending(c => c.Area).Limit(5)
    .Project(c => c.Name).ToListAsync();

var top5CountryNamesByPopulation = await countriesCollection.Find(_ => true).SortByDescending(c => c.Population)
    .Limit(5)
    .Project(c => c.Name).ToListAsync();

var countryNameWithLargestArea = await countriesCollection.Find(_ => true).SortByDescending(c => c.Area)
    .Limit(1).Project(c => c.Name).FirstOrDefaultAsync()!;

var countryNameWithLargestPopulation = await countriesCollection.Find(_ => true).SortByDescending(c => c.Population)
    .Limit(1).Project(c => c.Name).FirstOrDefaultAsync()!;

var africaId = continentsCollection.Find(c => c.Name == africa).Limit(1).Project(c => c.Id).FirstOrDefault();
var smallestCountryInAfrica = countriesCollection.Find(c => c.ContinentObjectId == africaId).SortBy(c => c.Area)
    .Limit(1).Project(c => c.Name).FirstOrDefault();

var asiaId = continentsCollection.Find(c => c.Name == asia).Limit(1).Project(c => c.Id).FirstOrDefault();
var avgAreaInAsia = countriesCollection.Find(c => c.ContinentObjectId == asiaId).ToList().Average(c => c.Area);

var totalNumberOfCountries = countriesCollection.Find(_ => true).CountDocuments();

var continentId = countriesCollection.Aggregate().Group(new BsonDocument
        { { "_id", "$ContinentObjectId" }, { "count", new BsonDocument("$sum", 1) } }).SortByDescending(c => c["count"])
    .Limit(1).Project(doc => doc["_id"]).FirstOrDefault();
var mostCountriesInContinent =
    continentsCollection.Find(c => c.Id == continentId).Limit(1).Project(c => c.Name).FirstOrDefault();

var pipeline = new BsonDocument[] {
    new("$lookup", new BsonDocument {
        { "from", countriesCollectionName },
        { "localField", "_id" },
        { "foreignField", "ContinentObjectId" },
        { "as", "Countries" }
    }),
    new("$unwind", "$Countries"),
    new("$group", new BsonDocument {
        { "_id", "$Name" },
        { "count", new BsonDocument("$sum", 1) }
    })
};

var countriesPerContinent = continentsCollection.Aggregate<BsonDocument>(pipeline).ToList();

PrintIEnumerable(countryNames);
PrintIEnumerable(capitalNames);
PrintIEnumerable(europeanCountryNames);
PrintIEnumerable(countriesWithMinArea);
PrintIEnumerable(countryWithAuNames);
PrintIEnumerable(countryNamesStartingWithA);
PrintIEnumerable(countryInAreaRangeNames);
PrintIEnumerable(countriesWithPopulationGreaterThanNames);
PrintIEnumerable(top5CountryNamesByArea);
PrintIEnumerable(top5CountryNamesByPopulation);
Print(countryNameWithLargestArea);
Print(countryNameWithLargestPopulation);
Print(smallestCountryInAfrica);
Print($"Average area in Asia = {avgAreaInAsia}km^2");
Print($"Total number of countries: {totalNumberOfCountries}");
Print(mostCountriesInContinent);
PrintIEnumerable(countriesPerContinent);

Console.WriteLine();

return;

void Print(string? str) {
    if (str is null) return;
    Console.WriteLine(new string('-', 20));
    Console.WriteLine(str);
}

void PrintIEnumerable<T>(IEnumerable<T> enumerable) {
    Console.WriteLine(new string('-', 20));
    foreach (var element in enumerable) {
        if (element is null) continue;
        Console.WriteLine(element);
    }
}

void Initialize() {
    var continentDocuments = new List<Continent> {
        new() {
            Id = new ObjectId("60c78a3b4b4d8e001a53d471"),
            Name = "Africa"
        },
        new() {
            Id = new ObjectId("60c78a3b4b4d8e001a53d472"),
            Name = "Europe"
        },
        new() {
            Id = new ObjectId("60c78a3b4b4d8e001a53d473"),
            Name = "Asia"
        },
        new() {
            Id = new ObjectId("60c78a3b4b4d8e001a53d474"),
            Name = "North America"
        },
        new() {
            Id = new ObjectId("60c78a3b4b4d8e001a53d475"),
            Name = "South America"
        },
        new() {
            Id = new ObjectId("60c78a3b4b4d8e001a53d476"),
            Name = "Australia"
        },
        new() {
            Id = new ObjectId("60c78a3b4b4d8e001a53d477"),
            Name = "Antarctica"
        }
    };
    continentsCollection?.InsertMany(continentDocuments);

    var countriesDocuments = new List<Country> {
        new() {
            Id = ObjectId.Parse("60c78a3b4b4d8e001a53d47a"),
            Name = "Egypt",
            Population = 104258327,
            Area = 1002450,
            CapitalObjectId = ObjectId.Parse("60c78a3b4b4d8e001a53d47b"),
            ContinentObjectId = ObjectId.Parse("60c78a3b4b4d8e001a53d471")
        },
        new() {
            Id = ObjectId.Parse("60c78a3b4b4d8e001a53d47c"),
            Name = "Nigeria",
            Population = 206139589,
            Area = 923768,
            CapitalObjectId = ObjectId.Parse("60c78a3b4b4d8e001a53d47d"),
            ContinentObjectId = ObjectId.Parse("60c78a3b4b4d8e001a53d471")
        },
        new() {
            Id = ObjectId.Parse("60c78a3b4b4d8e001a53d47e"),
            Name = "South Africa",
            Population = 59308690,
            Area = 1221037,
            CapitalObjectId = ObjectId.Parse("60c78a3b4b4d8e001a53d47f"),
            ContinentObjectId = ObjectId.Parse("60c78a3b4b4d8e001a53d471")
        },
        new() {
            Id = ObjectId.Parse("60c78a3b4b4d8e001a53d480"),
            Name = "France",
            Population = 67564275,
            Area = 551695,
            CapitalObjectId = ObjectId.Parse("60c78a3b4b4d8e001a53d481"),
            ContinentObjectId = ObjectId.Parse("60c78a3b4b4d8e001a53d472")
        },
        new() {
            Id = ObjectId.Parse("60c78a3b4b4d8e001a53d482"),
            Name = "Germany",
            Population = 83783942,
            Area = 357022,
            CapitalObjectId = ObjectId.Parse("60c78a3b4b4d8e001a53daaa"),
            ContinentObjectId = ObjectId.Parse("60c78a3b4b4d8e001a53d472")
        },
        new() {
            Id = ObjectId.Parse("60c78a3b4b4d8e001a53d484"),
            Name = "Spain",
            Population = 46754778,
            Area = 505990,
            CapitalObjectId = ObjectId.Parse("60c78a3b4b4d8e001a53d485"),
            ContinentObjectId = ObjectId.Parse("60c78a3b4b4d8e001a53d472")
        },
        new() {
            Id = ObjectId.Parse("60c78a3b4b4d8e001a53d486"),
            Name = "China",
            Population = 1444216107,
            Area = 9596960,
            CapitalObjectId = ObjectId.Parse("60c78a3b4b4d8e001a53d487"),
            ContinentObjectId = ObjectId.Parse("60c78a3b4b4d8e001a53d473")
        },
        new() {
            Id = ObjectId.Parse("60c78a3b4b4d8e001a53d488"),
            Name = "India",
            Population = 1393409038,
            Area = 3287263,
            CapitalObjectId = ObjectId.Parse("60c78a3b4b4d8e001a53d489"),
            ContinentObjectId = ObjectId.Parse("60c78a3b4b4d8e001a53d473")
        },
        new() {
            Id = ObjectId.Parse("60c78a3b4b4d8e001a53d48a"),
            Name = "Japan",
            Population = 126476461,
            Area = 377975,
            CapitalObjectId = ObjectId.Parse("60c78a3b4b4d8e001a53d48b"),
            ContinentObjectId = ObjectId.Parse("60c78a3b4b4d8e001a53d473")
        },
        new() {
            Id = ObjectId.Parse("60c78a3b4b4d8e001a53d46e"),
            Name = "United States of America",
            Population = 331002651,
            Area = 9833517.85,
            CapitalObjectId = ObjectId.Parse("72c78a3b4b4d8e001a53d485"),
            ContinentObjectId = ObjectId.Parse("60c78a3b4b4d8e001a53d474")
        },
        new() {
            Id = ObjectId.Parse("60c78a3b4b4d8e001a53d48c"),
            Name = "Canada",
            Population = 37742154,
            Area = 9976140,
            CapitalObjectId = ObjectId.Parse("71c78a3b4b4d8e001a53d485"),
            ContinentObjectId = ObjectId.Parse("60c78a3b4b4d8e001a53d474")
        },
        new() {
            Id = ObjectId.Parse("60c78a3b4b4d8e001a53d48e"),
            Name = "Brazil",
            Population = 212559417,
            Area = 8515767,
            CapitalObjectId = ObjectId.Parse("60c78a3b4b4d8e001a53d471"),
            ContinentObjectId = ObjectId.Parse("60c78a3b4b4d8e001a53d475")
        },
        new() {
            Id = ObjectId.Parse("60c78a3b4b4d8e001a53d490"),
            Name = "Australia",
            Population = 25499884,
            Area = 7692024,
            CapitalObjectId = ObjectId.Parse("60c78a3b4b4d8e001a53d472"),
            ContinentObjectId = ObjectId.Parse("60c78a3b4b4d8e001a53d476")
        }
    };
    countriesCollection?.InsertMany(countriesDocuments);

    var citiesDocuments = new List<City> {
        new() {
            Id = ObjectId.Parse("60c78a3b4b4d8e001a53d47b"),
            Name = "Cairo",
            CountryId = ObjectId.Parse("60c78a3b4b4d8e001a53d47a")
        },
        new() {
            Id = ObjectId.Parse("60c78a3b4b4d8e001a53d47d"),
            Name = "Alexandria",
            CountryId = ObjectId.Parse("60c78a3b4b4d8e001a53d47a")
        },
        new() {
            Id = ObjectId.Parse("60c78a3b4b4d8e001a53d47f"),
            Name = "Lagos",
            CountryId = ObjectId.Parse("60c78a3b4b4d8e001a53d47c")
        },
        new() {
            Id = ObjectId.Parse("60c78a3b4b4d8e001a53d480"),
            Name = "Abuja",
            CountryId = ObjectId.Parse("60c78a3b4b4d8e001a53d47c")
        },
        new() {
            Id = ObjectId.Parse("60c78a3b4b4d8e001a53d482"),
            Name = "Cape Town",
            CountryId = ObjectId.Parse("60c78a3b4b4d8e001a53d47e")
        },
        new() {
            Id = ObjectId.Parse("60c78a3b4b4d8e001a53d483"),
            Name = "Johannesburg",
            CountryId = ObjectId.Parse("60c78a3b4b4d8e001a53d47e")
        },
        new() {
            Id = ObjectId.Parse("60c78a3b4b4d8e001a53d481"),
            Name = "Paris",
            CountryId = ObjectId.Parse("60c78a3b4b4d8e001a53d480")
        },
        new() {
            Id = ObjectId.Parse("60c78a3b4b4d8e001a53d484"),
            Name = "Lyon",
            CountryId = ObjectId.Parse("60c78a3b4b4d8e001a53d480")
        },
        new() {
            Id = ObjectId.Parse("60c78a3b4b4d8e001a53daaa"),
            Name = "Berlin",
            CountryId = ObjectId.Parse("60c78a3b4b4d8e001a53d482")
        },
        new() {
            Id = ObjectId.Parse("60c78a3b4b4d8e001a53d486"),
            Name = "Munich",
            CountryId = ObjectId.Parse("60c78a3b4b4d8e001a53d482")
        },
        new() {
            Id = ObjectId.Parse("60c78a3b4b4d8e001a53d46f"),
            Name = "New York",
            CountryId = ObjectId.Parse("60c78a3b4b4d8e001a53d46e")
        },
        new() {
            Id = ObjectId.Parse("60c78a3b4b4d8e001a53d470"),
            Name = "Los Angeles",
            CountryId = ObjectId.Parse("60c78a3b4b4d8e001a53d46e")
        },
        new() {
            Id = ObjectId.Parse("60c78a3b4b4d8e001a53d471"),
            Name = "Rio de Janeiro",
            CountryId = ObjectId.Parse("60c78a3b4b4d8e001a53d48e")
        },
        new() {
            Id = ObjectId.Parse("60c78a3b4b4d8e001a53d472"),
            Name = "San Paulo",
            CountryId = ObjectId.Parse("60c78a3b4b4d8e001a53d48e")
        },
        new() {
            Id = ObjectId.Parse("60c78a3b4b4d8e001a53d473"),
            Name = "Sydney",
            CountryId = ObjectId.Parse("60c78a3b4b4d8e001a53d490")
        },
        new() {
            Id = ObjectId.Parse("60c78a3b4b4d8e001a53d474"),
            Name = "Melbourne",
            CountryId = ObjectId.Parse("60c78a3b4b4d8e001a53d490")
        },
        new() {
            Id = ObjectId.Parse("60c78a3b4b4d8e001a53d485"),
            Name = "Madrid",
            CountryId = ObjectId.Parse("60c78a3b4b4d8e001a53d484")
        },
        new() {
            Id = ObjectId.Parse("70c78a3b4b4d8e001a53d485"),
            Name = "New Delhi",
            CountryId = ObjectId.Parse("60c78a3b4b4d8e001a53d488")
        },
        new() {
            Id = ObjectId.Parse("72c78a3b4b4d8e001a53d485"),
            Name = "Washington",
            CountryId = ObjectId.Parse("60c78a3b4b4d8e001a53d46e")
        }
    };
    citiesCollection?.InsertMany(citiesDocuments);
}