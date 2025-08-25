#nullable enable

#r "nuget: MongoDB.Driver, 2.25.0"
#r "nuget: CsvHelper, 32.0.3"

using CsvHelper;
using CsvHelper.Configuration;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using System.Globalization;

// =================================================================================
// MODELO DE DADOS
// =================================================================================
public record class SteamGame(
    int AppID,
    string Name,
    [property: BsonElement("ReleaseDate")] string ReleaseDate,
    [property: BsonElement("EstimatedOwners")] string EstimatedOwners,
    double Price,
    List<string> Developers,
    List<string> Publishers,
    List<string> Categories,
    List<string> Genres,
    List<string> Tags,
    [property: BsonElement("PositiveReviews")] int Positive,
    [property: BsonElement("NegativeReviews")] int Negative,
    [property: BsonElement("SupportsWindows")] bool Windows,
    [property: BsonElement("SupportsMac")] bool Mac,
    [property: BsonElement("SupportsLinux")] bool Linux
)
{
    [BsonId]
    [BsonRepresentation(BsonType.ObjectId)]
    public string? Id { get; set; }
}

// =================================================================================
// MAPEAMENTO PARA CSVHELPER
// =================================================================================
public sealed class SteamGameMap : ClassMap<SteamGame>
{
    public SteamGameMap()
    {
        AutoMap(CultureInfo.InvariantCulture);

        Map(m => m.ReleaseDate).Name("Release date");
        Map(m => m.EstimatedOwners).Name("Estimated owners");

        var listConverter = (string? text) =>
            string.IsNullOrWhiteSpace(text)
            ? new List<string>()
            : [.. text.Split(',').Select(i => i.Trim())];

        Map(m => m.Developers).Convert(args => listConverter(args.Row.GetField("Developers")));
        Map(m => m.Publishers).Convert(args => listConverter(args.Row.GetField("Publishers")));
        Map(m => m.Categories).Convert(args => listConverter(args.Row.GetField("Categories")));
        Map(m => m.Genres).Convert(args => listConverter(args.Row.GetField("Genres")));
        Map(m => m.Tags).Convert(args => listConverter(args.Row.GetField("Tags")));
    }
}


// =================================================================================
// LÓGICA PRINCIPAL E MÉTODOS
// =================================================================================
const string ConnectionString = "xxx";
const string DatabaseName = "SteamDB";
const string CollectionName = "Games";
const string CsvFilePath = "teste.csv"; // <-- ALTERADO PARA O TESTE

var client = new MongoClient(ConnectionString);
var database = client.GetDatabase(DatabaseName);
var collection = database.GetCollection<SteamGame>(CollectionName);

await ImportGamesFromCsv(collection, CsvFilePath);
await QueryGames(collection);

Console.WriteLine("\nProcesso concluído.");


async Task ImportGamesFromCsv(IMongoCollection<SteamGame> gameCollection, string filePath)
{
    Console.WriteLine($"Iniciando importação com CsvHelper do arquivo '{filePath}'...");
    if (!File.Exists(filePath))
    {
        Console.WriteLine($"ERRO: Arquivo '{filePath}' não encontrado.");
        return;
    }

    await database.DropCollectionAsync(CollectionName);

    try
    {
        var config = new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            HeaderValidated = null,
            MissingFieldFound = null,
        };

        using var reader = new StreamReader(filePath);
        using var csv = new CsvReader(reader, config);
        
        csv.Context.RegisterClassMap<SteamGameMap>();
        var records = csv.GetRecords<SteamGame>().ToList();

        if (records.Any())
        {
            await gameCollection.InsertManyAsync(records);
            Console.WriteLine($"{records.Count} jogos importados com sucesso!");
        }
        else
        {
            Console.WriteLine("Nenhum jogo foi importado.");
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Ocorreu um erro fatal durante a leitura do CSV: {ex.Message}");
    }
}

async Task QueryGames(IMongoCollection<SteamGame> gameCollection)
{
    Console.WriteLine("\n--- Realizando Consultas ---");

    Console.WriteLine("\n[Consulta 1: Top 5 jogos gratuitos para Linux]");
    var freeLinuxGamesFilter = Builders<SteamGame>.Filter.And(
        Builders<SteamGame>.Filter.Eq(g => g.Price, 0.0),
        Builders<SteamGame>.Filter.Eq(g => g.Linux, true)
    );
    var freeLinuxGames = await gameCollection.Find(freeLinuxGamesFilter).Limit(5).ToListAsync();
    foreach (var game in freeLinuxGames)
    {
        Console.WriteLine($"- {game.Name} (Desenvolvedor: {string.Join(", ", game.Developers)})");
    }

    Console.WriteLine("\n[Consulta 2: Jogos da 'Rusty Moyher']");
    var rustyGames = await gameCollection.Find(g => g.Developers.Contains("Rusty Moyher")).ToListAsync();
    foreach (var game in rustyGames)
    {
        Console.WriteLine($"- {game.Name} (Tags: {string.Join(", ", game.Tags)})");
    }

    Console.WriteLine("\n[Consulta 3: Jogos populares e baratos (Positivas > 80, Preço < $10)]");
    var popularAndCheapFilter = Builders<SteamGame>.Filter.And(
        Builders<SteamGame>.Filter.Gt(g => g.Positive, 80),
        Builders<SteamGame>.Filter.Lt(g => g.Price, 10.0),
        Builders<SteamGame>.Filter.Ne(g => g.Price, 0.0)
    );
    var popularAndCheap = await gameCollection.Find(popularAndCheapFilter).Limit(5).ToListAsync();
    foreach (var game in popularAndCheap)
    {
         Console.WriteLine($"- {game.Name} (Positivas: {game.Positive}, Preço: ${game.Price})");
    }
}