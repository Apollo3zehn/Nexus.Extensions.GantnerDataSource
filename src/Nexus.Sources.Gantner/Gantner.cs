using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Nexus.DataModel;
using Nexus.Extensibility;
using UDBF.NET;

namespace Nexus.Sources;

[ExtensionDescription(
    "Provides access to databases with Gantner UDBF files.",
    "https://github.com/Apollo3zehn/nexus-sources-gantner",
    "https://github.com/Apollo3zehn/nexus-sources-gantner")]
public class Gantner : StructuredFileDataSource
{
    record CatalogDescription(
        string Title,
        Dictionary<string, IReadOnlyList<FileSource>> FileSourceGroups,
        JsonElement? AdditionalProperties);

    #region Fields

    private Dictionary<string, CatalogDescription> _config = default!;

    #endregion

    #region Methods

    protected override async Task InitializeAsync(CancellationToken cancellationToken)
    {
        var configFilePath = Path.Combine(Root, "config.json");

        if (!File.Exists(configFilePath))
            throw new Exception($"Configuration file {configFilePath} not found.");

        var jsonString = await File.ReadAllTextAsync(configFilePath, cancellationToken);
        _config = JsonSerializer.Deserialize<Dictionary<string, CatalogDescription>>(jsonString) ?? throw new Exception("config is null");
    }

    protected override Task<Func<string, Dictionary<string, IReadOnlyList<FileSource>>>> GetFileSourceProviderAsync(
        CancellationToken cancellationToken)
    {
        return Task.FromResult<Func<string, Dictionary<string, IReadOnlyList<FileSource>>>>(
            catalogId => _config[catalogId].FileSourceGroups);
    }

    protected override Task<CatalogRegistration[]> GetCatalogRegistrationsAsync(string path, CancellationToken cancellationToken)
    {
        if (path == "/")
            return Task.FromResult(_config.Select(entry => new CatalogRegistration(entry.Key, entry.Value.Title)).ToArray());

        else
            return Task.FromResult(Array.Empty<CatalogRegistration>());
    }

    protected override Task<ResourceCatalog> GetCatalogAsync(string catalogId, CancellationToken cancellationToken)
    {
        var catalogDescription = _config[catalogId];
        var catalog = new ResourceCatalog(id: catalogId);

        foreach (var (fileSourceId, fileSourceGroup) in catalogDescription.FileSourceGroups)
        {
            foreach (var fileSource in fileSourceGroup)
            {
                var filePaths = default(string[]);
                var catalogSourceFiles = fileSource.AdditionalProperties?.GetStringArray("CatalogSourceFiles");

                if (catalogSourceFiles is not null)
                {
                    filePaths = catalogSourceFiles
                        .Where(filePath => filePath is not null)
                        .Select(filePath => Path.Combine(Root, filePath!))
                        .ToArray();
                }
                else
                {
                    if (!TryGetFirstFile(fileSource, out var filePath))
                        continue;

                    filePaths = [filePath];
                }

                cancellationToken.ThrowIfCancellationRequested();

                foreach (var filePath in filePaths)
                {
                    var catalogBuilder = new ResourceCatalogBuilder(id: catalogId);

                    using var gantnerFile = new UDBFFile(filePath);
                    var gantnerVariables = gantnerFile.Variables.Where(x => x.DataDirection == UDBFDataDirection.Input || x.DataDirection == UDBFDataDirection.InputOutput);

                    foreach (var gantnerVariable in gantnerVariables)
                    {
                        var samplePeriod = TimeSpan.FromSeconds(1.0 / gantnerFile.SampleRate);

                        var representation = new Representation(
                            dataType: GantnerUtilities.GetNexusDataTypeFromUdbfDataType(gantnerVariable.DataType),
                            samplePeriod: samplePeriod);

                        if (!TryEnforceNamingConvention(gantnerVariable.Name, out var resourceId))
                            continue;

                        var resource = new ResourceBuilder(id: resourceId)
                            .WithUnit(gantnerVariable.Unit)
                            .WithGroups(fileSourceId)
                            .WithFileSourceId(fileSourceId)
                            .WithOriginalName(gantnerVariable.Name)
                            .AddRepresentation(representation)
                            .Build();

                        catalogBuilder.AddResource(resource);
                    }

                    catalog = catalog.Merge(catalogBuilder.Build());
                }
            }
        }

        return Task.FromResult(catalog);
    }

    protected override Task ReadAsync(ReadInfo info, StructuredFileReadRequest[] readRequests, CancellationToken cancellationToken)
    {
        return Task.Run(() =>
        {
            foreach (var readRequest in readRequests)
            {
                using var gantnerFile = new UDBFFile(info.FilePath);
                
                var gantnerVariable = gantnerFile.Variables
                    .FirstOrDefault(current => current.Name == readRequest.OriginalName);

                if (gantnerVariable != default)
                {
                    var (timeStamps, data) = gantnerFile.Read<byte>(gantnerVariable);
                    var result = data.Buffer;
                    var elementSize = readRequest.CatalogItem.Representation.ElementSize;

                    cancellationToken.ThrowIfCancellationRequested();

                    // write data
                    if (result.Length == info.FileLength * elementSize)
                    {
                        var offset = (int)info.FileOffset * elementSize;
                        var length = (int)info.FileBlock * elementSize;

                        result
                            .AsMemory()
                            .Slice(offset, length)
                            .CopyTo(readRequest.Data);

                        readRequest
                            .Status
                            .Span
                            .Fill(1);
                    }
                    // skip data
                    else
                    {
                        Logger.LogDebug("The actual buffer size does not match the expected size, which indicates an incomplete file");
                    }
                }
            }
        }, cancellationToken);
    }

    private static bool TryEnforceNamingConvention(string resourceId, [NotNullWhen(returnValue: true)] out string newResourceId)
    {
        newResourceId = resourceId;
        newResourceId = Resource.InvalidIdCharsExpression.Replace(newResourceId, "");
        newResourceId = Resource.InvalidIdStartCharsExpression.Replace(newResourceId, "");

        return Resource.ValidIdExpression.IsMatch(newResourceId);
    }

    #endregion
}
