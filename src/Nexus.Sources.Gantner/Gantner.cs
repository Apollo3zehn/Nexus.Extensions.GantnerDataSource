using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using Nexus.DataModel;
using Nexus.Extensibility;
using UDBF.NET;

namespace Nexus.Sources;

/// <summary>
/// Additional extension-specific settings.
/// </summary>
/// <param name="TitleMap">The catalog ID to title map. Add an entry here to specify a custom catalog title.</param>
public record GantnerSettings(
    Dictionary<string, string> TitleMap
);

/// <summary>
/// Additional file source settings.
/// </summary>
/// <param name="CatalogSourceFiles">The source files to populate the catalog with resources.</param>
public record GantnerAdditionalFileSourceSettings(
    string[]? CatalogSourceFiles
);

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

[ExtensionDescription(
    "Provides access to databases with Gantner UDBF files.",
    "https://github.com/Apollo3zehn/nexus-sources-gantner",
    "https://github.com/Apollo3zehn/nexus-sources-gantner")]
public class Gantner : StructuredFileDataSource<GantnerSettings, GantnerAdditionalFileSourceSettings>
{
    protected override Task<CatalogRegistration[]> GetCatalogRegistrationsAsync(
        string path,
        CancellationToken cancellationToken
    )
    {
        if (path == "/")
        {
            return Task.FromResult(Context.SourceConfiguration.FileSourceGroupsMap
                .Select(entry =>
                    {
                        Context.SourceConfiguration.AdditionalSettings.TitleMap.TryGetValue(entry.Key, out var title);
                        return new CatalogRegistration(entry.Key, title);
                    }
                ).ToArray());
        }

        else
        {
            return Task.FromResult(Array.Empty<CatalogRegistration>());
        }
    }

    protected override Task<ResourceCatalog> EnrichCatalogAsync(
        ResourceCatalog catalog,
        CancellationToken cancellationToken
    )
    {
        var fileSourceGroupsMap = Context.SourceConfiguration.FileSourceGroupsMap[catalog.Id];

        foreach (var (fileSourceId, fileSourceGroup) in fileSourceGroupsMap)
        {
            foreach (var fileSource in fileSourceGroup)
            {
                var additionalSettings = fileSource.AdditionalSettings;
                var filePaths = default(string[]);

                if (additionalSettings.CatalogSourceFiles is not null)
                {
                    filePaths = additionalSettings.CatalogSourceFiles
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
                    var catalogBuilder = new ResourceCatalogBuilder(id: catalog.Id);

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

    protected override Task ReadAsync(
        ReadInfo<GantnerAdditionalFileSourceSettings> info,
        ReadRequest[] readRequests,
        CancellationToken cancellationToken
    )
    {
        return Task.Run(() =>
        {
            foreach (var readRequest in readRequests)
            {
                using var gantnerFile = new UDBFFile(info.FilePath);
                
                var gantnerVariable = gantnerFile.Variables
                    .FirstOrDefault(current => current.Name == readRequest.OriginalResourceName);

                if (gantnerVariable != default)
                {
                    var (timeStamps, data) = gantnerFile.Read<byte>(gantnerVariable);
                    var result = data.Buffer;
                    var elementSize = readRequest.CatalogItem.Representation.ElementSize;

                    cancellationToken.ThrowIfCancellationRequested();

                    // write data
                    var offset = (int)info.FileOffset * elementSize;
                    var length = Math.Min(result.Length, (int)info.FileBlock * elementSize);

                    result
                        .AsMemory()
                        .Slice(offset, length)
                        .CopyTo(readRequest.Data);

                    readRequest
                        .Status
                        .Span
                        .Slice(0, length / elementSize)
                        .Fill(1);
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
}

#pragma warning restore CS1591 // Missing XML comment for publicly visible type or member