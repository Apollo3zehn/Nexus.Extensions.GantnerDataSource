using Microsoft.Extensions.Logging;
using Nexus.DataModel;
using Nexus.Extensibility;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using UDBF.NET;

namespace Nexus.Sources
{
    [ExtensionDescription("Provides access to databases with Gantner UDBF files.")]
    public class Gantner : StructuredFileDataSource
    {
        #region Fields

        private Dictionary<string, CatalogDescription> _config = null!;

        #endregion

        #region Properties

        private DataSourceContext Context { get; set; } = null!;

        #endregion

        #region Methods

        protected override async Task SetContextAsync(DataSourceContext context, CancellationToken cancellationToken)
        {
            this.Context = context;

            var configFilePath = Path.Combine(this.Root, "config.json");

            if (!File.Exists(configFilePath))
                throw new Exception($"Configuration file {configFilePath} not found.");

            var jsonString = await File.ReadAllTextAsync(configFilePath, cancellationToken);
            _config = JsonSerializer.Deserialize<Dictionary<string, CatalogDescription>>(jsonString) ?? throw new Exception("config is null");
        }

        protected override Task<FileSourceProvider> GetFileSourceProviderAsync(CancellationToken cancellationToken)
        {
            var allFileSources = _config.ToDictionary(
                config => config.Key,
                config => config.Value.FileSources.Cast<FileSource>().ToArray());

            var fileSourceProvider = new FileSourceProvider(
                All: allFileSources,
                Single: catalogItem =>
                {
                    var properties = catalogItem.Resource.Properties;

                    if (properties is null)
                        throw new ArgumentNullException(nameof(properties));

                    return allFileSources[catalogItem.Catalog.Id]
                        .First(fileSource => ((ExtendedFileSource)fileSource).Name == properties["FileSource"]);
                });

            return Task.FromResult(fileSourceProvider);
        }

        protected override Task<CatalogRegistration[]> GetCatalogRegistrationsAsync(string path, CancellationToken cancellationToken)
        {
            if (path == "/")
                return Task.FromResult(_config.Keys.Select(catalogId => new CatalogRegistration(catalogId)).ToArray());

            else
                return Task.FromResult(new CatalogRegistration[0]);
        }

        protected override Task<ResourceCatalog> GetCatalogAsync(string catalogId, CancellationToken cancellationToken)
        {
            var catalogDescription = _config[catalogId];
            var catalog = new ResourceCatalog(id: catalogId);

            foreach (var fileSource in catalogDescription.FileSources)
            {
                var filePaths = default(string[]);

                if (fileSource.CatalogSourceFiles is not null)
                {
                    filePaths = fileSource.CatalogSourceFiles
                        .Select(filePath => Path.Combine(this.Root, filePath))
                        .ToArray();
                }
                else
                {
                    if (!this.TryGetFirstFile(fileSource, out var filePath))
                        continue;

                    filePaths = new[] { filePath };
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

                        var resourceId = GantnerUtilities.EnforceNamingConvention(gantnerVariable.Name);

                        var resource = new ResourceBuilder(id: resourceId)
                            .WithUnit(gantnerVariable.Unit)
                            .WithGroups(fileSource.Name)
                            .WithProperty("FileSource", fileSource.Name)
                            .AddRepresentation(representation)
                            .Build();

                        catalogBuilder.AddResource(resource);
                    }

                    catalog = catalog.Merge(catalogBuilder.Build(), MergeMode.NewWins);
                }
            }

            return Task.FromResult(catalog);
        }

        protected override Task ReadSingleAsync(ReadInfo info, CancellationToken cancellationToken)
        {
            return Task.Run(() =>
            {
                using var gantnerFile = new UDBFFile(info.FilePath);

                var gantnerVariable = gantnerFile.Variables.First(current =>
                    GantnerUtilities.EnforceNamingConvention(current.Name) == info.CatalogItem.Resource.Id);

                if (gantnerVariable != null)
                {
                    var gantnerData = gantnerFile.Read<byte>(gantnerVariable);
                    var result = gantnerData.Data.Buffer;
                    var elementSize = info.CatalogItem.Representation.ElementSize;

                    cancellationToken.ThrowIfCancellationRequested();

                    // write data
                    if (result.Length == info.FileLength * elementSize)
                    {
                        var offset = (int)info.FileOffset * elementSize;

                        result
                            .AsMemory()
                            .Slice(offset)
                            .CopyTo(info.Data);

                        info
                            .Status
                            .Span
                            .Fill(1);
                    }
                    // skip data
                    else
                    {
                        this.Context.Logger.LogDebug("The actual buffer size does not match the expected size, which indicates an incomplete file");
                    }
                }
            });
        }

        #endregion
    }
}
