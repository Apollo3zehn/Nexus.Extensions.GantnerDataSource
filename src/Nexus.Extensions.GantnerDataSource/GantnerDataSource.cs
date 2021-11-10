using Microsoft.Extensions.Logging;
using Nexus.DataModel;
using Nexus.Extensibility;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using UDBF.NET;

namespace Nexus.Extensions.GantnerDataSource
{
    [ExtensionIdentification("Gantner", "Gantner", "Provides access to databases with Gantner UDBF files.")]
    public class GantnerDataSource : StructuredFileDataSource
    {
        #region Fields

        private Dictionary<string, CatalogDescription> _config;

        #endregion

        #region Properties

        private DataSourceContext Context { get; set; }

        #endregion

        #region Methods

        protected override async Task SetContextAsync(DataSourceContext context, CancellationToken cancellationToken)
        {
            this.Context = context;

            var configFilePath = Path.Combine(this.Root, "config.json");

            if (!File.Exists(configFilePath))
                throw new Exception($"Configuration file {configFilePath} not found.");

            _config = await JsonSerializerHelper
                .DeserializeAsync<Dictionary<string, CatalogDescription>>(configFilePath);
        }

        protected override Task<FileSourceProvider> GetFileSourceProviderAsync(CancellationToken cancellationToken)
        {
            var all = _config.ToDictionary(
                config => config.Key,
                config => config.Value.FileSources.Cast<FileSource>().ToArray());

            return Task.FromResult(new FileSourceProvider(
                All: all,
                Single: catalogItem => all[catalogItem.Catalog.Id].First()));
        }

        protected override Task<string[]> GetCatalogIdsAsync(CancellationToken cancellationToken)
        {
            return Task.FromResult(_config.Keys.ToArray());
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

                        var resource = new ResourceBuilder(id: gantnerVariable.Name)
                            .WithUnit(gantnerVariable.Unit)
                            .WithGroups(fileSource.Name)
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

                var gantnerVariable = gantnerFile.Variables.First(current => current.Name == info.CatalogItem.Resource.Id);
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
            });
        }

        #endregion
    }
}
