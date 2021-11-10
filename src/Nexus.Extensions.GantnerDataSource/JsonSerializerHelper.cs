using System.IO;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace Nexus.Extensions.GantnerDataSource
{
    public static class JsonSerializerHelper
    {
        public static void Serialize<T>(T value, string filePath)
        {
            var options = new JsonSerializerOptions()
            {
                WriteIndented = true,
                DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingDefault
            };

            options.Converters.Add(new TimeSpanConverter());
            var jsonString = JsonSerializer.Serialize(value, options);

            File.WriteAllText(filePath, jsonString);
        }

        public static T Deserialize<T>(string filePath)
        {
            var jsonString = File.ReadAllText(filePath);

            var options = new JsonSerializerOptions();
            options.Converters.Add(new TimeSpanConverter());

            return JsonSerializer.Deserialize<T>(jsonString, options);
        }

        public static async Task<T> DeserializeAsync<T>(string filePath)
        {
            using var jsonStream = File.OpenRead(filePath);

            var options = new JsonSerializerOptions();
            options.Converters.Add(new TimeSpanConverter());

            return await JsonSerializer
                .DeserializeAsync<T>(jsonStream, options)
                .AsTask()
                .ConfigureAwait(false);
        }
    }
}
