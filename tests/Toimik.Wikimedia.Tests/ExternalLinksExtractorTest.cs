namespace Toimik.Wikimedia.Tests
{
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Threading.Tasks;
    using Xunit;

    public class ExternalLinksExtractorTest
    {
        private static readonly string DataDirectory = $"Data{Path.DirectorySeparatorChar}";

        [Fact]
        public async Task Empty()
        {
            var extractor = new V129ExternalLinksExtractor();

            var results = await extractor.Extract($"{DataDirectory}empty.sql.gz").ToListAsync();

            Assert.Empty(results);
        }

        [Fact]
        public async Task OffsetToFirst()
        {
            var extractor = new V129ExternalLinksExtractor(new DummyDecompressStreamFactory());
            var expectedUrls = new List<string>
            {
                "http://1a.example.com/bleedin\'",
                "http://2a.example.com",
                "//3a.example.com",
            };

            await foreach (ExternalLinksExtractor.Result result in extractor.Extract($"{DataDirectory}externallinks.sql"))
            {
                var expectedUrl = expectedUrls[result.Index];
                Assert.Equal(expectedUrl, result.Url);
            }
        }

        [Fact]
        public async Task OffsetToLast()
        {
            var extractor = new V129ExternalLinksExtractor(new DummyDecompressStreamFactory());

            var offset = 2;
            await foreach (ExternalLinksExtractor.Result result in extractor.Extract($"{DataDirectory}externallinks.sql", offset))
            {
                Assert.Equal(offset, result.Index);
                Assert.Equal("//3a.example.com", result.Url);
            }
        }
    }
}