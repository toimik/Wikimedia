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

            var extractionResults = await extractor.Extract($"{DataDirectory}empty.sql.gz").ToListAsync();

            Assert.Empty(extractionResults);
        }

        [Fact]
        public async Task StartFromIndexThatIsFirst()
        {
            var extractor = new V129ExternalLinksExtractor(new DummyDecompressStreamFactory());
            var expectedUrls = new List<string>
            {
                "http://1a.example.com/bleedin\'",
                "http://2a.example.com",
                "//3a.example.com",
            };

            await foreach (ExtractionResult extractionResult in extractor.Extract($"{DataDirectory}externallinks.sql"))
            {
                var expectedUrl = expectedUrls[extractionResult.Index];
                Assert.Equal(expectedUrl, extractionResult.Url);
            }
        }

        [Fact]
        public async Task StartFromIndexThatIsLast()
        {
            var extractor = new V129ExternalLinksExtractor(new DummyDecompressStreamFactory());

            var startIndex = 2;
            await foreach (ExtractionResult extractionResult in extractor.Extract($"{DataDirectory}externallinks.sql", startIndex))
            {
                Assert.Equal(startIndex, extractionResult.Index);
                Assert.Equal("//3a.example.com", extractionResult.Url);
            }
        }
    }
}