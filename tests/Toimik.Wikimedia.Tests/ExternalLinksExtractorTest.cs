namespace Toimik.Wikimedia.Tests;

using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

public abstract class ExternalLinksExtractorTest
{
    protected static readonly string DataDirectory = $"Data{Path.DirectorySeparatorChar}";

    [Fact]
    public async Task Empty()
    {
        var extractor = CreateExtractor();

        var results = await extractor.Extract($"{DataDirectory}empty.sql.gz").ToListAsync().ConfigureAwait(false);

        Assert.Empty(results);
    }

    [Fact]
    public async Task OffsetToBeyond()
    {
        var extractor = CreateExtractor(new DummyDecompressStreamFactory());

        var offset = 3;
        var results = await extractor.Extract($"{DataDirectory}externallinks.sql", offset).ToListAsync().ConfigureAwait(false);
        Assert.Empty(results);
    }

    [Fact]
    public async Task OffsetToFirst()
    {
        var extractor = CreateExtractor(new DummyDecompressStreamFactory());
        var expectedUrls = new List<string>
        {
            "http://1a.example.com/bleedin\'",
            "http://2a.example.com",
            "//3a.example.com",
        };

        await foreach (ExternalLinksExtractor.Result result in extractor.Extract($"{DataDirectory}externallinks.sql").ConfigureAwait(false))
        {
            var expectedUrl = expectedUrls[result.Index];
            Assert.Equal(expectedUrl, result.Url);
        }
    }

    [Fact]
    public async Task OffsetToLast()
    {
        var extractor = CreateExtractor(new DummyDecompressStreamFactory());

        var offset = 2;
        await foreach (ExternalLinksExtractor.Result result in extractor.Extract($"{DataDirectory}externallinks.sql", offset).ConfigureAwait(false))
        {
            Assert.Equal(offset, result.Index);
            Assert.Equal("//3a.example.com", result.Url);
        }
    }

    protected abstract ExternalLinksExtractor CreateExtractor(DecompressStreamFactory? decompressStreamFactory = null);
}