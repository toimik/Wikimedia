﻿namespace Toimik.Wikimedia.Tests;

using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Moq;
using Moq.Protected;
using Xunit;

public class ExternalLinksStreamerTest
{
    private static readonly string DataDirectory = $"Data{Path.DirectorySeparatorChar}";

    [Fact]
    public async Task OffsetToNegative()
    {
        var httpClient = CreateHttpClient("externallinks.sql");
        var extractor = new V129ExternalLinksExtractor(new DummyDecompressStreamFactory());
        var streamer = new ExternalLinksStreamer(httpClient, extractor);
        var expectedUrls = new List<string>
        {
            "http://1a.example.com/bleedin\'",
            "http://2a.example.com",
            "//3a.example.com",
        };

        var offset = -1;
        await foreach (ExternalLinksExtractor.Result result in streamer.Stream(new Uri("http://example.com"), offset).ConfigureAwait(false))
        {
            var expectedUrl = expectedUrls[result.Index];
            Assert.Equal(expectedUrl, result.Url);
        }
    }

    private static HttpClient CreateHttpClient(string filename)
    {
        var mock = new Mock<HttpMessageHandler>();
        mock.Protected()
           .SetupSequence<Task<HttpResponseMessage>>(
                "SendAsync",
                ItExpr.IsAny<HttpRequestMessage>(),
                ItExpr.IsAny<CancellationToken>())
           .ReturnsAsync(new HttpResponseMessage
           {
               Content = new StreamContent(File.OpenRead($"{DataDirectory}{filename}")),
           });
        return new HttpClient(mock.Object);
    }
}