﻿/*
 * Copyright 2021-2024 nurhafiz@hotmail.sg
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Toimik.Wikimedia;

using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Represents a class to stream URLs via HTTP from publicly available Wikimedia Downloads' datasets.
/// </summary>
public sealed class ExternalLinksStreamer(HttpClient httpClient, ExternalLinksExtractor extractor)
{
    public ExternalLinksExtractor Extractor { get; } = extractor;

    public HttpClient HttpClient { get; } = httpClient;

    /// <summary>
    /// Streams, for this instance, all URLs.
    /// </summary>
    /// <param name="dataset">
    /// The <see cref="Uri"/> of the <c>externallinks.sql.gz</c> file to extract URLs from.
    /// </param>
    /// <param name="offset">The offset of the URLs to start from.</param>
    /// <param name="cancellationToken">Optional token to monitor for cancellation request.</param>
    /// <returns><see cref="ExternalLinksExtractor.Result"/>(s).</returns>
    /// <remarks>
    /// <para>
    /// <strong>Known Issue</strong> Streaming some large files over <c>HTTPS</c> may throw a
    /// <c>System.IO.IOException : Received an unexpected EOF or 0 bytes from the transport stream</c>.
    /// </para>
    /// <para>If that happens, use <c>HTTP</c> instead.</para>
    /// </remarks>
    public async IAsyncEnumerable<ExternalLinksExtractor.Result> Stream(
        Uri dataset,
        int offset = 0,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        using var stream = await Connect(dataset, cancellationToken).ConfigureAwait(false);
        var results = Extractor.Extract(
            stream,
            offset,
            cancellationToken);
        await foreach (ExternalLinksExtractor.Result result in results.ConfigureAwait(false))
        {
            yield return result;
        }
    }

    private async Task<Stream> Connect(Uri dataset, CancellationToken cancellationToken)
    {
        var requestMessage = new HttpRequestMessage(HttpMethod.Get, dataset);
        var responseMessage = await HttpClient.SendAsync(
            requestMessage,
            HttpCompletionOption.ResponseHeadersRead,
            cancellationToken).ConfigureAwait(false);
        var stream = await responseMessage.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
        return stream;
    }
}