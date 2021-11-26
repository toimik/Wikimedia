/*
 * Copyright 2021 nurhafiz@hotmail.sg
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

namespace Toimik.Wikimedia
{
    using System.Collections.Generic;
    using System.IO;

    using System.Runtime.CompilerServices;
    using System.Threading;

    /// <summary>
    /// Represents a class that extracts URLs from Wikimedia's "externallinks" datasets.
    /// </summary>
    /// <remarks>
    /// Wikimedia - the owner of Wikipedia - provides free copies of Wikipedia's periodic datasets,
    /// some of which are collections of URLs that point to third parties' resources.
    /// <para>
    /// The datasets are listed at https://dumps.wikimedia.org/backup-index.html and are also
    /// available via their mirrors at https://dumps.wikimedia.org/mirrors.html.
    /// </para>
    /// <para>
    /// Of particular interest are datasets whose filename is suffixed with "externallinks.sql.gz"
    /// and prefixed with "&lt;xx&gt;wiki" where "&lt;xx&gt;" is the first two / three
    /// language-specific characters. e.g. <c>enwiki...externallinks.sql.gz</c>,
    /// <c>ruwiki...externallinks.sql.gz</c>.
    /// </para>
    /// <para>
    /// As the filename's extension implies, each link points to an SQL file that is compressed
    /// using GZip. Specifically, each is a MySQL script to be fed to a MySQL program, which will
    /// auto-decompress the file to create and populate a table based on the respective schema
    /// detailed at https://www.mediawiki.org/wiki/Manual:Externallinks_table.
    /// </para>
    /// <para>
    /// However, this class extracts the URLs without importing them to MySQL at all so as to avoid
    /// requiring MySQL installed and using additional disk space / memory.
    /// </para>
    /// <strong>Parsing</strong>
    /// <para>
    /// As the schema has changed over the years, it is assumed that it may be changed again in
    /// future. Thus, this class serves as the base class for current and future implementations.
    /// </para>
    /// <para>
    /// It is assumed that the relevant lines start exactly with <c>INSERT INTO `externallinks`
    /// VALUES</c> and may contain multiple comma-separated values in the form of <c>(...),
    /// (...)</c>.
    /// </para>
    /// <para>
    /// Additionally, it is assumed that single quotes are used to enclose the URLs. As such,
    /// literal single quotes are escaped like this: <c>\'</c>.
    /// </para>
    /// <para>
    /// The values are extracted as-is without any validation or detection of duplicates. Also, it
    /// is not guaranteed that all of them are absolute URLs.
    /// </para>
    /// </remarks>
    public abstract class ExternalLinksExtractor
    {
        protected const string Prefix = "INSERT INTO `externallinks` VALUES ";

        protected ExternalLinksExtractor(DecompressStreamFactory decompressStreamFactory = null)
        {
            DecompressStreamFactory = decompressStreamFactory ?? new DecompressStreamFactory();
        }

        public DecompressStreamFactory DecompressStreamFactory { get; }

        /// <summary>
        /// Extracts, for this instance, all URLs.
        /// </summary>
        /// <param name="path">
        /// The absolute path to a local <c>externallinks.sql.gz</c> file to extract URLs from.
        /// </param>
        /// <param name="offset">
        /// The offset of the URLs to start from.
        /// </param>
        /// <param name="cancellationToken">
        /// Optional token to monitor for cancellation request.
        /// </param>
        /// <returns>
        /// <see cref="Result"/>(s).
        /// </returns>
        public async IAsyncEnumerable<Result> Extract(
            string path,
            int offset = 0,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            using var stream = File.OpenRead(path);
            var results = Extract(
                stream,
                offset,
                cancellationToken);
            await foreach (Result extractionResult in results)
            {
                yield return extractionResult;
            }
        }

        /// <summary>
        /// Extracts, for this instance, all URLs.
        /// </summary>
        /// <param name="stream">
        /// The <see cref="Stream"/> of the <c>externallinks.sql.gz</c> file to extract URLs from.
        /// </param>
        /// <param name="offset">
        /// The offset of the URLs to start from.
        /// </param>
        /// <param name="cancellationToken">
        /// Optional token to monitor for cancellation request.
        /// </param>
        /// <returns>
        /// <see cref="Result"/>(s).
        /// </returns>
        public async IAsyncEnumerable<Result> Extract(
            Stream stream,
            int offset = 0,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            if (offset < 0)
            {
                offset = 0;
            }

            using var tempStream = DecompressStreamFactory.CreateDecompressStream(stream);
            using var reader = new StreamReader(tempStream);

            string line;
            var index = 0;

            // Skip, if any, the URLs before the offset
            if (offset > 0)
            {
                IEnumerator<string> enumerator = null;
                while ((line = await reader.ReadLineAsync()) != null)
                {
                    if (!line.StartsWith(Prefix))
                    {
                        continue;
                    }

                    var urls = Extract(line);
                    enumerator = urls.GetEnumerator();
                    while (enumerator.MoveNext()
                        && index < offset)
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        index++;
                    }
                }

                // Yield, if any, the URLs that are in the same line but are after the offset
                if (enumerator != null)
                {
                    do
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        var url = enumerator.Current;
                        yield return new Result(index, url);
                        index++;
                    }
                    while (enumerator.MoveNext());
                }
            }

            // Yield, if any, the rest of the URLs in the rest of the lines
            while ((line = await reader.ReadLineAsync()) != null)
            {
                if (!line.StartsWith(Prefix))
                {
                    continue;
                }

                foreach (string url in Extract(line))
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    yield return new Result(index, url);
                    index++;
                }
            }
        }

        // NOTE: This helper is internal so that implementations for other schemas can use it
        internal static ExtractedLink ExtractLink(string line)
        {
            bool isEscapeRequired = false;
            string unescapedUrl;
            var startIndex = 0;
            do
            {
                var quoteIndex = line.IndexOf("'", startIndex);
                var precedingCharacters = line.Substring(quoteIndex - 1, 1);
                if (!precedingCharacters.Equals("\\"))
                {
                    unescapedUrl = line[..quoteIndex];
                    break;
                }

                // e.g. 'http://www.example.com/bleedin\''
                startIndex = quoteIndex + 1;
                isEscapeRequired = true;
            }
            while (true);

            var escapedUrl = isEscapeRequired
                ? unescapedUrl.Replace("\\'", "'")
                : unescapedUrl;
            return new ExtractedLink(unescapedUrl, escapedUrl);
        }

        protected abstract IEnumerable<string> Extract(string line);

        public struct Result
        {
            public Result(int index, string url)
            {
                Index = index;
                Url = url;
            }

            public int Index { get; }

            // NOTE: It is observed that this value can be absolute, relative or incorrect (use of
            // forward slashes) but self-correcting when creating a Uri
            public string Url { get; }
        }
    }
}