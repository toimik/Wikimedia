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

    /// <inheritdoc/>
    /// <remarks>
    /// This is customized for datasets that use schema version 1.29 and above.
    /// </remarks>
    public class V129ExternalLinksExtractor : ExternalLinksExtractor
    {
        public V129ExternalLinksExtractor(DecompressStreamFactory decompressStreamFactory = null)
            : base(decompressStreamFactory)
        {
        }

        protected override IEnumerable<string> Extract(string line)
        {
            // e.g. INSERT INTO `externallinks` VALUES
            // (...,...,'...','...','...')[,(...,...,'...','...','...')]*;

            // Skip the prefix
            line = line[Prefix.Length..];
            do
            {
                // Skip the opening parenthesis
                line = line[1..];

                // Skip the first column
                var commaIndex = line.IndexOf(',');
                line = line[(commaIndex + 1)..];

                // Skip the second column and the opening single quote
                commaIndex = line.IndexOf(',');
                line = line[(commaIndex + 2)..];

                // Yield the third column
                var extractedLink = ExtractLink(line);
                yield return extractedLink.Escaped;

                // Skip the fourth and fifth columns
                for (int i = 0; i < 2; i++)
                {
                    line = line[(extractedLink.Unescaped.Length + 1)..];
                    commaIndex = line.IndexOf(',');
                    line = line[(commaIndex + 2)..];
                    extractedLink = ExtractLink(line);
                }

                // Check if there is any more values. If there is, the first character starts with a
                // comma. Otherwise, it starts with a semi-colon.
                line = line[(extractedLink.Unescaped.Length + 2)..];
                if (line.StartsWith(';'))
                {
                    break;
                }

                // Remove the comma
                line = line[1..];

                // Repeat the process by continuing with the loop
            }
            while (true);
        }
    }
}