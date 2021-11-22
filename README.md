![GitHub Workflow Status](https://img.shields.io/github/workflow/status/toimik/Wikimedia/CI)
![Code Coverage](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/nurhafiz/b8b90b86301563c67f191b24f506ef15/raw/Wikimedia-coverage.json)
![Nuget](https://img.shields.io/nuget/v/Toimik.Wikimedia)

# Toimik.Wikimedia

.NET 5 C# [Wikimedia Downloads](https://dumps.wikimedia.org) processing tools.

## Features

- Extracts external URL link records from local or remote `externallinks.sql.gz` datasets
  (schema version 1.29 & above)
- More to come ...

## Quick Start

### Installation

#### Package Manager

```command
PM> Install-Package Toimik.Wikimedia
```

#### .NET CLI

```command
> dotnet add package Toimik.Wikimedia
```

### Usage

#### Extracting External URL Links

Some of the datasets are collections of URLs that point to third parties' resources.
Of particular interest are those whose filename is suffixed with `externallinks.sql.gz`
and prefixed with `<xx>wiki` where `<xx>` is the first two / three
language-specific characters.

e.g. `enwiki...externallinks.sql.gz`,
`ruwiki...externallinks.sql.gz`.

As the filename's extension implies, each link points to an SQL file that is compressed
using GZip. Specifically, each is a MySQL script to be fed to a MySQL program, which will
auto-decompress the file to create and populate a table based on the respective schema
detailed at https://www.mediawiki.org/wiki/Manual:Externallinks_table.

**However, these classes reduce disk space and memory requirements by eliminating the need to use MySQL at all._**

&nbsp;
#### V129ExternalLinksExtractor.cs

```c# 
// As the name implies, this extractor extracts from datasets meant for schema version 1.29 and above
var extractor = new V129ExternalLinksExtractor();
var path = ... // Path to a local `externallinks.sql.gz`
await foreach (ExtractionResult extractionResult in extractor.Extract(path))
{
    ...
}
```
&nbsp;  
#### ExternalLinksStreamer.cs

```c#
var streamer = new ExternalLinksStreamer(
    new HttpClient(), // Ideally, a singleton should be used
    new V129ExternalLinksExtractor());
// This example streams the external URL links from November 2021's English dataset
var dataset = new Uri("http://dumps.wikimedia.org/enwiki/20211120/enwiki-20211120-externallinks.sql.gz";
await foreach (ExtractionResult extractionResult in streamer.Stream(dataset))
{
    ...
}
```

&nbsp;  
##### Known Issue
Streaming some large files over `HTTPS` may throw a `System.IO.IOException : Received an unexpected EOF or 0 bytes from the transport stream.`

If that happens, a workaround is to use `HTTP` instead.