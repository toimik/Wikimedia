namespace Toimik.Wikimedia.Tests
{
    using System.IO;

    // NOTE: As the test data is in a file that is not GZip compressed, this class overrides the
    // need to decompress it
    public class DummyDecompressStreamFactory : DecompressStreamFactory
    {
        public override Stream CreateDecompressStream(Stream stream)
        {
            return stream;
        }
    }
}