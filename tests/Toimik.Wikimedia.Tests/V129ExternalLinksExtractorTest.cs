namespace Toimik.Wikimedia.Tests
{
    using Toimik.Wikimedia;

    public class V129ExternalLinksExtractorTest : ExternalLinksExtractorTest
    {
        protected override ExternalLinksExtractor CreateExtractor(DecompressStreamFactory decompressStreamFactory = null)
        {
            return new V129ExternalLinksExtractor(decompressStreamFactory);
        }
    }
}