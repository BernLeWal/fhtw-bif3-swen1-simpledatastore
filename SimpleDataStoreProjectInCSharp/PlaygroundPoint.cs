// ReSharper disable InconsistentNaming
namespace SimpleDataStoreProjectInCSharp
{
    // consider to use as record?
    // with some changes in the csv reader we can make this immutable easily
    // https://www.thomasclaudiushuber.com/2020/09/01/c-9-0-records-work-with-immutable-data-classes/
    // https://daveabrock.com/2020/07/06/c-sharp-9-deep-dive-records

    public class PlaygroundPoint
    {
        public string FID { get; set; }
        public int? OBJECTID { get; set; }
        public string SHAPE { get; set; }
        public string ANL_NAME { get; set; }
        public int? BEZIRK { get; set; }
        public string SPIELPLATZ_DETAIL { get; set; }
        public string TYP_DETAIL { get; set; }
        public string SE_ANNO_CAD_DATA { get; set; }
    }
}
