using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;
using Newtonsoft.Json;
using Npgsql;
using Npgsql.Replication.PgOutput.Messages;
using NpgsqlTypes;

namespace SimpleDataStoreProjectInCSharp
{
    // https://www.thomasclaudiushuber.com/2020/09/01/c-9-0-records-work-with-immutable-data-classes/
    // https://daveabrock.com/2020/07/06/c-sharp-9-deep-dive-records
    public record PlaygroundPoint(
        string FID,
        int? OBJECTID,
        string SHAPE,
        string ANL_NAME,
        int? BEZIRK,
        string SPIELPLATZ_DETAIL,
        string TYP_DETAIL,
        string SE_ANNO_CAD_DATA);

    public class PlaygroundPointClass
    {
        public string FID { get; set; }
        public int? OBJECTID { get; set; }
        public string SHAPE { get; set; }
        public string ANL_NAME { get; set; }
        public int? BEZIRK { get; set; }
        public string SPIELPLATZ_DETAIL { get; set; }
        public string TYP_DETAIL { get; set; }
        public string SE_ANNO_CAD_DATA { get; set; }

        public PlaygroundPointClass From(PlaygroundPoint point)
        {
            this.FID = point.FID;
            this.OBJECTID = point.OBJECTID;
            this.SHAPE = point.SHAPE;
            this.ANL_NAME = point.ANL_NAME;
            this.BEZIRK = point.BEZIRK;
            this.SPIELPLATZ_DETAIL = point.SPIELPLATZ_DETAIL;
            this.TYP_DETAIL = point.TYP_DETAIL;
            this.SE_ANNO_CAD_DATA = point.SE_ANNO_CAD_DATA;
            return this;
        }
    }

    public static class Program
    {
        /// <summary>
        /// Entry point for the application.
        /// </summary>
        /// <returns></returns>
        public static void Main()
        {
            Console.OutputEncoding = Encoding.UTF8;
            Console.WriteLine("Init? [Y/n]");
            var input = Console.ReadLine() ?? string.Empty;
            if (input.Length == 0 || input.ToUpper() == "Y")
            {
                Load();
            }

            Console.Write("Enter an object id: ");
            var searchedObjectId = int.Parse(Console.ReadLine() ?? "0");

            ReadBinaryData(searchedObjectId);
            ReadDataFromDB(searchedObjectId);
        }

        /// <summary>
        /// Loads austrian open data from the website, parses the data and serializes it in different formats
        /// based on an internal mapping.
        /// </summary>
        /// <returns></returns>
        private static void Load()
        {
            // HTTP: download file from https://www.data.gv.at/katalog/dataset/spielplatze-standorte-wien/resource/d7477bee-cfc3-45c0-96a1-5911e0ae122c
            using WebClient webClient = new WebClient();
            using Stream stream = webClient.OpenRead(
                "https://data.wien.gv.at/daten/geo?service=WFS&request=GetFeature&version=1.1.0&typeName=ogdwien:SPIELPLATZPUNKTOGD&srsName=EPSG:4326&outputFormat=csv");

            // (local testing) Stream: Read data ... see same interface as the webClient's stream
            // using Stream stream = File.OpenRead("SPIELPLATZPUNKTOGD.csv");

            // OOP: create PlaygroundPoint
            // Collections: get data
            var data = ReadStreamAsCsv(stream);

            // File: Persist data in own csv file with the same format
            using Stream writeStream = File.OpenWrite("custom.csv");
            WriteCollectionAsCsv(data, writeStream);

            // LINQ calculation of min/max
            var objectIds = data.Select(x => x.OBJECTID).Where(x => x.HasValue).Select(x => x.Value).ToList();
            Console.WriteLine("min objectid: " + objectIds.Min());
            Console.WriteLine("max objectid: " + objectIds.Max());

            // File handling: preparation for database-concept (index file)
            using Stream writeStreamBinary = File.OpenWrite("custom.dat");
            using Stream writeStreamIndexBinary = File.OpenWrite("custom.idx.dat");
            WriteCollectionAsBinary(data, writeStreamBinary, writeStreamIndexBinary);

            // Serialize to xml
            using Stream writeStreamXml = File.OpenWrite("custom.xml");
            WriteCollectionAsXml(data, writeStreamXml);

            // Serialize to json
            using Stream writeStreamJson = File.OpenWrite("custom.json");
            WriteCollectionAsJson(data, writeStreamJson);

            // write to database
            WriteCollectionToDB(data);
        }

        private static void WriteCollectionToDB(IList<PlaygroundPoint> data)
        {
            IDbConnection connection = new NpgsqlConnection("Host=localhost;Username=swe1user;Password=swe1pw;Database=simpledatastore");
            connection.Open();
            {
                IDbCommand command = connection.CreateCommand();
                command.CommandText = "delete from playgroundpoints";
                command.ExecuteNonQuery();
            }
            {
                IDbCommand command = connection.CreateCommand();
                command.CommandText = @"
insert into playgroundpoints 
    (fid, objectid, shape, anlname, 
     bezirk, spielplatzdetail, typdetail, seannocaddata) 
values
    (@fid, @objectid, @shape, @anlname, 
     @bezirk, @spielplatzdetail, @typdetail, @seannocaddata)
";
                
                var pFID = command.CreateParameter();
                pFID.DbType = DbType.String;
                pFID.ParameterName = "fid";
                pFID.Size = 50;
                command.Parameters.Add(pFID);

                NpgsqlCommand c = command as NpgsqlCommand;
                var pOBJECTID = c.CreateParameter();
                pOBJECTID.DbType = DbType.Int32;
                pOBJECTID.ParameterName = "objectid";
                c.Parameters.Add(pOBJECTID);

                c.Parameters.Add("shape", NpgsqlDbType.Varchar, 50);
                c.Parameters.Add("anlname", NpgsqlDbType.Varchar, 50);

                c.Parameters.Add("bezirk", NpgsqlDbType.Integer);
                c.Parameters.Add("spielplatzdetail", NpgsqlDbType.Varchar, 50);
                c.Parameters.Add("typdetail", NpgsqlDbType.Varchar, 50);
                c.Parameters.Add("seannocaddata", NpgsqlDbType.Varchar, 50);

                c.Prepare();

                foreach (PlaygroundPoint item in data)
                {
                    //command.Parameters["fid"].Value = item.FID;
                    c.Parameters["fid"].Value = item.FID;
                    c.Parameters["objectid"].Value = item.OBJECTID;
                    c.Parameters["shape"].Value = item.SHAPE;
                    c.Parameters["anlname"].Value = item.ANL_NAME;
                    // when only executed once,
                    // then the parameter setup and filling can be done within one line:
                    //
                    // c.Parameters.AddWithValue("anlname", data[0].ANL_NAME);


                    c.Parameters["bezirk"].Value = item.BEZIRK ?? 0;
                    c.Parameters["spielplatzdetail"].Value = item.SPIELPLATZ_DETAIL;
                    c.Parameters["typdetail"].Value = item.TYP_DETAIL;
                    c.Parameters["seannocaddata"].Value = item.SE_ANNO_CAD_DATA;
                    
                    command.ExecuteNonQuery();
                }
            }

        }

        // https://www.sqlines.com/postgresql/npgsql_cs_result_sets 
        private static void ReadDataFromDB(int searchedObjectId)
        {
            IDbConnection connection = new NpgsqlConnection("Host=localhost;Username=swe1user;Password=swe1pw;Database=simpledatastore");
            connection.Open();
            {
                IDbCommand command = connection.CreateCommand();
                command.CommandText = @"
SELECT fid, objectid, shape, anlname, 
     bezirk, spielplatzdetail, typdetail, seannocaddata
FROM playgroundpoints 
WHERE objectid = @objectid
";
                var pOBJECTID = command.CreateParameter();
                pOBJECTID.DbType = DbType.Int32;
                pOBJECTID.ParameterName = "objectid";
                pOBJECTID.Value = searchedObjectId;
                command.Parameters.Add(pOBJECTID);

                var reader = command.ExecuteReader();
                while (reader.Read())
                {
                    PlaygroundPoint pgp = new PlaygroundPoint(
                        reader.GetString(0),
                        reader.GetInt32(1),
                        reader.GetString(2),
                        reader.GetString(3),
                        reader.GetInt32(4),
                        reader.GetString(5),
                        reader.GetString(6),
                        reader.GetString(7)
                        );
                    Console.WriteLine(pgp);
                }
                reader.Close();
            }
        }

        private static void WriteCollectionAsJson(IList<PlaygroundPoint> data, Stream stream)
        {
            using StreamWriter writer = new StreamWriter(stream);
            writer.Write(JsonConvert.SerializeObject(data));
        }

        private static void WriteCollectionAsXml(IList<PlaygroundPoint> data, Stream stream)
        {
            // does not work, because of missing parameterless constructor
            //XmlSerializer xmlSerializer = new XmlSerializer(typeof(PlaygroundPoint));
            //xmlSerializer.Serialize(writeStreamXml, data);

            XmlSerializer xmlSerializer = new XmlSerializer(typeof(List<PlaygroundPointClass>));
            xmlSerializer.Serialize(
                stream,
                data.Select(x => new PlaygroundPointClass().From(x)).ToList());
        }

        /// <summary>
        /// Persist data in a binary file and add additional index file.
        /// </summary>
        /// <param name="data">Collection of data.</param>
        /// <param name="writeStreamBinary">Stream for the content.</param>
        /// <param name="writeStreamIndexBinary">Stream for the index file.</param>
        private static void WriteCollectionAsBinary(IEnumerable<PlaygroundPoint> data, Stream writeStreamBinary, Stream writeStreamIndexBinary)
        {
            using var writer = new BinaryWriter(writeStreamBinary, Encoding.UTF8);
            using var indexWriter = new BinaryWriter(writeStreamIndexBinary, Encoding.UTF8);
            foreach (var item in data)
            {

                if (item.OBJECTID.HasValue)
                {
                    indexWriter.Write(writer.BaseStream.Position); // long
                    indexWriter.Write(item.OBJECTID.Value); // int
                }

                writer.Write(item.FID);
                writer.Write(item.OBJECTID.HasValue);
                writer.Write(item.OBJECTID ?? 0);
                writer.Write(item.SHAPE);
                writer.Write(item.ANL_NAME);
                writer.Write(item.BEZIRK.HasValue);
                writer.Write(item.BEZIRK ?? 0);
                writer.Write(item.SPIELPLATZ_DETAIL);
                writer.Write(item.TYP_DETAIL);
                writer.Write(item.SE_ANNO_CAD_DATA);
            }
        }

        /// <summary>
        /// Reads the data from the stream in csv format and maps it to a list of objects (ORMapper).
        /// Consider returning an IAsyncEnumerable https://anthonychu.ca/post/async-streams-dotnet-core-3-iasyncenumerable/ .
        /// </summary>
        /// <param name="stream">Input stream to read from.</param>
        /// <returns></returns>
        private static IList<PlaygroundPoint> ReadStreamAsCsv(Stream stream)
        {
            using var reader = new StreamReader(stream, Encoding.UTF8);
            var list = new List<PlaygroundPoint>();

            // first line is the header (we already know and store for debugging purpose)
            // ReSharper disable once UnusedVariable
            var header = reader.ReadLineAsync();

            bool isContentOver = false;
            while (!isContentOver)
            {
                string currentItemFID = null;
                int? currentItemOBJECTID = null;
                string currentItemSHAPE = null;
                string currentItemANL_NAME = null;
                int? currentItemBEZIRK = null;
                string currentItemSPIELPLATZ_DETAIL = null;
                string currentItemTYP_DETAIL = null;
                string currentItemSE_ANNO_CAD_DATA = null;

                for (int i = 0; i < 8; i++)
                {
                    StringBuilder readPart = new StringBuilder();
                    var isPartOver = false;
                    while (!isPartOver)
                    {
                        var character = reader.Read(); // no Async equivalent with no parameters
                        if (character == ',')
                        {
                            isPartOver = true;
                        }
                        else if (character == '\r' || character == '\n')
                        {
                            if (reader.Peek() == '\n')
                            {
                                reader.Read();
                            }

                            isPartOver = true;
                        }
                        else if (character == '\"')
                        {
                            do
                            {
                                character = reader.Read();
                                if (character == -1)
                                {
                                    isPartOver = true;
                                    isContentOver = true;
                                    break;
                                }
                                else if (character == '\"')
                                {
                                    break;
                                }
                                else
                                {
                                    readPart.Append((char)character); // because character is of type int
                                }
                            } while (character != '\"');
                        }
                        else if (character == -1)
                        {
                            isPartOver = true;
                            isContentOver = true;
                        }
                        else
                        {
                            readPart.Append((char)character);
                        }
                    }

                    if (isContentOver)
                    {
                        // last line is not taken over
                        break;
                    }

                    switch (i)
                    {
                        case 0:
                            currentItemFID = readPart.ToString();
                            break;
                        case 1:
                            currentItemOBJECTID = readPart.Length == 0 ? null : new int?(int.Parse(readPart.ToString()));
                            break;
                        case 2:
                            currentItemSHAPE = readPart.ToString();
                            break;
                        case 3:
                            currentItemANL_NAME = readPart.ToString();
                            break;
                        case 4:
                            currentItemBEZIRK = readPart.Length == 0 ? null : new int?(int.Parse(readPart.ToString()));
                            break;
                        case 5:
                            currentItemSPIELPLATZ_DETAIL = readPart.ToString();
                            break;
                        case 6:
                            currentItemTYP_DETAIL = readPart.ToString();
                            break;
                        case 7:
                            currentItemSE_ANNO_CAD_DATA = readPart.ToString();
                            break;
                    }
                }

                if (!isContentOver)
                {
                    list.Add(new PlaygroundPoint(currentItemFID,
                                                    currentItemOBJECTID,
                                                    currentItemSHAPE,
                                                    currentItemANL_NAME,
                                                    currentItemBEZIRK,
                                                    currentItemSPIELPLATZ_DETAIL,
                                                    currentItemTYP_DETAIL,
                                                    currentItemSE_ANNO_CAD_DATA));
                }
            }

            return list;
        }

        /// <summary>
        /// Writes the data in CSV format to a stream.
        /// </summary>
        /// <param name="data"></param>
        /// <param name="stream"></param>
        /// <returns></returns>
        private static void WriteCollectionAsCsv(IEnumerable<PlaygroundPoint> data, Stream stream)
        {
            using StreamWriter writer = new StreamWriter(stream, Encoding.UTF8);
            writer.WriteLine("FID,OBJECTID,SHAPE,ANL_NAME,BEZIRK,SPIELPLATZ_DETAIL,TYP_DETAIL,SE_ANNO_CAD_DATA");
            foreach (var item in data)
            {
                writer.WriteLine($"{Escape(item.FID)},{Format(item.OBJECTID)},{Escape(item.SHAPE)},{Escape(item.ANL_NAME)},{Format(item.BEZIRK)},{Escape(item.SPIELPLATZ_DETAIL)},{Escape(item.TYP_DETAIL)},{Escape(item.SE_ANNO_CAD_DATA)}");
            }

            // local function https://docs.microsoft.com/en-us/dotnet/csharp/programming-guide/classes-and-structs/local-functions
            string Escape(string content)
            {
                if (content == null)
                {
                    return "";
                }

                if (content.Contains(','))
                {
                    return $"\"{content}\"";
                }

                return content;
            }

            string Format(int? content)
            {
                if (!content.HasValue)
                {
                    return "";
                }

                return $"{content}";
            }
        }

        /// <summary>
        /// Read binary data based on index file.
        /// </summary>
        /// <returns></returns>
        private static void ReadBinaryData(int searchedObjectId)
        {
            using Stream readStreamBinary = File.OpenRead("custom.dat");
            using Stream readStreamIndexBinary = File.OpenRead("custom.idx.dat");

            using var indexReader = new BinaryReader(readStreamIndexBinary, Encoding.UTF8);
            using var reader = new BinaryReader(readStreamBinary, Encoding.UTF8);

            while (indexReader.BaseStream.Position < indexReader.BaseStream.Length)
            {
                var position = indexReader.ReadInt64();
                var objectId = indexReader.ReadInt32();

                if (objectId != searchedObjectId)
                {
                    continue;
                }

                Console.WriteLine($"found at position: {position}");
                reader.BaseStream.Position = position;

                var currentItem = new PlaygroundPoint(
                    FID: reader.ReadString(),
                    OBJECTID: reader.ReadBoolean() ? new int?(reader.ReadInt32()) : null,
                    SHAPE: reader.ReadString(),
                    ANL_NAME: reader.ReadString(),
                    BEZIRK: reader.ReadBoolean() ? new int?(reader.ReadInt32()) : null,
                    SPIELPLATZ_DETAIL: reader.ReadString(),
                    TYP_DETAIL: reader.ReadString(),
                    SE_ANNO_CAD_DATA: reader.ReadString()
                );

                WriteCollectionAsCsv(new[] { currentItem }, Console.OpenStandardOutput());
            }
        }
    }
}