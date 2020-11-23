using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace SimpleDataStoreProjectInCSharp
{
    public static class Program
    {
        /// <summary>
        /// Entry point for the application.
        /// </summary>
        /// <returns></returns>
        public static async Task Main()
        {
            Console.OutputEncoding = Encoding.UTF8;
            Console.WriteLine("Init? [Y/n]");
            var input = Console.ReadLine() ?? string.Empty;
            if (input.Length == 0 || input.ToUpper() == "Y")
            {
                await Load();
            }

            await ReadBinaryData();
        }

        /// <summary>
        /// Loads austrian open data from the website, parses the data and serializes it in different formats
        /// based on an internal mapping.
        /// </summary>
        /// <returns></returns>
        private static async Task Load()
        {
            // HTTP: download file from https://www.data.gv.at/katalog/dataset/spielplatze-standorte-wien/resource/d7477bee-cfc3-45c0-96a1-5911e0ae122c
            // async disposable: https://docs.microsoft.com/en-us/dotnet/standard/garbage-collection/implementing-disposeasync
            using WebClient webClient = new WebClient();
            await using Stream stream = await webClient.OpenReadTaskAsync(
                "https://data.wien.gv.at/daten/geo?service=WFS&request=GetFeature&version=1.1.0&typeName=ogdwien:SPIELPLATZPUNKTOGD&srsName=EPSG:4326&outputFormat=csv");

            // (local testing) Stream: Read data ... see same interface as the webClient's stream
            // using Stream stream = File.OpenRead("SPIELPLATZPUNKTOGD.csv");

            // OOP: create PlaygroundPoint
            // Collections: get data
            var data = await ReadStreamAsCsv(stream);

            // File: Persist data in own csv file with the same format
            await using Stream writeStream = File.OpenWrite("custom.csv");
            await WriteCollectionAsCsv(data, writeStream);

            // LINQ
            var objectIds = data.Select(x => x.OBJECTID).Where(x => x.HasValue).Select(x => x.Value).ToList();
            Console.WriteLine("min objectid: " + objectIds.Min());
            Console.WriteLine("max objectid: " + objectIds.Max());

            // File handling: preparation for databases (index file)
            await using Stream writeStreamBinary = File.OpenWrite("custom.dat");
            await using Stream writeStreamIndexBinary = File.OpenWrite("custom.idx.dat");
            WriteCollectionAsBinary(data, writeStreamBinary, writeStreamIndexBinary);
        }

        /// <summary>
        /// Persist data in a binary file and add additional index file.
        /// </summary>
        /// <param name="data">Collection of data.</param>
        /// <param name="writeStreamBinary">Stream for the content.</param>
        /// <param name="writeStreamIndexBinary">Stream for the index file.</param>
        private static async void WriteCollectionAsBinary(IEnumerable<PlaygroundPoint> data, Stream writeStreamBinary, Stream writeStreamIndexBinary)
        {
            await using var writer = new BinaryWriter(writeStreamBinary, Encoding.UTF8);
            await using var indexWriter = new BinaryWriter(writeStreamIndexBinary, Encoding.UTF8);
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
        private static async Task<IList<PlaygroundPoint>> ReadStreamAsCsv(Stream stream)
        {
            using var reader = new StreamReader(stream, Encoding.UTF8);
            var list = new List<PlaygroundPoint>();

            // first line is the header (we already know and store for debugging purpose)
            // ReSharper disable once UnusedVariable
            var header = await reader.ReadLineAsync();

            bool isContentOver = false;
            while (!isContentOver)
            {
                var currentItem = new PlaygroundPoint();

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
                            currentItem.FID = readPart.ToString();
                            break;
                        case 1:
                            currentItem.OBJECTID = readPart.Length == 0 ? null : new int?(int.Parse(readPart.ToString()));
                            break;
                        case 2:
                            currentItem.SHAPE = readPart.ToString();
                            break;
                        case 3:
                            currentItem.ANL_NAME = readPart.ToString();
                            break;
                        case 4:
                            currentItem.BEZIRK = readPart.Length == 0 ? null : new int?(int.Parse(readPart.ToString()));
                            break;
                        case 5:
                            currentItem.SPIELPLATZ_DETAIL = readPart.ToString();
                            break;
                        case 6:
                            currentItem.TYP_DETAIL = readPart.ToString();
                            break;
                        case 7:
                            currentItem.SE_ANNO_CAD_DATA = readPart.ToString();
                            break;
                    }
                }

                if (!isContentOver)
                {
                    list.Add(currentItem);
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
        private static async Task WriteCollectionAsCsv(IEnumerable<PlaygroundPoint> data, Stream stream)
        {
            await using StreamWriter writer = new StreamWriter(stream, Encoding.UTF8);
            await writer.WriteLineAsync("FID,OBJECTID,SHAPE,ANL_NAME,BEZIRK,SPIELPLATZ_DETAIL,TYP_DETAIL,SE_ANNO_CAD_DATA");
            foreach (var item in data)
            {
                await writer.WriteLineAsync($"{Escape(item.FID)},{Format(item.OBJECTID)},{Escape(item.SHAPE)},{Escape(item.ANL_NAME)},{Format(item.BEZIRK)},{Escape(item.SPIELPLATZ_DETAIL)},{Escape(item.TYP_DETAIL)},{Escape(item.SE_ANNO_CAD_DATA)}");
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
        private static async Task ReadBinaryData()
        {
            Console.Write("Enter an object id: ");
            var searchedObjectId = int.Parse(Console.ReadLine() ?? "0");

            await using Stream readStreamBinary = File.OpenRead("custom.dat");
            await using Stream readStreamIndexBinary = File.OpenRead("custom.idx.dat");

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

                var currentItem = new PlaygroundPoint
                {
                    FID = reader.ReadString(),
                    OBJECTID = reader.ReadBoolean() ? new int?(reader.ReadInt32()) : null,
                    SHAPE = reader.ReadString(),
                    ANL_NAME = reader.ReadString(),
                    BEZIRK = reader.ReadBoolean() ? new int?(reader.ReadInt32()) : null,
                    SPIELPLATZ_DETAIL = reader.ReadString(),
                    TYP_DETAIL = reader.ReadString(),
                    SE_ANNO_CAD_DATA = reader.ReadString()
                };

                await WriteCollectionAsCsv(new[] { currentItem }, Console.OpenStandardOutput());
            }
        }
    }
}