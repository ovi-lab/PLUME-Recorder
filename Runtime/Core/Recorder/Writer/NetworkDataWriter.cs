using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using K4os.Compression.LZ4;
using K4os.Compression.LZ4.Streams;
using UnityEngine;

namespace PLUME.Core.Recorder.Writer
{
    public class NetworkDataWriter : IDataWriter
    {
        private Stream _stream;
        private string ipAddress;
        private int port;

        public NetworkDataWriter(string ipAddress="127.0.0.1", int port=8000)
        {
            this.ipAddress = ipAddress;
            this.port = port;
        }

        public void Initialize(Record _)
        {
            // Create a tcp server
            var server = new TcpListener(IPAddress.Parse(ipAddress), port);
            server.Start();
            
            var stream = server.AcceptTcpClient().GetStream();
            
            while(!stream.CanWrite)
            {
                Debug.Log("Waiting for client to connect...");
                Thread.Sleep(100);
            }
            
            _stream = LZ4Stream.Encode(stream, LZ4Level.L00_FAST);
        }

        public void WriteTimelessData(DataChunks dataChunks)
        {
        }

        public void WriteTimestampedData(DataChunksTimestamped dataChunks)
        {
            var data = dataChunks.GetDataSpan();
            _stream.Write(data);
            _stream.Flush();
        }

        public void Flush()
        {
            _stream.Flush();
        }

        public void Close()
        {
            _stream.Close();
        }
    }
}
