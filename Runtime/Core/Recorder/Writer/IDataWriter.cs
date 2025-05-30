namespace PLUME.Core.Recorder.Writer
{
    public interface IDataWriter
    {
        public void Initialize(Record record);

        public void WriteTimelessData(DataChunks dataChunks);

        public void WriteTimestampedData(DataChunksTimestamped dataChunks);

        void Flush();

        public void Close();
    }
}
