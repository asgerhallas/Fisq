namespace Fisq
{
    public class EventData
    {
        public EventData(string streamId, long seq, byte[] data)
        {
            StreamId = streamId;
            Seq = seq;
            Data = data;
        }

        public string StreamId { get; }
        public long Seq { get; }
        public byte[] Data { get; }
    }
}