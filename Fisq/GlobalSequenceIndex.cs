using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace Fisq
{
    /// <summary>
    /// Writes a reference to each event with it's global sequence number and link to event file.
    /// Reading can be done concurrently. Writes/Recovers must be sequential.
    /// </summary>
    internal class GlobalSequenceIndex : IDisposable
    {
        public const int SizeofSeqRecord = 8 + 1 + 255 + 8;

        readonly string _seqFilePath;

        BinaryWriter _writer;

        public GlobalSequenceIndex(string basePath, bool dropEvents)
        {
            _seqFilePath = Path.Combine(basePath, "seq.idx");

            if (dropEvents && File.Exists(_seqFilePath))
                File.Delete(_seqFilePath);

            OpenWriter();
        }

        public void Write(long globalSeq, EventData @event)
        {
            Write(new GlobalSequenceRecord
            {
                GlobalSequenceNumber = globalSeq,
                AggregateRootId = @event.StreamId,
                LocalSequenceNumber = @event.Seq,
            });
        }

        public void Write(GlobalSequenceRecord record)
        {
            _writer.Write(record.GlobalSequenceNumber);

            var keyAsBytes = Encoding.UTF8.GetBytes(record.AggregateRootId);
            var length = keyAsBytes.Length;

            Array.Resize(ref keyAsBytes, 255);

            _writer.Write((byte)length);
            _writer.Write(keyAsBytes);
            _writer.Write(record.LocalSequenceNumber);

            _writer.Flush();
        }

        public IEnumerable<GlobalSequenceRecord> Read(long lastCommittedGlobalSequenceNumber, long offset)
        {
            using (var readStream = new FileStream(_seqFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, 100 * SizeofSeqRecord, FileOptions.None))
            using (var reader = new BinaryReader(readStream, Encoding.ASCII))
            {
                readStream.Seek(offset * SizeofSeqRecord, SeekOrigin.Begin);

                while (readStream.Position + SizeofSeqRecord <= readStream.Length)
                {
                    var record = new GlobalSequenceRecord
                    {
                        GlobalSequenceNumber = reader.ReadInt64(),
                        AggregateRootId = ReadFixedLengthString(reader),
                        LocalSequenceNumber = reader.ReadInt64(),
                    };

                    if (record.GlobalSequenceNumber > lastCommittedGlobalSequenceNumber)
                        break;

                    yield return record;
                }
            }
        }

        public string ReadFixedLengthString(BinaryReader reader)
        {
            var length = reader.ReadByte();
            var str = Encoding.UTF8.GetString(reader.ReadBytes(length));
            reader.ReadBytes(255 - length);
            return str;
        }

        public void DetectCorruptionAndRecover(DataStore store, long lastCommittedGlobalSequenceNumber)
        {
            var numberOfRecords = _writer.BaseStream.Length / SizeofSeqRecord;
            var expectedNumberOfRecords = lastCommittedGlobalSequenceNumber + 1;

            if (expectedNumberOfRecords == numberOfRecords)
                return;

            _writer.Dispose();

            foreach (var orphan in Read(long.MaxValue, lastCommittedGlobalSequenceNumber + 1))
            {
                store.Truncate(orphan.AggregateRootId, orphan.LocalSequenceNumber);
            }

            using (var stream = new FileStream(_seqFilePath, FileMode.Open, FileAccess.Write, FileShare.Read, 1024, FileOptions.None))
            {
                stream.SetLength(expectedNumberOfRecords * SizeofSeqRecord);
                stream.Flush();
            }

            OpenWriter();
        }

        void OpenWriter()
        {
            _writer = new BinaryWriter(
                new FileStream(_seqFilePath, FileMode.Append, FileAccess.Write, FileShare.Read, 100 * SizeofSeqRecord, FileOptions.None),
                Encoding.ASCII, leaveOpen: false);
        }

        public void Dispose()
        {
            _writer.Dispose();
        }

        public class GlobalSequenceRecord
        {
            public long GlobalSequenceNumber { get; set; }
            public string AggregateRootId { get; set; }
            public long LocalSequenceNumber { get; set; }
        }
    }
}