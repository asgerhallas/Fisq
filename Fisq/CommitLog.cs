using System;
using System.IO;
using System.Text;

namespace Fisq
{
    /// <summary>
    /// Writes last global sequence number of each batch to a log with a checksum. A full record indicates a successful commit of a full batch.
    /// Reading is thread safe and can be done concurrently with writes. Writes/Recovers must be sequential.
    /// </summary>
    internal class CommitLog : IDisposable
    {
        public const int SizeofHalfRecord = sizeof(long);
        public const int SizeofFullRecord = SizeofHalfRecord * 2;

        readonly string _commitsFilePath;
        
        BinaryWriter writer;
        BinaryReader reader;

        public CommitLog(string basePath, bool dropEvents)
        {
            _commitsFilePath = Path.Combine(basePath, "commits.idx");

            if (dropEvents && File.Exists(_commitsFilePath)) 
                File.Delete(_commitsFilePath);

            OpenWriter();
            OpenReader();
        }

        public BinaryWriter Writer => writer;

        public void Write(long globalSequenceNumber)
        {
            Writer.Write(globalSequenceNumber);
            Writer.Write(globalSequenceNumber); // "checksum"
            Writer.Flush();
        }

        public long Read() => Read(out long isCorrupted);

        public long Read(out bool isCorrupted)
        {
            var read = Read(out long garbage);
            isCorrupted = garbage > 0;
            return read;
        }

        long Read(out long garbage)
        {
            garbage = 0;

            var currentLength = reader.BaseStream.Length;

            if (currentLength == 0) return -1;

            reader.BaseStream.Seek(currentLength, SeekOrigin.Begin);

            garbage = currentLength % SizeofHalfRecord;
            if (garbage > 0)
            {
                // we have a failed commit on our hands, skip the garbage
                reader.BaseStream.Seek(-garbage, SeekOrigin.Current);
            }

            if (currentLength < garbage + SizeofFullRecord)
            {
                // there's no full commit, it's all garbage
                garbage = currentLength;
                return -1;
            }

            // read commit and checksum
            reader.BaseStream.Seek(-SizeofFullRecord, SeekOrigin.Current);
            var globalSequenceNumber = reader.ReadInt64();
            var checksum = reader.ReadInt64();

            if (globalSequenceNumber == checksum)
                return globalSequenceNumber;

            // checksum was wrong, mark as garbage
            garbage = garbage + SizeofHalfRecord;

            if (currentLength < garbage + SizeofFullRecord)
            {
                // there's no commit before, this it's all garbage
                garbage = currentLength;
                return -1;
            }

            // try skip the orphaned commit and try again
            reader.BaseStream.Seek(-(SizeofHalfRecord + SizeofFullRecord), SeekOrigin.Current);
            globalSequenceNumber = reader.ReadInt64();
            checksum = reader.ReadInt64();

            if (globalSequenceNumber == checksum)
                return globalSequenceNumber;

            throw new InvalidOperationException("Commit file is unreadable.");
        }

        public void Recover()
        {
            writer.Dispose();

            Read(out long garbage);

            if (garbage == 0)
            {
                throw new InvalidOperationException(
                    "Recover must not be called if there's is no garbage. Please check that before the call.");
            }

            using (var stream = new FileStream(_commitsFilePath, FileMode.Open, FileAccess.Write, FileShare.Read, 1024, FileOptions.None))
            {
                stream.SetLength(stream.Length - garbage);
                stream.Flush();
            }

            OpenWriter();
        }

        void OpenWriter()
        {
            writer = new BinaryWriter(
                new FileStream(_commitsFilePath, FileMode.Append, FileAccess.Write, FileShare.Read, 1024, FileOptions.None),
                Encoding.ASCII, leaveOpen: false);
        }

        void OpenReader()
        {
            reader = new BinaryReader(
                new FileStream(_commitsFilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, 1024, FileOptions.None),
                Encoding.ASCII, leaveOpen: false);
        }

        public void Dispose()
        {
            writer.Dispose();
            reader.Dispose();
        }
    }
}