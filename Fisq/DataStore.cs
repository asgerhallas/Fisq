using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;

namespace Fisq
{
    /// <summary>
    /// Stores events as individual files utilizing the file system to do indexed lookup.
    /// Enforces AggregateId/SequenceNumber uniqueness.
    /// Reading and writing can be done concurrently.
    /// </summary>
    internal class DataStore
    {
        readonly string _dataDirectory;

        public DataStore(string basePath, bool dropEvents)
        {
            _dataDirectory = Path.Combine(basePath, "events");

            if (dropEvents && Directory.Exists(_dataDirectory))
                new DirectoryInfo(_dataDirectory).Delete(true);

            Directory.CreateDirectory(_dataDirectory);
        }

        public void Write(EventData @event)
        {
            try
            {
                var aggregateRootId = @event.StreamId;
                var sequenceNumber = @event.Seq;

                var aggregateDirectory = Path.Combine(_dataDirectory, aggregateRootId);
                Directory.CreateDirectory(aggregateDirectory);

                var filename = Path.Combine(aggregateDirectory, GetFilename(sequenceNumber));

                using (var file = new FileStream(filename, FileMode.CreateNew, FileAccess.Write, FileShare.None, 1024, FileOptions.None))
                using (var writer = new BinaryWriter(file))
                {
                    writer.Write(@event.Seq);
                    writer.Write(@event.Data);

                    writer.Flush();
                }
            }
            catch (IOException ex)
            {
                throw new ConcurrencyException("Data could not be written. Please retry.", ex);
            }
        }

        public IEnumerable<byte[]> Read(long lastCommittedGlobalSequenceNumber, string aggregateRootId, long offset)
        {
            var aggregateDirectory = Path.Combine(_dataDirectory, aggregateRootId);

            if (!Directory.Exists(aggregateDirectory))
            {
                return Enumerable.Empty<byte[]>();
            }

            return from path in Directory.EnumerateFiles(aggregateDirectory)
                   let seq = long.Parse(Path.GetFileName(path))
                   where seq >= offset
                   let item = TryRead(path)
                   where item.seq != -1 && item.seq <= lastCommittedGlobalSequenceNumber
                   select item.data;

        }

        public byte[] Read(string aggregateRootId, long sequenceNumber)
        {
            var filename = Path.Combine(_dataDirectory, aggregateRootId, GetFilename(sequenceNumber));
            
            var item = TryRead(filename);
            
            if (item.seq == -1)
            {
                throw new InvalidOperationException("The event you tried to read did not exist on disk or was corrupted.");
            }

            return item.data;
        }

        public void Truncate(string aggregateRootId, long sequenceNumber)
        {
            var filename = Path.Combine(_dataDirectory, aggregateRootId, GetFilename(sequenceNumber));
            if (File.Exists(filename))
                File.Delete(filename);
        }

        (long seq, byte[] data) TryRead(string filename)
        {
            if (!File.Exists(filename))
                return (-1, null);

            using (var file = new FileStream(filename, FileMode.Open))
            using (var reader = new BinaryReader(file))
            {
                if (file.Length == 0)
                    return (-1, null);

                var seq = reader.ReadInt64();
                var data = reader.ReadBytes((int)file.Length - sizeof(long)); // this will fail on very large msgs

                return (seq, data);
            }
        }

        static string GetFilename(long sequenceNumber) => sequenceNumber.ToString(CultureInfo.InvariantCulture).PadLeft(20, '0');
    }
}