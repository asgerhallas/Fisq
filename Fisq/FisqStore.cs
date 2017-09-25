using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("Fisq.Tests")]

namespace Fisq
{
    /// <summary>
    /// Event store that saves events directly to files for use in embedded scenarios. 
    /// Only supports a single process using the store at a time. 
    /// Uses filesystem's index for easy lookup of single aggregate streams.
    /// Uses a single file for global sequence index.
    /// Uses a single file for commit log.
    /// </summary>
    public class FisqStore : IDisposable
    {
        readonly object _lock = new object();

        public FisqStore(string basePath) : this(basePath, dropEvents: false) {}

        internal FisqStore(string basePath, bool dropEvents)
        {
            Directory.CreateDirectory(basePath);

            GlobalSequenceIndex = new GlobalSequenceIndex(basePath, dropEvents);
            DataStore = new DataStore(basePath, dropEvents);
            CommitLog = new CommitLog(basePath, dropEvents);
        }

        internal GlobalSequenceIndex GlobalSequenceIndex { get;  }
        internal DataStore DataStore { get; }
        internal CommitLog CommitLog { get; }

        public void Enqueue(string streamId, long seq, byte[] data)
        {
            lock (_lock)
            {
                var commitId = CommitLog.Read(out var isCorrupted);

                if (isCorrupted) CommitLog.Recover();

                GlobalSequenceIndex.DetectCorruptionAndRecover(DataStore, commitId);

                var nextCommitId = commitId + 1;
                var @event = new EventData(streamId, seq, data);

                GlobalSequenceIndex.Write(nextCommitId, @event);
                DataStore.Write(@event);
                CommitLog.Write(nextCommitId);
            }
        }

        public IEnumerable<byte[]> Load(string aggregateRootId, long firstSeq = 0)
        {
            var lastCommitId = CommitLog.Read();

            return DataStore.Read(lastCommitId, aggregateRootId, firstSeq);
        }

        public IEnumerable<byte[]> Stream(long globalSequenceNumber = 0)
        {
            var lastCommitId = CommitLog.Read();

            return from record in GlobalSequenceIndex.Read(lastCommitId, offset: globalSequenceNumber)
                   select DataStore.Read(record.AggregateRootId, record.LocalSequenceNumber);
        }

        public long GetNextGlobalSequenceNumber()
        {
            return CommitLog.Read() + 1;
        }
        
        public void Dispose()
        {
            GlobalSequenceIndex.Dispose();
            CommitLog.Dispose();
        }
    }
}
