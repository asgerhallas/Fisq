using System;
using System.Linq;
using System.Text;
using Xunit;

namespace Fisq.Tests
{
    public class FisqStoreTests : IDisposable
    {
        readonly FisqStore store;

        public FisqStoreTests() => store = new FisqStore("testdata", dropEvents: true);

        [Fact]
        public void OnlyReadCommittedOnLoad()
        {
            // make one full commit
            store.Enqueue("id", 0, new byte[0]);

            // save an event to a file, without committing
            store.DataStore.Write(new EventData("id", 1, new byte[0]));

            var events = store.Load("id");
            Assert.Equal(1, events.Count());
        }

        //[Fact]
        //public void OnlyReadCommittedOnStream()
        //{
        //    // make one full commit
        //    store.Save(Guid.NewGuid(), new[]
        //    {
        //        EventData.FromMetadata(new Metadata
        //        {
        //            {DomainEvent.MetadataKeys.SequenceNumber, 0.ToString(Metadata.NumberCulture)},
        //            {DomainEvent.MetadataKeys.AggregateRootId, "id".ToString()}
        //        }, new byte[0])
        //    });

        //    // save an event to sequence-index, without committing
        //    store.GlobalSequenceIndex.Write(new[]
        //    {
        //        new Fisq.GlobalSequenceIndex.GlobalSequenceRecord
        //        {
        //            GlobalSequenceNumber = 1,
        //            AggregateRootId = "id",
        //            LocalSequenceNumber = 1
        //        }
        //    });

        //    var events = store.Stream();
        //    Assert.AreEqual(1, events.Count());
        //}

        //[Fact]
        //public void CanRecoverAfterWritingIndex()
        //{
        //    // make one full commit
        //    store.Save(Guid.NewGuid(), new[]
        //    {
        //        EventData.FromMetadata(new Metadata
        //        {
        //            {DomainEvent.MetadataKeys.SequenceNumber, 0.ToString(Metadata.NumberCulture)},
        //            {DomainEvent.MetadataKeys.AggregateRootId, "rootid"}
        //        }, new byte[0])
        //    });

        //    // make one that fails right after index write
        //    store.GlobalSequenceIndex.Write(new[]
        //    {
        //        new Fisq.GlobalSequenceIndex.GlobalSequenceRecord
        //        {
        //            GlobalSequenceNumber = 1,
        //            AggregateRootId = "rootid",
        //            LocalSequenceNumber = 1
        //        }
        //    });

        //    // make one full commit
        //    store.Save(Guid.NewGuid(), new[]
        //    {
        //        EventData.FromMetadata(new Metadata
        //        {
        //            {DomainEvent.MetadataKeys.SequenceNumber, 1.ToString(Metadata.NumberCulture)},
        //            {DomainEvent.MetadataKeys.AggregateRootId, "rootid"}
        //        }, new byte[0])
        //    });

        //    var stream = store.Stream().ToList();
        //    Assert.AreEqual(1, stream.Last().GetGlobalSequenceNumber());
        //    Assert.AreEqual(2, stream.Count());

        //    var load = store.Load("rootid");
        //    Assert.AreEqual(2, load.Count());
        //}

        [Fact]
        public void CanRecoverAfterSavingEventData()
        {
            // make one full commit
            store.Enqueue("rootid", 0, Encoding.UTF8.GetBytes("The first one"));

            // make one that fails right after index write
            var item = new EventData("rootid", 1, Encoding.UTF8.GetBytes("The bad one"));

            store.GlobalSequenceIndex.Write(1, item);
            store.DataStore.Write(item);

            // make one full commit
            store.Enqueue("rootid", 1, Encoding.UTF8.GetBytes("The good one"));

            var stream = store.Stream().ToList();
            Assert.Equal(2, stream.Count);
            Assert.Equal("The first one", Encoding.UTF8.GetString(stream[0]));
            Assert.Equal("The good one", Encoding.UTF8.GetString(stream[1]));

            var load = store.Load("rootid").ToList();
            Assert.Equal(2, load.Count);
            Assert.Equal("The first one", Encoding.UTF8.GetString(load[0]));
            Assert.Equal("The good one", Encoding.UTF8.GetString(load[1]));
        }

        public void Dispose()
        {
            store.Dispose();
        }
    }
}