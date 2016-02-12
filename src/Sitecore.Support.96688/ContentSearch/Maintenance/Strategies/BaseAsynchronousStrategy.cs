using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Sitecore.Abstractions;
using Sitecore.ContentSearch;
using Sitecore.ContentSearch.Diagnostics;
using Sitecore.ContentSearch.Maintenance;
using Sitecore.ContentSearch.Maintenance.Strategies;
using Sitecore.ContentSearch.Utilities;
using Sitecore.Data;
using Sitecore.Data.Eventing.Remote;
using Sitecore.Diagnostics;
using Sitecore.Eventing;
using Sitecore.Globalization;
using Sitecore.Jobs;

namespace Sitecore.Support.ContentSearch.Maintenance.Strategies
{
  public abstract class BaseAsynchronousStrategy : IIndexUpdateStrategy
  {
    private volatile int initialized;

    #region Properties

    public Database Database { get; protected set; }

    public bool CheckForThreshold { get; set; }

    internal Sitecore.ContentSearch.Abstractions.ISettings Settings { get; set; }

    protected ISearchIndex Index { get; set; }

    protected IContentSearchConfigurationSettings ContentSearchSettings { get; set; }

    protected Dictionary<string, long> IndexTimestamps { get; set; }
    
    #endregion

    protected BaseAsynchronousStrategy(string database)
    {
      Assert.IsNotNullOrEmpty(database, "database");
      this.Database = ContentSearchManager.Locator.GetInstance<IFactory>().GetDatabase(database);
      Assert.IsNotNull(this.Database, string.Format("Database '{0}' was not found", database));
    }

    #region Public methods

    public virtual void Initialize(ISearchIndex searchIndex)
    {
      Assert.IsNotNull(searchIndex, "searchIndex");
      CrawlingLog.Log.Info(string.Format("[Index={0}] Initializing {1}.", searchIndex.Name, this.GetType().Name));

      if (Interlocked.CompareExchange(ref this.initialized, 1, 0) == 0)
      {
        this.Index = searchIndex;
        this.Settings = this.Index.Locator.GetInstance<Sitecore.ContentSearch.Abstractions.ISettings>();
        this.ContentSearchSettings = this.Index.Locator.GetInstance<IContentSearchConfigurationSettings>();

        if (!this.Settings.EnableEventQueues())
        {
          CrawlingLog.Log.Fatal(string.Format("[Index={0}] Initialization of {1} failed because event queue is not enabled.", searchIndex.Name, this.GetType().Name));
        }
        else
        {
          if (this.IndexTimestamps == null)
          {
            this.IndexTimestamps = new Dictionary<string, long>();
          }

          EventHub.OnIndexingStarted += this.OnIndexingStarted;
          EventHub.OnIndexingEnded += this.OnIndexingEnded;
        }
      }
    }

    public virtual void Run()
    {
      EventManager.RaiseQueuedEvents();

      var eventQueue = this.Database.RemoteEvents.Queue;

      if (eventQueue == null)
      {
        CrawlingLog.Log.Fatal("Event Queue is empty. Returning.");
        return;
      }

      var queue = this.ReadQueue(eventQueue);

      this.Run(queue, this.Index);
    }

    #endregion

    #region Protected methods

    protected void Handle()
    {
      OperationMonitor.Register(this.Run);
      OperationMonitor.Trigger();
    }

    protected virtual List<QueuedEvent> ReadQueue(EventQueue eventQueue)
    {
      return this.ReadQueue(eventQueue, this.Index.Summary.LastUpdatedTimestamp);
    }

    protected virtual List<QueuedEvent> ReadQueue(EventQueue eventQueue, long? lastUpdatedTimestamp)
    {
      var queue = new List<QueuedEvent>();

      lastUpdatedTimestamp = lastUpdatedTimestamp ?? 0;

      var query = new EventQueueQuery { FromTimestamp = lastUpdatedTimestamp };
      query.EventTypes.Add(typeof(RemovedVersionRemoteEvent));
      query.EventTypes.Add(typeof(SavedItemRemoteEvent));
      query.EventTypes.Add(typeof(DeletedItemRemoteEvent));
      query.EventTypes.Add(typeof(MovedItemRemoteEvent));

      queue.AddRange(eventQueue.GetQueuedEvents(query));

      return queue.Where(e => e.Timestamp > lastUpdatedTimestamp).ToList();
    }

    protected IEnumerable<IndexableInfo> ExtractIndexableInfoFromQueue(List<QueuedEvent> queue)
    {
      var indexableListToUpdate = new Dictionary<Data.DataUri, IndexableInfo>();
      var indexableListToRemove = new Dictionary<Data.DataUri, IndexableInfo>();

      foreach (var queuedEvent in queue)
      {
        var instanceData = this.Database.RemoteEvents.Queue.DeserializeEvent(queuedEvent) as ItemRemoteEventBase;

        if (instanceData == null)
        {
          continue;
        }

        var key = new Data.DataUri(ID.Parse(instanceData.ItemId), Language.Parse(instanceData.LanguageName), Sitecore.Data.Version.Parse(instanceData.VersionNumber));
        var itemUri = new ItemUri(key.ItemID, key.Language, key.Version, this.Database);
        var indexable = new IndexableInfo(new SitecoreItemUniqueId(itemUri), queuedEvent.Timestamp);

        if (instanceData is RemovedVersionRemoteEvent || instanceData is DeletedItemRemoteEvent)
        {
          this.HandleIndexableToRemove(indexableListToRemove, key, indexable);
        }
        else
        {
          this.UpdateIndexableInfo(instanceData, indexable);
          this.HandleIndexableToUpdate(indexableListToUpdate, key, indexable);
        }
      }

      return indexableListToUpdate.Select(x => x.Value)
        .Union(indexableListToRemove.Select(x => x.Value))
        .OrderBy(x => x.Timestamp).ToList();
    }

    protected virtual void Run(List<QueuedEvent> queue, ISearchIndex index)
    {
      CrawlingLog.Log.Debug(string.Format("[Index={0}] {1} executing.", index.Name, this.GetType().Name));

      if (this.Database == null)
      {
        CrawlingLog.Log.Fatal(string.Format("[Index={0}] OperationMonitor has invalid parameters. Index Update cancelled.", index.Name));
        return;
      }

      queue = queue.Where(q => q.Timestamp > (index.Summary.LastUpdatedTimestamp ?? 0)).ToList();

      if (queue.Count <= 0)
      {
        CrawlingLog.Log.Debug(string.Format("[Index={0}] Event Queue is empty. Incremental update returns", index.Name));
        return;
      }

      if (this.CheckForThreshold && queue.Count > this.ContentSearchSettings.FullRebuildItemCountThreshold())
      {
        CrawlingLog.Log.Warn(string.Format("[Index={0}] The number of changes exceeded maximum threshold of '{1}'.", index.Name, this.ContentSearchSettings.FullRebuildItemCountThreshold()));
        Job fullRebuildJob = IndexCustodian.FullRebuild(index);
        fullRebuildJob.Wait();
        return;
      }

      List<IndexableInfo> parsed = this.ExtractIndexableInfoFromQueue(queue).ToList();
      CrawlingLog.Log.Info(string.Format("[Index={0}] Updating '{1}' items from Event Queue.", index.Name, parsed.Count()));

      Job incrememtalUpdateJob = IndexCustodian.IncrementalUpdate(index, parsed);
      incrememtalUpdateJob.Wait();
    }

    protected virtual void OnIndexingStarted(object sender, EventArgs args)
    {
      var indexName = ContentSearchManager.Locator.GetInstance<IEvent>().ExtractParameter<string>(args, 0);
      var isFullRebuild = (bool)ContentSearchManager.Locator.GetInstance<IEvent>().ExtractParameter(args, 1);

      if (this.Index.Name == indexName && isFullRebuild)
      {
        this.IndexTimestamps[indexName] = this.Database.RemoteEvents.Queue.GetLastEvent().Timestamp;
      }
    }

    protected virtual void OnIndexingEnded(object sender, EventArgs args)
    {
      var indexName = ContentSearchManager.Locator.GetInstance<IEvent>().ExtractParameter<string>(args, 0);
      var isFullRebuild = (bool)ContentSearchManager.Locator.GetInstance<IEvent>().ExtractParameter(args, 1);

      if (this.Index.Name == indexName && isFullRebuild && this.IndexTimestamps.ContainsKey(indexName))
      {
        this.Index.Summary.LastUpdatedTimestamp = this.IndexTimestamps[indexName];
      }
    }

    #endregion

    #region Private methods

    private void UpdateIndexableInfo(ItemRemoteEventBase instanceData, IndexableInfo indexable)
    {
      if (instanceData is SavedItemRemoteEvent)
      {
        var savedEvent = instanceData as SavedItemRemoteEvent;

        if (savedEvent.IsSharedFieldChanged)
        {
          indexable.IsSharedFieldChanged = true;
        }

        if (savedEvent.IsUnversionedFieldChanged)
        {
          indexable.IsUnversionedFieldChanged = true;
        }
      }

      if (instanceData is MovedItemRemoteEvent)
      {
        indexable.IsItemMoved = true;
      }
    }

    private void HandleIndexableToRemove(Dictionary<Data.DataUri, IndexableInfo> collection, Data.DataUri key, IndexableInfo indexable)
    {
      if (collection.ContainsKey(key))
      {
        collection[key].Timestamp = indexable.Timestamp;
      }
      else
      {
        collection.Add(key, indexable);
      }
    }

    private void HandleIndexableToUpdate(Dictionary<Data.DataUri, IndexableInfo> collection, Data.DataUri key, IndexableInfo indexable)
    {
      bool alreadyAddedMovedItemEvent = collection.Any(x => x.Key.ItemID == key.ItemID && x.Value.IsItemMoved);
      bool alreadyAddedSharedFieldChange = collection.Any(x => x.Key.ItemID == key.ItemID && x.Value.IsSharedFieldChanged);
      bool alreadyAddedUnversionedFieldChange = collection.Any(x => x.Key.ItemID == key.ItemID && x.Key.Language == key.Language && x.Value.IsUnversionedFieldChanged);

      if (alreadyAddedMovedItemEvent || alreadyAddedSharedFieldChange)
      {
        var entry = collection.First(x => x.Key.ItemID == key.ItemID).Value;
        entry.Timestamp = indexable.Timestamp;
        entry.IsItemMoved = entry.IsItemMoved || indexable.IsItemMoved;
      }
      else if (indexable.IsSharedFieldChanged || indexable.IsItemMoved)
      {
        collection.Keys.Where(x => x.ItemID == key.ItemID).ToList().ForEach(x => collection.Remove(x));
        collection.Add(key, indexable);
      }
      else if (alreadyAddedUnversionedFieldChange)
      {
        collection.First(x => x.Key.ItemID == key.ItemID && x.Key.Language == key.Language).Value.Timestamp = indexable.Timestamp;
      }
      else if (indexable.IsUnversionedFieldChanged)
      {
        collection.Keys.Where(x => x.ItemID == key.ItemID && x.Language == key.Language).ToList().ForEach(x => collection.Remove(x));
        collection.Add(key, indexable);
      }
      else
      {
        if (collection.ContainsKey(key))
        {
          collection[key].Timestamp = indexable.Timestamp;
        }
        else
        {
          collection.Add(key, indexable);
        }
      }
    }

    #endregion
  }
}