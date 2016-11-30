namespace Sitecore.Support.ContentSearch.Maintenance.Strategies
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using System.Runtime.Serialization;
    using Sitecore.ContentSearch;
    using Sitecore.ContentSearch.Diagnostics;
    using Sitecore.ContentSearch.Maintenance;
    using Sitecore.Data;
    using Sitecore.Data.Eventing.Remote;
    using Sitecore.Eventing;
    using Sitecore.Globalization;
    using Sitecore.Jobs;

    [DataContract]
    public class OnPublishEndAsynchronousStrategy : Sitecore.ContentSearch.Maintenance.Strategies.OnPublishEndAsynchronousStrategy
    {
        private static readonly MethodInfo HandleIndexableToRemoveMethodInfo;
        private static readonly MethodInfo UpdateIndexableInfoMethodInfo;
        private static readonly MethodInfo HandleIndexableToAddVersionMethodInfo;
        private static readonly MethodInfo HandleIndexableToUpdateMethodInfo;
        static OnPublishEndAsynchronousStrategy()
        {
            HandleIndexableToRemoveMethodInfo = typeof(Sitecore.ContentSearch.Maintenance.Strategies.BaseAsynchronousStrategy).GetMethod("HandleIndexableToRemove", BindingFlags.Instance | BindingFlags.NonPublic);
            UpdateIndexableInfoMethodInfo = typeof(Sitecore.ContentSearch.Maintenance.Strategies.BaseAsynchronousStrategy).GetMethod("UpdateIndexableInfo", BindingFlags.Instance | BindingFlags.NonPublic);
            HandleIndexableToAddVersionMethodInfo = typeof(Sitecore.ContentSearch.Maintenance.Strategies.BaseAsynchronousStrategy).GetMethod("HandleIndexableToAddVersion", BindingFlags.Instance | BindingFlags.NonPublic);
            HandleIndexableToUpdateMethodInfo = typeof(Sitecore.ContentSearch.Maintenance.Strategies.BaseAsynchronousStrategy).GetMethod("HandleIndexableToUpdate", BindingFlags.Instance | BindingFlags.NonPublic);
        }


        /// <summary>
        /// Extracts instances of <see cref="IndexableInfo"/> from queue.
        /// </summary>
        /// <param name="queue">The event queue.</param>
        /// <returns><see cref="IEnumerable{T}"/></returns>
        protected new IEnumerable<IndexableInfo> ExtractIndexableInfoFromQueue(List<QueuedEvent> queue)
        {
            var indexableListToUpdate = new Dictionary<DataUri, IndexableInfo>();
            var indexableListToRemove = new Dictionary<DataUri, IndexableInfo>();
            var indexableListToAddVersion = new Dictionary<DataUri, IndexableInfo>();

            foreach (var queuedEvent in queue)
            {
                var instanceData = this.Database.RemoteEvents.Queue.DeserializeEvent(queuedEvent) as ItemRemoteEventBase;

                if (instanceData == null)
                {
                    continue;
                }

                var key = new DataUri(ID.Parse(instanceData.ItemId), Language.Parse(instanceData.LanguageName), Data.Version.Parse(instanceData.VersionNumber))
                {
                    Path = instanceData.ItemId.ToString()//the fix
                };                
                var itemUri = new ItemUri(key.ItemID, key.Language, key.Version, this.Database);
                var indexable = new IndexableInfo(new SitecoreItemUniqueId(itemUri), queuedEvent.Timestamp);

                if (instanceData is RemovedVersionRemoteEvent || instanceData is DeletedItemRemoteEvent)
                {
                    this.HandleIndexableToRemove(indexableListToRemove, key, indexable);
                }
                else if (instanceData is AddedVersionRemoteEvent)
                {
                    this.HandleIndexableToAddVersion(indexableListToAddVersion, key, indexable);
                }
                else
                {
                    this.UpdateIndexableInfo(instanceData, indexable);
                    this.HandleIndexableToUpdate(indexableListToUpdate, key, indexable);
                }
            }

            return indexableListToUpdate.Select(x => x.Value)
              .Union(indexableListToRemove.Select(x => x.Value))
              .Union(indexableListToAddVersion.Select(x => x.Value))
              .OrderBy(x => x.Timestamp).ToList();
        }

        /// <summary>
        /// Runs the specified queue.
        /// </summary>
        /// <param name="queue">The queue.</param>
        /// <param name="index">The index.</param>
        protected override void Run(List<QueuedEvent> queue, ISearchIndex index)
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
                if (this.RaiseRemoteEvents)
                {
                    Job fullRebuildJob = IndexCustodian.FullRebuild(index);
                    fullRebuildJob.Wait();
                }
                else
                {
                    Job fullRebuildRemoteJob = IndexCustodian.FullRebuildRemote(index);
                    fullRebuildRemoteJob.Wait();
                }

                return;
            }

            List<IndexableInfo> parsed = this.ExtractIndexableInfoFromQueue(queue).ToList();
            CrawlingLog.Log.Info(string.Format("[Index={0}] Updating '{1}' items from Event Queue.", index.Name, parsed.Count()));

            Job incrememtalUpdateJob = IndexCustodian.IncrementalUpdate(index, parsed);
            incrememtalUpdateJob.Wait();
        }

        #region Private methods

        /// <summary>
        /// Updates the indexable info.
        /// </summary>
        /// <param name="instanceData">The instance data.</param>
        /// <param name="indexable">The indexable.</param>
        private void UpdateIndexableInfo(ItemRemoteEventBase instanceData, IndexableInfo indexable)
        {
            UpdateIndexableInfoMethodInfo.Invoke(this, new object[]
            {
                instanceData, indexable
            });
        }

        /// <summary>
        /// Processes indexable that contains data about item or version removed events.
        /// </summary>
        /// <param name="collection">The indexable collection.</param>
        /// <param name="key">The indexable key.</param>
        /// <param name="indexable">The indexable data.</param>
        private void HandleIndexableToRemove(Dictionary<DataUri, IndexableInfo> collection, DataUri key, IndexableInfo indexable)
        {
            HandleIndexableToRemoveMethodInfo.Invoke(this, new object[]
            {
                collection, key, indexable
            });
        }

        /// <summary>
        /// Handles the indexable to add version.
        /// </summary>
        /// <param name="collection">The collection.</param>
        /// <param name="key">The key.</param>
        /// <param name="indexable">The indexable.</param>
        private void HandleIndexableToAddVersion(Dictionary<DataUri, IndexableInfo> collection, DataUri key, IndexableInfo indexable)
        {
            HandleIndexableToAddVersionMethodInfo.Invoke(this, new object[]
            {
                collection, key, indexable
            });
        }

        /// <summary>
        /// Processes indexable that contains data about item or version removed events.
        /// </summary>
        /// <param name="collection">The indexable collection.</param>
        /// <param name="key">The indexable key.</param>
        /// <param name="indexable">The indexable data.</param>
        private void HandleIndexableToUpdate(Dictionary<DataUri, IndexableInfo> collection, DataUri key, IndexableInfo indexable)
        {
            HandleIndexableToUpdateMethodInfo.Invoke(this, new object[]
            {
                collection, key, indexable
            });
        }

        #endregion

        public OnPublishEndAsynchronousStrategy(string database) : base(database)
        {
        }
    }
}
