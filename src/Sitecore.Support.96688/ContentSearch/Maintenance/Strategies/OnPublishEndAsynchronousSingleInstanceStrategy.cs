namespace Sitecore.Support.ContentSearch.Maintenance.Strategies
{
    using System.Linq;
    using System.Runtime.Serialization;
    using System.Threading.Tasks;
    using Sitecore.ContentSearch;
    using Sitecore.ContentSearch.Diagnostics;
    using Sitecore.Eventing;

    [DataContract]
    public class OnPublishEndAsynchronousSingleInstanceStrategy : Sitecore.Support.ContentSearch.Maintenance.Strategies.OnPublishEndAsynchronousStrategy
    {
        public OnPublishEndAsynchronousSingleInstanceStrategy(string database) : base(database)
        {
        }

        /// <summary>
        /// Runs the pipeline.
        /// </summary>
        public override void Run()
        {
            EventManager.RaiseQueuedEvents(); // in order to prevent indexing out-of-date data we have to force processing queued events before reading queue.

            var eventQueue = this.Database.RemoteEvents.Queue;

            if (eventQueue == null)
            {
                CrawlingLog.Log.Fatal(string.Format("Event Queue is empty. Returning."));
                return;
            }

            long? minLastUpdatedTimestamp = this.Indexes.OrderBy(index => (index.Summary.LastUpdatedTimestamp ?? 0)).FirstOrDefault().Summary.LastUpdatedTimestamp;

            var queue = this.ReadQueue(eventQueue, minLastUpdatedTimestamp);

            if (this.ContentSearchSettings.IsParallelIndexingEnabled())
            {
                Parallel.ForEach(this.Indexes, new ParallelOptions
                {
                    TaskScheduler = TaskSchedulerManager.LimitedConcurrencyLevelTaskSchedulerForIndexing
                },
                index => base.Run(queue, index));
            }
            else
            {
                foreach (ISearchIndex index in this.Indexes)
                {
                    base.Run(queue, index);
                }
            }
        }
    }
}
