using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using Sitecore.Abstractions;
using Sitecore.ContentSearch;
using Sitecore.ContentSearch.Maintenance;
using Sitecore.Eventing;

namespace Sitecore.Support.ContentSearch.Maintenance.Strategies
{
  [DataContract]
  public class OnPublishEndAsynchronousStrategy : BaseAsynchronousStrategy
  {
    public OnPublishEndAsynchronousStrategy(string database) : base(database)
    {
    }

    protected List<ISearchIndex> Indexes { get; set; }

    public override void Initialize(ISearchIndex searchIndex)
    {
      base.Initialize(searchIndex);

      if (this.Settings.EnableEventQueues())
      {
        EventHub.PublishEnd += (sender, args) => this.Handle();
        this.Handle();
      }

      this.Indexes = this.Indexes ?? new List<ISearchIndex>();
      this.Indexes.Add(searchIndex);
    }

    protected override void OnIndexingStarted(object sender, EventArgs args)
    {
      var indexName = ContentSearchManager.Locator.GetInstance<IEvent>().ExtractParameter<string>(args, 0);
      var isFullRebuild = (bool)ContentSearchManager.Locator.GetInstance<IEvent>().ExtractParameter(args, 1);

      if (this.Indexes.All(x => x.Name != indexName) || !isFullRebuild)
      {
        return;
      }

      QueuedEvent lastEvent = this.Database.RemoteEvents.Queue.GetLastEvent();
      if (lastEvent != null)
      {
        this.IndexTimestamps[indexName] = lastEvent.Timestamp;
      }
    }

    protected override void OnIndexingEnded(object sender, EventArgs args)
    {
      var indexName = ContentSearchManager.Locator.GetInstance<IEvent>().ExtractParameter<string>(args, 0);
      var isFullRebuild = (bool)ContentSearchManager.Locator.GetInstance<IEvent>().ExtractParameter(args, 1);
      var index = this.Indexes.FirstOrDefault(x => x.Name == indexName);

      if (index != null && isFullRebuild && this.IndexTimestamps.ContainsKey(indexName))
      {
        this.Index.Summary.LastUpdatedTimestamp = this.IndexTimestamps[indexName];
      }
    }
  }
}