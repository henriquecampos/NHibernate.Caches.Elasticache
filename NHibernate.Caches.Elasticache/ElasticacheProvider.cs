using Amazon.ElastiCacheCluster;
using Enyim.Caching;
using NHibernate.Cache;
using System.Collections.Generic;

namespace NHibernate.Caches.Elasticache
{
    public class ElasticacheProvider : ICacheProvider
    {
        private MemcachedClient _clientInstance;
        private readonly object _syncObject = new object();

        #region ICacheProvider Members

        public ICache BuildCache(string regionName, IDictionary<string, string> properties)
        {
            if (regionName == null)
                regionName = "";

            if (properties == null)
                properties = new Dictionary<string, string>();

            return new ElasticacheClient(regionName, properties, _clientInstance);
        }

        public long NextTimestamp()
        {
            return Timestamper.Next();
        }

        public void Start(IDictionary<string, string> properties)
        {
            // Needs to lock staticly because the pool and the internal maintenance thread
            // are both static, and I want them syncs between starts and stops.
            lock (_syncObject)
            {
                if (_clientInstance != null) return;

                _clientInstance = new MemcachedClient(new ElastiCacheClusterConfig());
            }
        }

        public void Stop()
        {
            lock (_syncObject)
            {
                _clientInstance.Dispose();
                _clientInstance = null;
            }
        }

        #endregion
    }
}