using Enyim.Caching;
using Enyim.Caching.Configuration;
using Enyim.Caching.Memcached;
using Enyim.Caching.Memcached.Results.Factories;
using NHibernate.Cache;
using System.Collections.Generic;
using System.Configuration;
using System.Net;

namespace NHibernate.Caches.Elasticache
{
    public class MemCacheProvider : ICacheProvider
    {
        private static readonly IMemcachedClientConfiguration Config;
        private static readonly IElasticConfiguration ElasticConfig;

        private MemcachedClient _clientInstance;
        private readonly object _syncObject = new object();

        static MemCacheProvider()
        {
            Config = ConfigurationManager.GetSection("enyim.com/memcached") as IMemcachedClientConfiguration;
            ElasticConfig = ConfigurationManager.GetSection("elastiCache") as IElasticConfiguration;

            if (Config == null)
            {
                Config = new MemcachedClientConfiguration();
                Config.Servers.Add(new IPEndPoint(IPAddress.Loopback, 11211));
            }

            if (ElasticConfig == null)
            {
                ElasticConfig = new ElasticConfigurationSection();
            }
        }

        #region ICacheProvider Members

        public ICache BuildCache(string regionName, IDictionary<string, string> properties)
        {
            if (regionName == null)
                regionName = "";

            if (properties == null)
                properties = new Dictionary<string, string>();

            return new MemCacheClient(regionName, properties, _clientInstance);
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

                if (Config == null)
                    throw new ConfigurationErrorsException("Configuration for enyim.com/memcached not found");

                if (ElasticConfig == null)
                    throw new ConfigurationErrorsException("Configuration for elastiCache not found");

                var pool = new BinaryPool(Config, ElasticConfig);
                IMemcachedKeyTransformer keyTransformer = Config.CreateKeyTransformer() ?? new DefaultKeyTransformer();
                ITranscoder transcoder = Config.CreateTranscoder() ?? new DefaultTranscoder();
                IPerformanceMonitor performanceMonitor = Config.CreatePerformanceMonitor();
                _clientInstance = new MemcachedClient(pool, keyTransformer, transcoder, performanceMonitor)
                {
                    StoreOperationResultFactory = new DefaultStoreOperationResultFactory(),
                    GetOperationResultFactory = new DefaultGetOperationResultFactory(),
                    MutateOperationResultFactory = new DefaultMutateOperationResultFactory(),
                    ConcatOperationResultFactory = new DefaultConcatOperationResultFactory(),
                    RemoveOperationResultFactory = new DefaultRemoveOperationResultFactory()
                };
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