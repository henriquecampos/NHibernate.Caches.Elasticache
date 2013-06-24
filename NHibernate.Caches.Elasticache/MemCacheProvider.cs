﻿using System.Collections.Generic;
using System.Configuration;
using System.Net;
using System.Text;
using Enyim.Caching;
using Enyim.Caching.Configuration;
using Enyim.Caching.Memcached;
using Enyim.Caching.Memcached.Results.Factories;
using NHibernate.Cache;

namespace NHibernate.Caches.Elasticache
{
    /// <summary>
    /// Cache provider using the .NET client (http://github.com/enyim/EnyimMemcached)
    /// for memcached, which is located at http://memcached.org/
    /// </summary>
    public class MemCacheProvider : ICacheProvider
    {
        private static readonly IInternalLogger log;
        private static MemcachedClient clientInstance;
        private static readonly IMemcachedClientConfiguration config;
        private static readonly IElasticConfiguration elasticConfig;
        private static readonly object syncObject = new object();

        static MemCacheProvider()
        {
            log = LoggerProvider.LoggerFor(typeof(MemCacheProvider));
            config = ConfigurationManager.GetSection("enyim.com/memcached") as IMemcachedClientConfiguration;
            elasticConfig = ConfigurationManager.GetSection("elastiCache") as IElasticConfiguration;

            if (config == null)
            {
                log.Info("enyim.com/memcached configuration section not found, using default configuration (127.0.0.1:11211).");
                config = new MemcachedClientConfiguration();
                config.Servers.Add(new IPEndPoint(IPAddress.Loopback, 11211));
            }

            if (elasticConfig == null)
            {
                elasticConfig = new ElasticConfigurationSection();
            }
        }

        #region ICacheProvider Members

        public ICache BuildCache(string regionName, IDictionary<string, string> properties)
        {
            if (regionName == null)
            {
                regionName = "";
            }
            if (properties == null)
            {
                properties = new Dictionary<string, string>();
            }
            if (log.IsDebugEnabled)
            {
                var sb = new StringBuilder();
                foreach (var pair in properties)
                {
                    sb.Append("name=");
                    sb.Append(pair.Key);
                    sb.Append("&value=");
                    sb.Append(pair.Value);
                    sb.Append(";");
                }
                log.Debug("building cache with region: " + regionName + ", properties: " + sb);
            }
            return new MemCacheClient(regionName, properties, clientInstance);
        }

        public long NextTimestamp()
        {
            return Timestamper.Next();
        }

        public void Start(IDictionary<string, string> properties)
        {
            // Needs to lock staticly because the pool and the internal maintenance thread
            // are both static, and I want them syncs between starts and stops.
            lock (syncObject)
            {
                if (config == null)
                {
                    throw new ConfigurationErrorsException("Configuration for enyim.com/memcached not found");
                }

                if (clientInstance == null)
                {
                    var pool = new BinaryPool(config, elasticConfig);
                    IMemcachedKeyTransformer keyTransformer = config.CreateKeyTransformer() ?? new DefaultKeyTransformer();
                    ITranscoder transcoder = config.CreateTranscoder() ?? new DefaultTranscoder();
                    IPerformanceMonitor performanceMonitor = config.CreatePerformanceMonitor();
                    clientInstance = new MemcachedClient(pool, keyTransformer, transcoder, performanceMonitor);
                    clientInstance.StoreOperationResultFactory = new DefaultStoreOperationResultFactory();
                    clientInstance.GetOperationResultFactory = new DefaultGetOperationResultFactory();
                    clientInstance.MutateOperationResultFactory = new DefaultMutateOperationResultFactory();
                    clientInstance.ConcatOperationResultFactory = new DefaultConcatOperationResultFactory();
                    clientInstance.RemoveOperationResultFactory = new DefaultRemoveOperationResultFactory();
                }
            }
        }

        public void Stop()
        {
            lock (syncObject)
            {
                clientInstance.Dispose();
                clientInstance = null;
            }
        }

        #endregion
    }
}