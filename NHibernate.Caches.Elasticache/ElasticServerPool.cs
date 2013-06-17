using Amazon;
using Amazon.ElastiCache;
using Enyim.Caching.Configuration;
using Enyim.Caching.Memcached;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;

namespace NHibernate.Caches.Elasticache
{
    public class ElasticServerPool : IServerPool, IDisposable
    {
        private IPEndPoint[] endpoints;
        private IMemcachedNode[] allNodes;

		private IMemcachedClientConfiguration configuration;
        private IOperationFactory factory;
		private IMemcachedNodeLocator nodeLocator;

        private RegionEndpoint regionEndpoint;
        private TimeSpan interval;
        private string clusterId;

		private object DeadSync = new Object();
        private bool isDisposed;
        private System.Threading.Timer checkNodesTimer;
		private event Action<IMemcachedNode> nodeFailed;

		public ElasticServerPool(IMemcachedClientConfiguration configuration, IElasticConfiguration elasticConfig, IOperationFactory opFactory)
		{
			if (configuration == null) throw new ArgumentNullException("socketConfig");
            if (elasticConfig == null) throw new ArgumentNullException("elasticConfig");
			if (opFactory == null) throw new ArgumentNullException("opFactory");
            
			this.configuration = configuration;
            this.regionEndpoint = elasticConfig.CreateRegionEndpoint();
            this.interval = elasticConfig.CreateInterval();
            this.clusterId = elasticConfig.ClusterId;
			this.factory = opFactory;
		}

        ~ElasticServerPool()
		{
			try { ((IDisposable)this).Dispose(); }
			catch { }
		}

		protected virtual IMemcachedNode CreateNode(IPEndPoint endpoint)
		{
			return new MemcachedNode(endpoint, this.configuration.SocketPool);
		}

        private IMemcachedNode[] CreateNodes(IPEndPoint[] endpoints)
        {
            IMemcachedNode[] newNodes = new IMemcachedNode[endpoints.Length];
            for (int i = 0; i < endpoints.Length; i++)
            {
                var node = this.CreateNode(endpoints[i]);
                node.Failed += this.NodeFail;
                newNodes[i] = node;
            }
            return newNodes;
        }

		private void CheckNodesCallback(object state)
		{
            this.CheckNodes();	
		}

        private void CheckNodes()
        {
            lock (this.DeadSync)
            {
                if (this.isDisposed) return;

                IPEndPoint[] newEndpoints = GetCurrentEndpoints();

                if (this.endpoints.SequenceEqual(newEndpoints)) return;

                IMemcachedNode[] newNodes = this.CreateNodes(newEndpoints);

                Interlocked.Exchange(ref this.endpoints, newEndpoints);
                this.nodeLocator.Initialize(newNodes);

                var oldNodes = this.allNodes;
                Interlocked.Exchange(ref this.allNodes, newNodes);

                foreach (var node in oldNodes)
                    node.Dispose();

                oldNodes = null;
            }
        }

        private IPEndPoint[] GetCurrentEndpoints()
        {
            IPEndPoint[] newEndpoints;
            using (var client = new AmazonElastiCacheClient(this.regionEndpoint))
            {
                var request = new Amazon.ElastiCache.Model.DescribeCacheClustersRequest().WithCacheClusterId(this.clusterId).WithShowCacheNodeInfo(true);
                var result = client.DescribeCacheClusters(request).DescribeCacheClustersResult;

                var cluster = result.CacheClusters.FirstOrDefault();
                if (cluster == null)
                {
                    //Cant find cluster id, disabling callback
                    this.checkNodesTimer.Change(Timeout.Infinite, Timeout.Infinite);
                    return new IPEndPoint[0];
                }

                newEndpoints = new IPEndPoint[cluster.CacheNodes.Count];

                var cacheNodes = cluster.CacheNodes;
                for (int i = 0; i < cacheNodes.Count; i++)
                {
                    var nodeEndpoint = cacheNodes[i].Endpoint;
                    newEndpoints[i] = ConfigurationHelper.ResolveToEndPoint(nodeEndpoint.Address, nodeEndpoint.Port);
                }
            }

            return newEndpoints;
        }

		private void NodeFail(IMemcachedNode node)
		{
			lock (this.DeadSync)
			{
				if (this.isDisposed) return;

				// bubble up the fail event to the client
				var fail = this.nodeFailed;
				if (fail != null)
					fail(node);
			}
		}

		#region [ IServerPool                  ]

		IMemcachedNode IServerPool.Locate(string key)
		{
			return this.nodeLocator.Locate(key);
		}

		IOperationFactory IServerPool.OperationFactory
		{
			get { return this.factory; }
		}

		IEnumerable<IMemcachedNode> IServerPool.GetWorkingNodes()
		{
			return this.nodeLocator.GetWorkingNodes();
		}

		void IServerPool.Start()
		{
            this.endpoints = new IPEndPoint[0];
            this.allNodes = new IMemcachedNode[0];
            this.nodeLocator = new KetamaNodeLocator();
            this.nodeLocator.Initialize(this.allNodes);
            this.CheckNodes();

            this.checkNodesTimer = new System.Threading.Timer(CheckNodesCallback, null, this.interval, this.interval);
		}

		event Action<IMemcachedNode> IServerPool.NodeFailed
		{
			add { this.nodeFailed += value; }
			remove { this.nodeFailed -= value; }
		}

		#endregion
		#region [ IDisposable                  ]

		void IDisposable.Dispose()
		{
			GC.SuppressFinalize(this);

			lock (this.DeadSync)
			{
				if (this.isDisposed) return;

				this.isDisposed = true;

				// dispose the locator first, maybe it wants to access 
				// the nodes one last time
				var nd = this.nodeLocator as IDisposable;
				if (nd != null)
					try { nd.Dispose(); }
					catch (Exception) { }

				this.nodeLocator = null;

				for (var i = 0; i < this.allNodes.Length; i++)
					try { this.allNodes[i].Dispose(); }
					catch (Exception) { }

				// stop the timer
				if (this.checkNodesTimer != null)
					using (this.checkNodesTimer)
						this.checkNodesTimer.Change(Timeout.Infinite, Timeout.Infinite);

				this.allNodes = null;
				this.checkNodesTimer = null;
			}
		}

		#endregion
    }
}

#region [ License information          ]
/* ************************************************************
 * 
 *    Copyright (c) 2010 Attila Kiskó, enyim.com
 *    
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *    
 *        http://www.apache.org/licenses/LICENSE-2.0
 *    
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *    
 * ************************************************************/
#endregion