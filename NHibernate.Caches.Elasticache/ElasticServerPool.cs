using Amazon;
using Amazon.ElastiCache;
using Amazon.ElastiCache.Model;
using Enyim.Caching.Configuration;
using Enyim.Caching.Memcached;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;

namespace NHibernate.Caches.Elasticache
{
    public class ElasticServerPool : IServerPool
    {
        private readonly Timer _checkNodesTimer;
        private readonly string _clusterId;
        private readonly IMemcachedClientConfiguration _configuration;
        private readonly Queue<IMemcachedNode> _deadNodes;
        private readonly object _deadSync = new Object();
        private readonly IOperationFactory _factory;
        private readonly TimeSpan _interval;
        private readonly RegionEndpoint _regionEndpoint;
        private readonly Timer _ressurectTimer;
        private IMemcachedNode[] _allNodes;
        private IPEndPoint[] _endpoints;
        private bool _isDisposed;
        private bool _isRessurectRunning;
        private IMemcachedNodeLocator _nodeLocator;

        public ElasticServerPool(IMemcachedClientConfiguration configuration, IElasticConfiguration elasticConfig, IOperationFactory opFactory)
        {
            if (configuration == null) throw new ArgumentNullException("configuration");
            if (elasticConfig == null) throw new ArgumentNullException("elasticConfig");
            if (opFactory == null) throw new ArgumentNullException("opFactory");

            _configuration = configuration;
            _regionEndpoint = elasticConfig.CreateRegionEndpoint();
            _interval = elasticConfig.CreateInterval();
            _clusterId = elasticConfig.ClusterId;
            _factory = opFactory;

            _ressurectTimer = new Timer(RessurectNodes, null, Timeout.Infinite, Timeout.Infinite);
            _checkNodesTimer = new Timer(CheckNodesCallback, null, Timeout.Infinite, Timeout.Infinite);
            _deadNodes = new Queue<IMemcachedNode>();
        }

        ~ElasticServerPool()
        {
            try
            {
                ((IDisposable)this).Dispose();
            }
            catch { }
        }

        protected virtual IMemcachedNode CreateNode(IPEndPoint endpoint)
        {
            return new MemcachedNode(endpoint, _configuration.SocketPool);
        }

        private IMemcachedNode[] CreateNodes(IPEndPoint[] endpoints)
        {
            var newNodes = new IMemcachedNode[endpoints.Length];
            for (int i = 0; i < endpoints.Length; i++)
            {
                IMemcachedNode node = CreateNode(endpoints[i]);
                node.Failed += NodeFail;
                newNodes[i] = node;
            }
            return newNodes;
        }

        private void CheckNodesCallback(object state)
        {
            CheckNodes();
        }

        private void CheckNodes()
        {
            lock (_deadSync)
            {
                if (_isDisposed) return;

                IPEndPoint[] newEndpoints = GetCurrentEndpoints();

                if (_endpoints.SequenceEqual(newEndpoints))
                {
                    return;
                }

                //Clear dead nodes and stop the ressurect timer
                _ressurectTimer.Change(Timeout.Infinite, Timeout.Infinite);
                _isRessurectRunning = false;
                _deadNodes.Clear();

                IMemcachedNode[] newNodes = CreateNodes(newEndpoints);

                Interlocked.Exchange(ref _endpoints, newEndpoints);
                _nodeLocator.Initialize(newNodes);

                IMemcachedNode[] oldNodes = Interlocked.Exchange(ref _allNodes, newNodes);

                foreach (IMemcachedNode node in oldNodes)
                {
                    try { node.Dispose(); }
                    catch { }
                }
            }
        }

        private IPEndPoint[] GetCurrentEndpoints()
        {
            if (String.IsNullOrEmpty(_clusterId))
            {
                _checkNodesTimer.Change(Timeout.Infinite, Timeout.Infinite);
                return new IPEndPoint[0];
            }

            IPEndPoint[] newEndpoints;
            using (var client = new AmazonElastiCacheClient(_regionEndpoint))
            {
                var request = new DescribeCacheClustersRequest
                {
                    CacheClusterId = _clusterId,
                    ShowCacheNodeInfo = true
                };

                DescribeCacheClustersResponse response = client.DescribeCacheClusters(request);

                if (!response.CacheClusters.Any())
                {
                    //Cant find cluster id, disabling callback
                    _checkNodesTimer.Change(Timeout.Infinite, Timeout.Infinite);
                    return new IPEndPoint[0];
                }

                int nodeCount = response.CacheClusters.Sum(x => x.CacheNodes.Count);
                newEndpoints = new IPEndPoint[nodeCount];

                int i = 0;
                foreach (CacheCluster cluster in response.CacheClusters)
                {
                    foreach (CacheNode node in cluster.CacheNodes)
                    {
                        Endpoint nodeEndpoint = node.Endpoint;
                        IPEndPoint ipEndPoint = ConfigurationHelper.ResolveToEndPoint(nodeEndpoint.Address, nodeEndpoint.Port);
                        newEndpoints[i++] = ipEndPoint;
                    }
                }
            }

            return newEndpoints;
        }

        private void NodeFail(IMemcachedNode node)
        {
            lock (_deadSync)
            {
                if (_isDisposed) return;

                // bubble up the fail event to the client
                Action<IMemcachedNode> handler = InnerNodeFailed;
                if (handler != null) handler(node);

                if (!_deadNodes.Contains(node))
                    _deadNodes.Enqueue(node);

                if (_isRessurectRunning) return;

                _isRessurectRunning = true;
                _ressurectTimer.Change(TimeSpan.FromSeconds(30), TimeSpan.FromMilliseconds(-1));
            }
        }

        private void RessurectNodes(object state)
        {
            lock (_deadSync)
            {
                if (_isDisposed) return;

                int deadNodesCount = _deadNodes.Count;
                for (int i = 0; i < deadNodesCount; i++)
                {
                    IMemcachedNode node = _deadNodes.Dequeue();
                    if (!node.Ping())
                        _deadNodes.Enqueue(node);
                }

                if (_deadNodes.Count > 0)
                {
                    _ressurectTimer.Change(TimeSpan.FromSeconds(10), TimeSpan.FromMilliseconds(-1));
                }
                else
                    _isRessurectRunning = false;
            }
        }

        #region [ IServerPool                  ]

        IMemcachedNode IServerPool.Locate(string key)
        {
            return _nodeLocator.Locate(key);
        }

        IOperationFactory IServerPool.OperationFactory
        {
            get { return _factory; }
        }

        IEnumerable<IMemcachedNode> IServerPool.GetWorkingNodes()
        {
            return _nodeLocator.GetWorkingNodes();
        }

        void IServerPool.Start()
        {
            _endpoints = new IPEndPoint[0];
            _allNodes = new IMemcachedNode[0];
            _nodeLocator = new KetamaNodeLocator();
            _nodeLocator.Initialize(_allNodes);
            CheckNodes();

            _checkNodesTimer.Change(_interval, _interval);
        }

        private event Action<IMemcachedNode> InnerNodeFailed;
        event Action<IMemcachedNode> IServerPool.NodeFailed
        {
            add { InnerNodeFailed += value; }
            remove { InnerNodeFailed -= value; }
        }

        #endregion

        #region [ IDisposable                  ]

        void IDisposable.Dispose()
        {
            GC.SuppressFinalize(this);

            lock (_deadSync)
            {
                if (_isDisposed) return;

                _isDisposed = true;

                // dispose the locator first, maybe it wants to access 
                // the nodes one last time
                var nd = _nodeLocator as IDisposable;
                if (nd != null)
                {
                    try { nd.Dispose(); }
                    catch { }
                }

                _nodeLocator = null;

                foreach (IMemcachedNode node in _allNodes)
                {
                    try { node.Dispose(); }
                    catch { }
                }

                // stop the timer
                using (_checkNodesTimer)
                    _checkNodesTimer.Change(Timeout.Infinite, Timeout.Infinite);

                using (_ressurectTimer)
                    _ressurectTimer.Change(Timeout.Infinite, Timeout.Infinite);

                _allNodes = null;
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