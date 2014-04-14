using Amazon;
using System;

namespace NHibernate.Caches.Elasticache
{
    public interface IElasticConfiguration
    {
        String RegionEndpointName { get; }
        String ClusterId { get; }
        TimeSpan CheckInterval { get; }
        RegionEndpoint CreateRegionEndpoint();
        TimeSpan CreateInterval();
    }
}