using System;
using Amazon;

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