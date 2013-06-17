using Amazon;
using System;
using System.Configuration;

namespace NHibernate.Caches.Elasticache
{
    public sealed class ElasticConfigurationSection : ConfigurationSection, IElasticConfiguration
    {
        [ConfigurationProperty("endpoint", IsRequired = false, IsKey = true)]
        public String RegionEndpointName
        {
            get { return (String)base["endpoint"]; }
        }

        [ConfigurationProperty("cluster", IsRequired = true, IsKey = true)]
        public string ClusterId
        {
            get { return (string)base["cluster"]; }
        }

        [ConfigurationProperty("interval", IsRequired = false, IsKey = true)]
        public TimeSpan CheckInterval
        {
            get { return (TimeSpan)base["interval"]; }
        }

        public RegionEndpoint CreateRegionEndpoint()
        {
            var allRegions = RegionEndpoint.EnumerableAllRegions;
            foreach (var regionEndpoint in allRegions)
            {
                if (regionEndpoint.SystemName.Equals(RegionEndpointName))
                    return regionEndpoint;
            }

            return RegionEndpoint.SAEast1;
        }

        public TimeSpan CreateInterval()
        {
            if (CheckInterval == default(TimeSpan))
            {
                return TimeSpan.FromMinutes(10);
            }
            return CheckInterval;
        }
    }
}