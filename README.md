NHibernate.Caches.Elasticache
=============================
What is NHibernate.Caches.Elasticache?
--------------------------------
NHibernate.Caches.Elasticache is a second level cache provider for NHibernate using Enyim Memcached and Amazon ElastiCache Cluster Config, based on the Amazon's Elasticache service.
How do I get started?
--------------------------------
Create an account in the [AWS](http://aws.amazon.com/) and then subscribe to the Elasticache service.

Install NHibernate.Caches.Elasticache, run the following command in the [Package Manager Console](http://docs.nuget.org/docs/start-here/using-the-package-manager-console): 

	PM> Install-Package NHibernate.Caches.Elasticache

Config your application to use the [Enyim Memcached](https://github.com/enyim/EnyimMemcached) and [Amazon ElastiCache Cluster Config](https://github.com/awslabs/elasticache-cluster-config-net) (app.config or web.config):

	<configuration>
	
	  <configSections>
        <sectionGroup name="enyim.com">
          <section name="memcached" type="Enyim.Caching.Configuration.MemcachedClientSection, Enyim.Caching" />
        </sectionGroup>
		<section name="clusterclient" type="Amazon.ElastiCacheCluster.ClusterConfigSettings, Amazon.ElastiCacheCluster"/>
      </configSections>
	  
	  <clusterclient>
        <endpoint hostname="YOU-ENDPOINT-URL-HERE" port="YOUR-PORT-HERE"/> <!-- REQUIRED -->
        <node nodeTries="INTEGER" nodeDelay="INTEGER"/> <!-- OPTIONAL -->
        <poller intervalDelay="INTEGER"/> <!-- OPTIONAL -->
      </clusterclient>
	  
	  <enyim.com>
		<memcached protocol="Binary">
		  <socketPool minPoolSize="10" maxPoolSize="200" connectionTimeout="00:00:10" deadTimeout="00:00:10" />
		</memcached>
	  </enyim.com>
	  
	</configuration>

Add the following property to your NHibernate config, the following example could change depending of your configuration: 

	<property name="cache.provider_class">NHibernate.Caches.Elasticache.ElasticacheProvider, NHibernate.Caches.Elasticache</property>
	<property name="cache.use_query_cache">true</property>
    <property name="cache.use_second_level_cache">true</property>

