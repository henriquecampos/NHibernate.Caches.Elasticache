NHibernate.Caches.Elasticache
=============================
What is NHibernate.Caches.Elasticache?
--------------------------------
NHibernate.Caches.Elasticache is a second level cache provider for NHibernate using a custom Enyim Memcached client and the provider found at http://sourceforge.net/projects/nhcontrib, based on the Amazon's Elasticache service.
How do I get started?
--------------------------------
Create an account in the [AWS](http://aws.amazon.com/) and then subscribe to the Elasticache service.

Install NHibernate.Caches.Elasticache, run the following command in the [Package Manager Console](http://docs.nuget.org/docs/start-here/using-the-package-manager-console): 

	PM> Install-Package NHibernate.Caches.Elasticache

Config your application to use the NHibernate.Caches.Elasticache (app.config or web.config):

	<configuration>
	
	  <configSections>
	  
		<section name="elastiCache" type="NHibernate.Caches.Elasticache.ElasticConfigurationSection, NHibernate.Caches.Elasticache" />

		<sectionGroup name="enyim.com">
		  <section name="memcached" type="Enyim.Caching.Configuration.MemcachedClientSection, Enyim.Caching" />
		</sectionGroup>
		
	  </configSections>	
	  
	  <elastiCache endpoint="us-east-1" interval="00:10:00" cluster="your cluster name in AWS Elasticache" />
	  
	  <enyim.com>
		<memcached protocol="Binary">
		  <socketPool minPoolSize="10" maxPoolSize="200" connectionTimeout="00:00:10" deadTimeout="00:00:10" />
		</memcached>
	  </enyim.com>
	  
	</configuration>

Add the following property to your NHibernate config, the following example could change depending of your configuration: 

	<property name="cache.provider_class">NHibernate.Caches.Elasticache.MemCacheProvider, NHibernate.Caches.Elasticache</property>

