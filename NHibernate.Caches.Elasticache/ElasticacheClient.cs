using Enyim.Caching;
using Enyim.Caching.Memcached;
using NHibernate.Cache;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;

namespace NHibernate.Caches.Elasticache
{
    public class ElasticacheClient : ICache
    {
        private static readonly IInternalLogger Log;
        private static readonly HashAlgorithm Hasher;
        private static readonly MD5 Md5;

        private readonly MemcachedClient _client;
        private readonly int _expiry;

        private readonly string _region;
        private readonly string _regionPrefix = "";

        static ElasticacheClient()
        {
            Log = LoggerProvider.LoggerFor(typeof(ElasticacheClient));
            Hasher = HashAlgorithm.Create();
            Md5 = MD5.Create();
        }

        public ElasticacheClient(string regionName, IDictionary<string, string> properties, MemcachedClient memcachedClient)
        {
            _region = regionName;
            _client = memcachedClient;
            _expiry = 300;

            if (properties == null) return;

            var expirationString = GetExpirationString(properties);
            if (expirationString != null)
            {
                _expiry = Convert.ToInt32(expirationString);
                if (Log.IsDebugEnabled)
                {
                    Log.DebugFormat("using expiration of {0} seconds", _expiry);
                }
            }

            if (properties.ContainsKey("regionPrefix"))
            {
                _regionPrefix = properties["regionPrefix"];
                if (Log.IsDebugEnabled)
                {
                    Log.DebugFormat("new regionPrefix :{0}", _regionPrefix);
                }
            }
            else
            {
                if (Log.IsDebugEnabled)
                {
                    Log.Debug("no regionPrefix value given, using defaults");
                }
            }
        }

        #region ICache Members

        public object Get(object key)
        {
            if (key == null)
            {
                return null;
            }
            if (Log.IsDebugEnabled)
            {
                Log.DebugFormat("fetching object {0} from the cache", key);
            }
            object maybeObj = _client.Get(KeyAsString(key));
            if (maybeObj == null)
            {
                return null;
            }
            //we need to check here that the key that we stored is really the key that we got
            //the reason is that for long keys, we hash the value, and this mean that we may get
            //hash collisions. The chance is very low, but it is better to be safe
            var de = (DictionaryEntry)maybeObj;
            string checkKeyHash = GetAlternateKeyHash(key);
            if (checkKeyHash.Equals(de.Key))
            {
                return de.Value;
            }
            return null;
        }

        public void Put(object key, object value)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key), "null key not allowed");
            }
            if (value == null)
            {
                throw new ArgumentNullException(nameof(value), "null value not allowed");
            }

            if (Log.IsDebugEnabled)
            {
                Log.DebugFormat("setting value for item {0}", key);
            }
            /*bool returnOk = _client.Store(
                StoreMode.Set, KeyAsString(key),
                new DictionaryEntry(GetAlternateKeyHash(key), value),
                TimeSpan.FromSeconds(_expiry));*/

            var operationResult = _client.ExecuteStore(StoreMode.Set, KeyAsString(key), new DictionaryEntry(GetAlternateKeyHash(key), value),
                TimeSpan.FromSeconds(_expiry));

            if (!operationResult.Success)
            {
                if (Log.IsWarnEnabled)
                {
                    Log.WarnFormat("could not save: {0} => {1} ({2})", key, value);
                }
            }
        }

        public void Remove(object key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }
            if (Log.IsDebugEnabled)
            {
                Log.DebugFormat("removing item {0}", key);
            }
            _client.Remove(KeyAsString(key));
        }

        public void Clear()
        {
            _client.FlushAll();
        }

        public void Destroy()
        {
            Clear();
        }

        public void Lock(object key)
        {
            // do nothing
        }

        public void Unlock(object key)
        {
            // do nothing
        }

        public long NextTimestamp()
        {
            return Timestamper.Next();
        }

        public int Timeout => Timestamper.OneMs * 60000;

        public string RegionName => _region;

        #endregion

        private static string GetExpirationString(IDictionary<string, string> props)
        {
            string result;
            if (!props.TryGetValue("expiration", out result))
            {
                props.TryGetValue(Cfg.Environment.CacheDefaultExpiration, out result);
            }
            return result;
        }

        /// <summary>
        /// Turn the key obj into a string, preperably using human readable
        /// string, and if the string is too long (>=250) it will be hashed
        /// </summary>
        private string KeyAsString(object key)
        {
            string fullKey = FullKeyAsString(key);
            if (fullKey.Length >= 250) //max key size for memcache
            {
                return ComputeHash(fullKey, Hasher);
            }
            return fullKey.Replace(' ', '-');
        }

        /// <summary>
        /// Turn the key object into a human readable string.
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        private string FullKeyAsString(object key)
        {
            return $"{_regionPrefix}{_region}@{key?.ToString() ?? string.Empty}";
        }

        /// <summary>
        /// Compute the hash of the full key string using the given hash algorithm
        /// </summary>
        /// <param name="fullKeyString">The full key return by call FullKeyAsString</param>
        /// <param name="hashAlgorithm">The hash algorithm used to hash the key</param>
        /// <returns>The hashed key as a string</returns>
        private static string ComputeHash(string fullKeyString, HashAlgorithm hashAlgorithm)
        {
            byte[] bytes = Encoding.ASCII.GetBytes(fullKeyString);
            byte[] computedHash = hashAlgorithm.ComputeHash(bytes);
            return Convert.ToBase64String(computedHash);
        }

        /// <summary>
        /// Compute an alternate key hash; used as a check that the looked-up value is 
        /// in fact what has been put there in the first place.
        /// </summary>
        /// <param name="key"></param>
        /// <returns>The alternate key hash (using the MD5 algorithm)</returns>
        private string GetAlternateKeyHash(object key)
        {
            string fullKey = FullKeyAsString(key);
            return fullKey.Length >= 250 ? ComputeHash(fullKey, Md5) : fullKey.Replace(' ', '-');
        }
    }
}