using System;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using BenchmarkDotNet.Attributes;
using CacheManager.Core;
using Enyim.Caching;
using Enyim.Caching.Configuration;

namespace CacheManager.Benchmarks
{
    [ExcludeFromCodeCoverage]
    public abstract class BaseCacheBenchmark
    {
        private static ICacheManagerConfiguration BaseConfig
            => new ConfigurationBuilder()
            .WithMaxRetries(10)
            .WithRetryTimeout(500)
            .WithJsonSerializer()
            .WithUpdateMode(CacheUpdateMode.Up)
            .Build();

        private static IMemcachedClientConfiguration MemcachedConfig
        {
            get
            {
                var cfg = new MemcachedClientConfiguration();
                cfg.AddServer("localhost", 11211);
                return cfg;
            }
        }

        protected ICacheManager<string> DictionaryCache = new BaseCacheManager<string>(BaseConfig.Builder.WithDictionaryHandle().Build());

        protected ICacheManager<string> RuntimeCache = new BaseCacheManager<string>(BaseConfig.Builder.WithSystemRuntimeCacheHandle().Build());

        protected ICacheManager<string> RedisCache = new BaseCacheManager<string>(
                BaseConfig
                .Builder
                .WithRedisConfiguration("redisKey", "localhost:6379,allowAdmin=true")
                .WithRedisCacheHandle("redisKey")
                .Build());

        protected ICacheManager<string> MsMemoryCache = new BaseCacheManager<string>(BaseConfig.Builder.WithMicrosoftMemoryCacheHandle().Build());

        protected ICacheManager<string> MemcachedCache =
            new BaseCacheManager<string>(BaseConfig.Builder
                .WithMemcachedCacheHandle(new MemcachedClient(MemcachedConfig)).Build());

        [GlobalSetup]
        public void Setup()
        {
            DictionaryCache.ClearAsync();
            RuntimeCache.ClearAsync();
            RedisCache.ClearAsync();
            MsMemoryCache.ClearAsync();
            MemcachedCache.ClearAsync();
            SetupBench();
        }

        [Benchmark(Baseline = true)]
        public void Dictionary()
        {
            Excecute(DictionaryCache);
        }

        [Benchmark]
        public void Runtime()
        {
            Excecute(RuntimeCache);
        }

        [Benchmark]
        public void MsMemory()
        {
            Excecute(MsMemoryCache);
        }

        [Benchmark]
        public void Redis()
        {
            Excecute(RedisCache);
        }

        [Benchmark]
        public void Memcached()
        {
            Excecute(MemcachedCache);
        }

        protected abstract void Excecute(ICacheManager<string> cache);

        protected virtual void SetupBench()
        {
        }
    }

    #region add

    [ExcludeFromCodeCoverage]
    public class AddSingleBenchmark : BaseCacheBenchmark
    {
        private string _key = Guid.NewGuid().ToString();

        protected override void Excecute(ICacheManager<string> cache)
        {
            if (!cache.AddAsync(_key, "value"))
            {
                cache.RemoveAsync(_key);
            }
        }
    }

    [ExcludeFromCodeCoverage]
    public class AddWithRegionSingleBenchmark : BaseCacheBenchmark
    {
        private string _key = Guid.NewGuid().ToString();

        protected override void Excecute(ICacheManager<string> cache)
        {
            if (!cache.AddAsync(_key, "value", "region"))
            {
                cache.RemoveAsync(_key);
            }
        }
    }

    #endregion add

    #region put

    [ExcludeFromCodeCoverage]
    public class PutSingleBenchmark : BaseCacheBenchmark
    {
        private string _key = Guid.NewGuid().ToString();

        protected override void Excecute(ICacheManager<string> cache)
        {
            cache.PutAsync(_key, "value");
        }
    }

    [ExcludeFromCodeCoverage]
    public class PutWithRegionSingleBenchmark : BaseCacheBenchmark
    {
        private string _key = Guid.NewGuid().ToString();

        protected override void Excecute(ICacheManager<string> cache)
        {
            cache.PutAsync(_key, "value", "region");
        }
    }

    #endregion put

    #region get

    [ExcludeFromCodeCoverage]
    public class GetSingleBenchmark : BaseCacheBenchmark
    {
        protected string Key = Guid.NewGuid().ToString();

        protected override void Excecute(ICacheManager<string> cache)
        {
            var val = cache.GetAsync(Key);
            if (val == null)
            {
                throw new InvalidOperationException();
            }
        }

        protected override void SetupBench()
        {
            base.SetupBench();

            DictionaryCache.AddAsync(Key, Key);
            DictionaryCache.AddAsync(Key, Key, "region");
            RuntimeCache.AddAsync(Key, Key);
            RuntimeCache.AddAsync(Key, Key, "region");
            MsMemoryCache.AddAsync(Key, Key);
            MsMemoryCache.AddAsync(Key, Key, "region");
            MemcachedCache.AddAsync(Key, Key);
            MemcachedCache.AddAsync(Key, Key, "region");
            RedisCache.AddAsync(Key, Key);
            RedisCache.AddAsync(Key, Key, "region");
        }
    }

    [ExcludeFromCodeCoverage]
    public class GetWithRegionSingleBenchmark : GetSingleBenchmark
    {
        protected override void Excecute(ICacheManager<string> cache)
        {
            var val = cache.GetAsync(Key, "region");
            if (val == null)
            {
                throw new InvalidOperationException();
            }
        }
    }

    #endregion get

    #region update

    [ExcludeFromCodeCoverage]
    public class UpdateSingleBenchmark : GetSingleBenchmark
    {
        protected override void Excecute(ICacheManager<string> cache)
        {
            var val = cache.Update(Key, (v) => v.Equals("bla") ? "bla" : "blub");
            if (val == null)
            {
                throw new InvalidOperationException();
            }
        }
    }

    #endregion upate
}
