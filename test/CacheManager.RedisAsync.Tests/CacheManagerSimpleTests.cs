using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CacheManager.Core;
using CacheManager.Core.Internal;
using CacheManager.Core.Utility;
using FluentAssertions;
using Xunit;

namespace CacheManager.RedisAsync.Tests
{
    /// <summary>
    /// Validates that add and put adds a new item to all handles defined. Validates that remove
    /// removes an item from all handles defined.
    /// </summary>
    [ExcludeFromCodeCoverage]
    public class CacheManagerSimpleTests
    {
        private static readonly AsyncLock asyncLock = new AsyncLock();

        #region general

        [Fact]
        public void CacheManager_AddCacheItem_WithExpMode_ButWithoutTimeout()
        {
            // arrange
            var cache = TestManagers.WithManyDictionaryHandles;
            var key = "key";

            // act
            Func<Task> act = () => cache.AddAsync(new CacheItem<object>(key, "something", ExpirationMode.Absolute, default(TimeSpan)));

            act.Should().Throw<ArgumentOutOfRangeException>()
                .WithMessage("Expiration timeout must be greater than zero*");
        }

        [Fact]
        [ReplaceCulture]
        public void CacheManager_CtorA_NoConfig()
        {
            Action act = () => new BaseCacheManager<object>(null);
            act.Should().Throw<ArgumentException>()
                    .WithMessage("*Parameter name: configuration");
        }

        [Fact]
        [ReplaceCulture]
        public void CacheManager_CtorA_ConfigNoName()
        {
            // name should be set from config and default is a Guid
            var manager = new BaseCacheManager<object>(ConfigurationBuilder.BuildConfiguration(s => s.WithDictionaryHandle()));
            manager.Name.Should().NotBeNullOrWhiteSpace();
        }

        [Fact]
        [ReplaceCulture]
        public void CacheManager_CtorA_ConfigWithName()
        {
            // name should be implicitly set
            var manager = new BaseCacheManager<object>(
                ConfigurationBuilder.BuildConfiguration("newName", s => s.WithDictionaryHandle()));

            manager.Name.Should().Be("newName");
        }

        #endregion general

        #region exists

        [Theory]
        [ReplaceCulture]
        [ClassData(typeof(TestCacheManagers))]
        public void CacheManager_Exists_InvalidKey(ICacheManager<object> cache)
        {
            using (cache)
            {
                // arrange act
                Func<Task> act = () => cache.ExistsAsync(null);
                Func<Task> actB = () => cache.ExistsAsync(null, "region");
                Func<Task> actR = () => cache.ExistsAsync("key", null);

                // assert
                act.Should().Throw<ArgumentException>(cache.Configuration.ToString())
                    .WithMessage("*Parameter name: key", cache.Configuration.ToString());

                actB.Should().Throw<ArgumentException>(cache.ToString())
                    .WithMessage("*Parameter name: key", cache.Configuration.ToString());

                actR.Should().Throw<ArgumentException>(cache.ToString())
                    .WithMessage("*Parameter name: region", cache.Configuration.ToString());
            }
        }

        [Theory]
        [ClassData(typeof(TestCacheManagers))]
        public async Task CacheManager_Exists_KeyDoesExist(ICacheManager<object> cache)
        {
            using (cache)
            {
                // arrange
                var key = Guid.NewGuid().ToString();
                var value = ComplexType.Create();

                // act
                await cache.AddAsync(key, value);

                // assert
                (await cache.ExistsAsync(key)).Should().BeTrue(cache.Configuration.ToString());
                (await cache.GetAsync(key)).Should().Be(value, cache.Configuration.ToString());
            }
        }

        [Theory]
        [ClassData(typeof(TestCacheManagers))]
        public async Task CacheManager_Exists_KeyRegionDoesExist(ICacheManager<object> cache)
        {
            using (cache)
            {
                // arrange
                var key = Guid.NewGuid().ToString();
                var region = Guid.NewGuid().ToString();
                var value = ComplexType.Create();

                // act
                await cache.AddAsync(key, value, region);

                // assert
                (await cache.ExistsAsync(key, region)).Should().BeTrue(cache.Configuration.ToString());
                (await cache.GetAsync(key, region)).Should().Be(value, cache.Configuration.ToString());
            }
        }

        [Theory]
        [ClassData(typeof(TestCacheManagers))]
        public async Task CacheManager_Exists_KeyDoesNotExist(ICacheManager<object> cache)
        {
            using (cache)
            {
                // arrange
                var key = Guid.NewGuid().ToString();

                // act
                // assert
                (await cache.ExistsAsync(key)).Should().BeFalse(cache.Configuration.ToString());
            }
        }

        [Theory]
        [ClassData(typeof(TestCacheManagers))]
        public async Task CacheManager_Exists_KeyRegionDoesNotExist(ICacheManager<object> cache)
        {
            using (cache)
            {
                // arrange
                var key = Guid.NewGuid().ToString();
                var region = Guid.NewGuid().ToString();

                // act
                // assert
                (await cache.ExistsAsync(key, region)).Should().BeFalse(cache.Configuration.ToString());
            }
        }

        #endregion

        #region put params validation

        [Fact]
        [ReplaceCulture]
        public void CacheManager_Put_InvalidKey()
        {
            using (var cache = CacheFactory.Build(settings =>
            {
                settings.WithDictionaryHandle("h1");
            }))
            {
                // arrange act
                Func<Task> act = () => cache.PutAsync(null, null);
                Func<Task> actR = () => cache.PutAsync(null, null, null);

                // assert
                act.Should().Throw<ArgumentException>()
                    .WithMessage("*Parameter name: key");

                actR.Should().Throw<ArgumentException>()
                    .WithMessage("*Parameter name: key");
            }
        }

        [Fact]
        [ReplaceCulture]
        public void CacheManager_Put_InvalidValue()
        {
            using (var cache = CacheFactory.Build(settings =>
                {
                    settings.WithDictionaryHandle("h1");
                }))
            {
                // arrange act
                Func<Task> act = () => cache.PutAsync("key", null);
                Func<Task> actR = () => cache.PutAsync("key", null, null);

                // assert
                act.Should().Throw<ArgumentException>()
                    .WithMessage("*Parameter name: value");

                actR.Should().Throw<ArgumentException>()
                    .WithMessage("*Parameter name: value");
            }
        }

        [Fact]
        [ReplaceCulture]
        public void CacheManager_Put_InvalidCacheItem()
        {
            using (var cache = CacheFactory.Build(settings =>
                {
                    settings.WithDictionaryHandle("h1");
                }))
            {
                // act
                Func<Task> act = () => cache.PutAsync(null);

                // assert
                act.Should().Throw<ArgumentNullException>()
                    .WithMessage("*Parameter name: item");
            }
        }

        [Fact]
        [ReplaceCulture]
        public void CacheManager_Put_InvalidRegion()
        {
            using (var cache = CacheFactory.Build(settings =>
            {
                settings.WithDictionaryHandle("h1");
            }))
            {
                // act
                Func<Task> act = () => cache.PutAsync("key", "value", null);

                // assert
                act.Should().Throw<ArgumentNullException>()
                    .WithMessage("*Parameter name: region");
            }
        }

        [Theory]
        [InlineData(new[] { 12345 })]
        [InlineData("something")]
        [InlineData(true)]
        [InlineData(0.223f)]
        public async Task CacheManager_Put_CacheItem_Positive<T>(T value)
        {
            using (var cache = CacheFactory.Build(settings =>
                {
                    settings.WithDictionaryHandle("h1");
                }))
            {
                // arrange
                var key = "my key";
                var item = new CacheItem<object>(key, value);
                var itemRegion = new CacheItem<object>(key, "region", value);

                // act
                Func<Task> act = () => cache.PutAsync(item);
                Func<Task> actRegion = () => cache.PutAsync(itemRegion);

                // assert
                act.Should().NotThrow();
                actRegion.Should().NotThrow();
                (await cache.GetAsync(key)).Should().Be(value);
                (await cache.GetAsync(key, "region")).Should().Be(value);
            }
        }

        [Theory]
        [InlineData(12345)]
        [InlineData("something")]
        [InlineData(true)]
        [InlineData(0.223f)]
        public async Task CacheManager_Put_KeyValue_Positive<T>(T value)
        {
            using (var cache = CacheFactory.Build(settings =>
                {
                    settings.WithDictionaryHandle("h1");
                }))
            {
                // arrange
                var key = "my key";

                // act
                Func<Task> act = () => cache.PutAsync(key, value);
                Func<Task> actRegion = () => cache.PutAsync(key, value, "region");

                // assert
                act.Should().NotThrow();
                actRegion.Should().NotThrow();
                (await cache.GetAsync(key)).Should().Be(value);
                (await cache.GetAsync(key, "region")).Should().Be(value);
            }
        }

        #endregion put params validation

        #region update call validation

        [Fact]
        [ReplaceCulture]
        public void CacheManager_Update_InvalidKey()
        {
            using (var cache = CacheFactory.Build(settings =>
            {
                settings.WithDictionaryHandle("h1");
            }))
            {
                // arrange act
                Func<Task> act = () => cache.UpdateAsync(null, null);
                Func<Task> actR = () => cache.UpdateAsync(null, "r", null);
                Func<Task> actU = () => cache.UpdateAsync(null, async (o) => o, 33);
                Func<Task> actRU = () => cache.UpdateAsync(null, null, null, 33);

                object val = null;
                Func<Task> actT = () => cache.TryUpdateAsync(null, null, out val);
                Func<Task> actTR = () => cache.TryUpdateAsync(null, "r", null, out val);
                Func<Task> actTU = () => cache.TryUpdateAsync(null, async (o) => o, 33, out val);
                Func<Task> actTRU = () => cache.TryUpdateAsync(null, null, null, 33, out val);

                // assert
                act.Should().Throw<ArgumentException>()
                    .WithMessage("*Parameter name: key*");

                actR.Should().Throw<ArgumentException>()
                    .WithMessage("*Parameter name: key*");

                actU.Should().Throw<ArgumentException>()
                    .WithMessage("*Parameter name: key*");

                actRU.Should().Throw<ArgumentException>()
                    .WithMessage("*Parameter name: key*");

                actT.Should().Throw<ArgumentException>()
                    .WithMessage("*Parameter name: key*");

                actTR.Should().Throw<ArgumentException>()
                    .WithMessage("*Parameter name: key*");

                actTU.Should().Throw<ArgumentException>()
                    .WithMessage("*Parameter name: key*");

                actTRU.Should().Throw<ArgumentException>()
                    .WithMessage("*Parameter name: key*");
            }
        }

        [Fact]
        [ReplaceCulture]
        public void CacheManager_Update_InvalidUpdateFunc()
        {
            using (var cache = CacheFactory.Build(settings =>
            {
                settings.WithDictionaryHandle("h1");
            }))
            {
                // arrange act
                Func<Task> act = () => cache.UpdateAsync("key", null);
                Func<Task> actR = () => cache.UpdateAsync("key", "region", null);
                Func<Task> actU = () => cache.UpdateAsync("key", null, 33);
                Func<Task> actRU = () => cache.UpdateAsync("key", "region", null, 33);

                object val = null;
                Func<Task> actT = () => cache.TryUpdateAsync("key", null, out val);
                Func<Task> actTR = () => cache.TryUpdateAsync("key", "r", null, out val);
                Func<Task> actTU = () => cache.TryUpdateAsync("key", null, 33, out val);
                Func<Task> actTRU = () => cache.TryUpdateAsync("key", "r", null, 33, out val);

                // assert
                act.Should().Throw<ArgumentException>()
                    .WithMessage("*Parameter name: updateValue*");

                actR.Should().Throw<ArgumentException>()
                    .WithMessage("*Parameter name: updateValue*");

                actU.Should().Throw<ArgumentException>()
                    .WithMessage("*Parameter name: updateValue*");

                actRU.Should().Throw<ArgumentException>()
                    .WithMessage("*Parameter name: updateValue*");

                actT.Should().Throw<ArgumentException>()
                    .WithMessage("*Parameter name: updateValue*");

                actTR.Should().Throw<ArgumentException>()
                    .WithMessage("*Parameter name: updateValue*");

                actTU.Should().Throw<ArgumentException>()
                    .WithMessage("*Parameter name: updateValue*");

                actTRU.Should().Throw<ArgumentException>()
                    .WithMessage("*Parameter name: updateValue*");
            }
        }

        [Fact]
        [ReplaceCulture]
        public void CacheManager_Update_InvalidRegion()
        {
            using (var cache = CacheFactory.Build(settings =>
            {
                settings.WithDictionaryHandle("h1");
            }))
            {
                // arrange act
                Func<Task> actR = () => cache.UpdateAsync("key", null, async a => a);
                Func<Task> actRU = () => cache.UpdateAsync("key", null, async a => a, 33);

                object val = null;
                Func<Task> actTR = () => cache.TryUpdateAsync("key", null, null, out val);
                Func<Task> actTRU = () => cache.TryUpdateAsync("key", null, null, 33, out val);

                // assert
                actR.Should().Throw<ArgumentException>()
                    .WithMessage("*Parameter name: region*");

                actRU.Should().Throw<ArgumentException>()
                    .WithMessage("*Parameter name: region*");

                actTR.Should().Throw<ArgumentException>()
                    .WithMessage("*Parameter name: region*");

                actTRU.Should().Throw<ArgumentException>()
                    .WithMessage("*Parameter name: region*");
            }
        }

        [Fact]
        [ReplaceCulture]
        public void CacheManager_Update_InvalidConfig()
        {
            using (var cache = CacheFactory.Build(settings =>
            {
                settings.WithDictionaryHandle("h1");
            }))
            {
                // arrange act
                Func<Task> act = () => cache.UpdateAsync("key", async a => a, -1);
                Func<Task> actR = () => cache.UpdateAsync("key", "region", async a => a, -1);

                object val = null;
                Func<Task> actTU = () => cache.TryUpdateAsync("key", async a => a, -1, out val);
                Func<Task> actTRU = () => cache.TryUpdateAsync("key", "region", async a => a, -1, out val);

                // assert
                act.Should().Throw<InvalidOperationException>()
                    .WithMessage("*retries must be greater than*");

                actR.Should().Throw<InvalidOperationException>()
                    .WithMessage("*retries must be greater than*");

                actTU.Should().Throw<InvalidOperationException>()
                    .WithMessage("*retries must be greater than*");

                actTRU.Should().Throw<InvalidOperationException>()
                    .WithMessage("*retries must be greater than*");
            }
        }

        [Theory]
        [ClassData(typeof(TestCacheManagers))]
        public async Task CacheManager_Update_ItemNotAdded<T>(T cache)
            where T : ICacheManager<object>
        {
            using (cache)
            {
                // arrange
                var key = Guid.NewGuid().ToString();

                // act
                Func<Task> act = () => cache.UpdateAsync(key, async item => item);
                Func<Task> actR = () => cache.UpdateAsync(key, "region", async item => item);

                object value;
                Func<Task<bool>> act2 = () => cache.TryUpdateAsync(key, async item => item, out value);
                Func<Task<bool>> act2R = () => cache.TryUpdateAsync(key, "region", async item => item, out value);

                // assert
                act.Should().Throw<InvalidOperationException>("*failed*");
                actR.Should().Throw<InvalidOperationException>("*failed*");
                (await act2()).Should().BeFalse("Item has not been added to the cache");
                (await act2R()).Should().BeFalse("Item has not been added to the cache");
            }
        }

        [Theory]
        [ClassData(typeof(TestCacheManagers))]
        public async Task CacheManager_Update_ValueFactoryReturnsNull<T>(T cache)
            where T : ICacheManager<object>
        {
            using (cache)
            {
                // arrange
                var key = Guid.NewGuid().ToString();
                var region = Guid.NewGuid().ToString();

                await cache.AddAsync(key, "value");
                await cache.AddAsync(key, "value", region);

                // act
                Func<Task> act = () => cache.UpdateAsync(key, async (v) => null);
                Func<Task> actR = () => cache.UpdateAsync(key, region, (v) => null);
                Func<Task> actU = () => cache.UpdateAsync(key, async (v) => null, 33);
                Func<Task> actRU = () => cache.UpdateAsync(key, region, (v) => null, 33);

                object val = null;
                Func<Task<bool>> actT = () => cache.TryUpdateAsync(key, async (v) => null, out val);
                Func<Task<bool>> actTR = () => cache.TryUpdateAsync(key, region, async (v) => null, out val);
                Func<Task<bool>> actTU = () => cache.TryUpdateAsync(key, async (v) => null, 33, out val);
                Func<Task<bool>> actTRU = () => cache.TryUpdateAsync(key, region, async (v) => null, 33, out val);

                // assert
                act.Should().Throw<InvalidOperationException>().WithMessage("*value factory returned null*");
                actR.Should().Throw<InvalidOperationException>().WithMessage("*value factory returned null*");
                actU.Should().Throw<InvalidOperationException>().WithMessage("*value factory returned null*");
                actRU.Should().Throw<InvalidOperationException>().WithMessage("*value factory returned null*");

                (await actT()).Should().BeFalse();
                (await actTR()).Should().BeFalse();
                (await actTU()).Should().BeFalse();
                (await actTRU()).Should().BeFalse();
            }
        }

        [Theory]
        [ClassData(typeof(TestCacheManagers))]
        public async Task CacheManager_Update_Simple<T>(T cache)
            where T : ICacheManager<object>
        {
            using (cache)
            {
                // arrange
                var key = Guid.NewGuid().ToString();
                var region = Guid.NewGuid().ToString();
                await cache.AddAsync(key, "something");
                await cache.AddAsync(key, "something", region);

                // act
                Func<Task<object>> act = () => cache.UpdateAsync(key, async item => item + " more");
                Func<Task<object>> actR = () => cache.UpdateAsync(key, region, async item => item + " more");

                object value = string.Empty;
                object value2 = string.Empty;
                Func<Task<bool>> actT = () => cache.TryUpdateAsync(key, async item => item + " awesome", out value);
                Func<Task<bool>> actTR = () => cache.TryUpdateAsync(key, region, async item => item + " awesome", out value2);
                Func<Task<string>> act2 = () => cache.GetAsync<string>(key);

                // assert
                (await act()).Should().Be("something more");
                (await actR()).Should().Be("something more");
                (await actT()).Should().BeTrue();
                (await actTR()).Should().BeTrue();
                value.Should().Be("something more awesome");
                value2.Should().Be("something more awesome");
                (await act2()).Should().Be("something more awesome");
            }
        }

        #endregion update call validation

        #region add or update call validation

        [Fact]
        [ReplaceCulture]
        public void CacheManager_AddOrUpdate_InvalidKey()
        {
            using (var cache = CacheFactory.Build(settings =>
            {
                settings.WithDictionaryHandle("h1");
            }))
            {
                // arrange act
                Func<Task> act = () => cache.AddOrUpdateAsync(null, null, async (o) => o);
                Func<Task> actR = () => cache.AddOrUpdateAsync(null, "r", null, async (o) => o);
                Func<Task> actU = () => cache.AddOrUpdateAsync(null, null, async (o) => o, 33);
                Func<Task> actRU = () => cache.AddOrUpdateAsync(null, "r", null, null, 33);

                // assert
                act.Should().Throw<ArgumentException>()
                    .WithMessage("*Parameter name: key*");

                actR.Should().Throw<ArgumentException>()
                    .WithMessage("*Parameter name: key*");

                actU.Should().Throw<ArgumentException>()
                    .WithMessage("*Parameter name: key*");

                actRU.Should().Throw<ArgumentException>()
                    .WithMessage("*Parameter name: key*");
            }
        }

        [Fact]
        [ReplaceCulture]
        public void CacheManager_AddOrUpdate_InvalidUpdateFunc()
        {
            using (var cache = CacheFactory.Build(settings =>
            {
                settings.WithDictionaryHandle("h1");
            }))
            {
                // arrange act
                Func<Task> act = () => cache.AddOrUpdateAsync("key", "value", null);
                Func<Task> actR = () => cache.AddOrUpdateAsync("key", "region", "value", null);
                Func<Task> actU = () => cache.AddOrUpdateAsync("key", "value", null, 1);
                Func<Task> actRU = () => cache.AddOrUpdateAsync("key", "region", "value", null, 1);
                Func<Task> actI = () => cache.AddOrUpdateAsync(new CacheItem<object>("k", "v"), null);
                Func<Task> actIU = () => cache.AddOrUpdateAsync(new CacheItem<object>("k", "v"), null, 1);

                // assert
                act.Should().Throw<ArgumentException>()
                    .WithMessage("*Parameter name: updateValue*");

                actR.Should().Throw<ArgumentException>()
                    .WithMessage("*Parameter name: updateValue*");

                actU.Should().Throw<ArgumentException>()
                    .WithMessage("*Parameter name: updateValue*");

                actRU.Should().Throw<ArgumentException>()
                    .WithMessage("*Parameter name: updateValue*");
            }
        }

        [Fact]
        [ReplaceCulture]
        public void CacheManager_AddOrUpdate_InvalidRegion()
        {
            using (var cache = CacheFactory.Build(settings =>
            {
                settings.WithDictionaryHandle("h1");
            }))
            {
                // arrange act
                Func<Task> actR = () => cache.AddOrUpdateAsync("key", null, "value", async a => a);
                Func<Task> actRU = () => cache.AddOrUpdateAsync("key", null, "value", async a => a, 1);

                // assert
                actR.Should().Throw<ArgumentException>()
                    .WithMessage("*Parameter name: region*");

                actRU.Should().Throw<ArgumentException>()
                    .WithMessage("*Parameter name: region*");
            }
        }

        [Fact]
        [ReplaceCulture]
        public void CacheManager_AddOrUpdate_InvalidConfig()
        {
            using (var cache = CacheFactory.Build(settings =>
            {
                settings.WithDictionaryHandle("h1");
            }))
            {
                // arrange act
                Func<Task> actU = () => cache.AddOrUpdateAsync("key", "value", async (o) => o, -1);
                Func<Task> actRU = () => cache.AddOrUpdateAsync("key", "region", "value", async (o) => o, -1);
                Func<Task> actIU = () => cache.AddOrUpdateAsync(new CacheItem<object>("k", "v"), async (o) => o, -1);

                // assert
                actU.Should().Throw<InvalidOperationException>()
                    .WithMessage("*retries must be greater than*");

                actRU.Should().Throw<InvalidOperationException>()
                    .WithMessage("*retries must be greater than*");

                actIU.Should().Throw<InvalidOperationException>()
                    .WithMessage("*retries must be greater than*");
            }
        }

        [Theory]
        [ClassData(typeof(TestCacheManagers))]
        public async Task CacheManager_AddOrUpdate_ItemNotAdded<T>(T cache)
            where T : ICacheManager<object>
        {
            using (cache)
            {
                // arrange
                var key = Guid.NewGuid().ToString();
                object value = "value";

                // act
                Func<Task<object>> act = () => cache.AddOrUpdateAsync(key, value, async item => "not this value");

                // assert
                (await act()).Should().Be(value, $"{key} {value} {cache}");

                var addCalls = cache.CacheHandles.Select(h => h.Stats.GetStatistic(CacheStatsCounterType.AddCalls)).Sum();
                addCalls.Should().Be(1, "Item should be added to last handle only");
            }
        }

        [Theory]
        [ClassData(typeof(TestCacheManagers))]
        public async Task CacheManager_AddOrUpdate_Update_Simple<T>(T cache)
            where T : ICacheManager<object>
        {
            using (cache)
            {
                // arrange
                var key = Guid.NewGuid().ToString();
                await cache.AddAsync(key, "something");

                // act
                Func<Task<object>> act = () => cache.AddOrUpdateAsync(key, "does exist", async item =>
                 {
                     item.Should().Be("something");
                     return item + " more";
                 });
                Func<Task<string>> act2 = () => cache.GetAsync<string>(key);

                // assert
                act().Should().Be("something more");
                act2().Should().Be("something more");
            }
        }

        #endregion add or update call validation

        #region get or add

        [Fact]
        [ReplaceCulture]
        public void CacheManager_GetOrAdd_InvalidKey()
        {
            using (var cache = CacheFactory.Build(settings =>
            {
                settings.WithDictionaryHandle("h1");
            }))
            {
                // arrange act
                Func<Task> actA = () => cache.GetOrAddAsync(null, "value");
                Func<Task> actB = () => cache.GetOrAddAsync(null, "region", "value");
                Func<Task> actC = () => cache.GetOrAddAsync(null, async (k) => "value");
                Func<Task> actD = () => cache.GetOrAddAsync(null, "region", async (k, r) => "value");
                Func<Task> actE = () => cache.GetOrAddAsync(null, async (k) => new CacheItem<object>(k, "value"));
                Func<Task> actF = () => cache.GetOrAddAsync(null, "region", async (k, r) => new CacheItem<object>(k, "value"));

                // assert
                actA.Should().Throw<ArgumentException>()
                    .WithMessage("*key*");

                actB.Should().Throw<ArgumentException>()
                    .WithMessage("*key*");

                actC.Should().Throw<ArgumentException>()
                    .WithMessage("*key*");

                actD.Should().Throw<ArgumentException>()
                    .WithMessage("*key*");

                actE.Should().Throw<ArgumentException>()
                    .WithMessage("*key*");

                actF.Should().Throw<ArgumentException>()
                    .WithMessage("*key*");
            }
        }

        [Fact]
        [ReplaceCulture]
        public void CacheManager_TryGetOrAdd_InvalidKey()
        {
            using (var cache = CacheFactory.Build(settings =>
            {
                settings.WithDictionaryHandle("h1");
            }))
            {
                // arrange act
                object val;
                Func<Task> actC = () => cache.TryGetOrAddAsync(null, async (k) => "value", out val);
                Func<Task> actD = () => cache.TryGetOrAddAsync(null, "region", async (k, r) => "value", out val);
                Func<Task> actE = () => cache.TryGetOrAddAsync(null, async (k) => new CacheItem<object>(k, "value"), out val);
                Func<Task> actF = () => cache.TryGetOrAddAsync(null, "region", async (k, r) => new CacheItem<object>(k, "value"), out val);

                // assert
                actC.Should().Throw<ArgumentException>()
                    .WithMessage("*key*");

                actD.Should().Throw<ArgumentException>()
                    .WithMessage("*key*");

                actE.Should().Throw<ArgumentException>()
                    .WithMessage("*key*");

                actF.Should().Throw<ArgumentException>()
                    .WithMessage("*key*");
            }
        }

        [Fact]
        [ReplaceCulture]
        public void CacheManager_GetOrAdd_InvalidRegion()
        {
            using (var cache = CacheFactory.Build(settings =>
            {
                settings.WithDictionaryHandle("h1");
            }))
            {
                // arrange act
                Func<Task> actA = () => cache.GetOrAddAsync("key", " ", "value");
                Func<Task> actB = () => cache.GetOrAddAsync("key", null, async (k, r) => "value");

                // assert
                actA.Should().Throw<ArgumentException>()
                    .WithMessage("*region*");

                actB.Should().Throw<ArgumentException>()
                    .WithMessage("*region*");
            }
        }

        [Fact]
        [ReplaceCulture]
        public void CacheManager_TryGetOrAdd_InvalidRegion()
        {
            using (var cache = CacheFactory.Build(settings =>
            {
                settings.WithDictionaryHandle("h1");
            }))
            {
                // arrange act
                object val;
                Func<Task> actB = () => cache.TryGetOrAddAsync("key", null, async (k, r) => "value", out val);

                // assert
                actB.Should().Throw<ArgumentException>()
                    .WithMessage("*region*");
            }
        }

        [Fact]
        [ReplaceCulture]
        public void CacheManager_GetOrAdd_InvalidFactory()
        {
            using (var cache = CacheFactory.Build(settings =>
            {
                settings.WithDictionaryHandle("h1");
            }))
            {
                // arrange act
                Func<Task> actA = () => cache.GetOrAddAsync("key", null);
                Func<Task> actB = () => cache.GetOrAddAsync("key", "region", null);

                // assert
                actA.Should().Throw<ArgumentException>()
                    .WithMessage("*valueFactory*");

                actB.Should().Throw<ArgumentException>()
                    .WithMessage("*valueFactory*");
            }
        }

        [Fact]
        [ReplaceCulture]
        public void CacheManager_TryGetOrAdd_InvalidFactory()
        {
            using (var cache = CacheFactory.Build(settings =>
            {
                settings.WithDictionaryHandle("h1");
            }))
            {
                // arrange act
                object val;
                Func<Task> actA = () => cache.TryGetOrAddAsync("key", null, out val);
                Func<Task> actB = () => cache.TryGetOrAddAsync("key", "region", null, out val);

                // assert
                actA.Should().Throw<ArgumentException>()
                    .WithMessage("*valueFactory*");

                actB.Should().Throw<ArgumentException>()
                    .WithMessage("*valueFactory*");
            }
        }

        [Theory]
        [ClassData(typeof(TestCacheManagers))]
        public async Task CacheManager_GetOrAdd_SimpleAdd<T>(T cache)
            where T : ICacheManager<object>
        {
            // arrange
            var key = Guid.NewGuid().ToString();
            var keyF = Guid.NewGuid().ToString();
            var keyG = Guid.NewGuid().ToString();
            var region = Guid.NewGuid().ToString();
            var val = Guid.NewGuid().ToString();

            using (cache)
            {
                // act
                await cache.GetOrAddAsync(key, val);
                await cache.GetOrAddAsync(key, region, val);
                await cache.GetOrAddAsync(keyF, async (k) => val);
                await cache.GetOrAddAsync(keyF, region, async (k, r) => val);
                await cache.GetOrAddAsync(keyG, async (k) => new CacheItem<object>(keyG, val));
                await cache.GetOrAddAsync(keyG, region, async (k, r) => new CacheItem<object>(keyG, region, val, ExpirationMode.Absolute, TimeSpan.FromMinutes(42)));

                // assert
                cache[key].Should().Be(val);
                cache[key, region].Should().Be(val);
                cache[keyF].Should().Be(val);
                cache[keyF, region].Should().Be(val);
                cache[keyG].Should().Be(val);
                cache[keyG, region].Should().Be(val);
                var item = await cache.GetCacheItemAsync(keyG, region);
                item.ExpirationMode.Should().Be(ExpirationMode.Absolute);
                item.ExpirationTimeout.Should().Be(TimeSpan.FromMinutes(42));
            }
        }

        [Theory]
        [ClassData(typeof(TestCacheManagers))]
        public async Task CacheManager_TryGetOrAdd_SimpleAdd<T>(T cache)
            where T : ICacheManager<object>
        {
            // arrange
            var key = Guid.NewGuid().ToString();
            var key2 = Guid.NewGuid().ToString();
            var val = Guid.NewGuid().ToString();
            var region = Guid.NewGuid().ToString();
            object valueA = null;
            object valueB = null;
            CacheItem<object> valueC = null;
            CacheItem<object> valueD = null;

            using (cache)
            {
                // act
                Func<Task<bool>> actA = () => cache.TryGetOrAddAsync(key, async k => val, out valueA);
                Func<Task<bool>> actB = () => cache.TryGetOrAddAsync(key, region, async (k, r) => val, out valueB);
                var valC = new CacheItem<object>(key2, val);
                Func<Task<bool>> actC = () => cache.TryGetOrAddAsync(key2, async k => valC, out valueC);
                var valD = new CacheItem<object>(key2, region, val, ExpirationMode.Absolute, TimeSpan.FromMinutes(42));
                Func<Task<bool>> actD = () => cache.TryGetOrAddAsync(key2, region, async (k, r) => valD, out valueD);

                // assert
                (await actA()).Should().BeTrue();
                (await actB()).Should().BeTrue();
                (await actC()).Should().BeTrue();
                (await actD()).Should().BeTrue();
                valueA.Should().Be(val);
                valueB.Should().Be(val);
                valueC.Should().Be(valC);
                valueD.Should().Be(valD);
                cache[key].Should().Be(val);
                cache[key, region].Should().Be(val);
                cache[key2].Should().Be(val);
                cache[key2, region].Should().Be(val);
                var item = await cache.GetCacheItemAsync(key2, region);
                item.ExpirationMode.Should().Be(ExpirationMode.Absolute);
                item.ExpirationTimeout.Should().Be(TimeSpan.FromMinutes(42));
            }
        }

        [Theory]
        [ClassData(typeof(TestCacheManagers))]
        public void CacheManager_GetOrAdd_FactoryReturnsNull<T>(T cache)
            where T : ICacheManager<object>
        {
            // arrange
            var key = Guid.NewGuid().ToString();

            using (cache)
            {
                // act
                Func<Task> act = () => cache.GetOrAddAsync(key, async (k) => null);

                // assert
                act.Should().Throw<InvalidOperationException>("added");
            }
        }

        [Theory]
        [ClassData(typeof(TestCacheManagers))]
        public async Task CacheManager_TryGetOrAdd_FactoryReturnsNull<T>(T cache)
            where T : ICacheManager<object>
        {
            // arrange
            var key = Guid.NewGuid().ToString();

            using (cache)
            {
                // act
                object val = null;
                CacheItem<object> val2 = null;
                Func<Task<bool>> act = () => cache.TryGetOrAddAsync(key, async (k) => null, out val);
                Func<Task<bool>> actB = () => cache.TryGetOrAddAsync(key, async (k) => null, out val2);

                // assert
                (await act()).Should().BeFalse();
                (await actB()).Should().BeFalse();
                val.Should().BeNull();
                val2.Should().BeNull();
            }
        }

        [Theory]
        [ClassData(typeof(TestCacheManagers))]
        public void CacheManager_GetOrAdd_AddNull<T>(T cache)
            where T : ICacheManager<object>
        {
            // arrange
            var key = Guid.NewGuid().ToString();

            using (cache)
            {
                // act
                Action act = () => cache.GetOrAddAsync(key, (object)null);

                // assert
                act.Should().Throw<ArgumentNullException>("added");
            }
        }

        [Theory]
        [ClassData(typeof(TestCacheManagers))]
        public async Task CacheManager_GetOrAdd_SimpleGet<T>(T cache)
            where T : ICacheManager<object>
        {
            // arrange
            var key = Guid.NewGuid().ToString();
            var keyF = Guid.NewGuid().ToString();
            var region = Guid.NewGuid().ToString();
            var val = Guid.NewGuid().ToString();
            Func<string, object> add = (k) => { throw new InvalidOperationException(); };
            Func<string, string, object> addRegion = (k, r) => { throw new InvalidOperationException(); };

            using (cache)
            {
                await cache.AddAsync(key, val);
                await cache.AddAsync(key, val, region);
                await cache.AddAsync(keyF, val);
                await cache.AddAsync(keyF, val, region);

                // act
                var result = await cache.GetOrAddAsync(key, val);
                var resultB = await cache.GetOrAddAsync(key, region, val);
                var resultC = await cache.GetOrAddAsync(key, async (k) => new CacheItem<object>(key, val));
                var resultD = await cache.GetOrAddAsync(key, region, async (k, r) => new CacheItem<object>(key, val));
                Func<Task> act = () => cache.GetOrAddAsync(keyF, add);
                Func<Task> actB = () => cache.GetOrAddAsync(keyF, region, addRegion);

                // assert
                result.Should().Be(val);
                resultB.Should().Be(val);
                resultC.Value.Should().Be(val);
                resultD.Value.Should().Be(val);
                act.Should().NotThrow();
                actB.Should().NotThrow();
            }
        }

        [Theory]
        [ClassData(typeof(TestCacheManagers))]
        public async Task CacheManager_TryGetOrAdd_SimpleGet<T>(T cache)
            where T : ICacheManager<object>
        {
            // arrange
            var key = Guid.NewGuid().ToString();
            var val = Guid.NewGuid().ToString();
            var region = Guid.NewGuid().ToString();

            // the factories should not get invoked because the item exists.
            Func<string, Task<object>> add = async (k) => { throw new InvalidOperationException(); };
            Func<string, string, Task<object>> addRegion = async (k, r) => { throw new InvalidOperationException(); };
            object resultA = null;
            object resultB = null;
            object resultC = null;

            using (cache)
            {
                await cache.AddAsync(key, val);
                await cache.AddAsync(key, val, region);
                var cacheItem = new CacheItem<object>(key, val);
                await cache.AddAsync(cacheItem);

                // act
                Func<Task<bool>> actA = () => cache.TryGetOrAddAsync(key, add, out resultA);
                Func<Task<bool>> actB = () => cache.TryGetOrAddAsync(key, region, addRegion, out resultB);
                Func<Task<bool>> actC = () => cache.TryGetOrAddAsync(key, region, async (k, r) => cacheItem, out resultC);

                // assert
                (await actA()).Should().BeTrue();
                (await actB()).Should().BeTrue();
                (await actC()).Should().BeTrue();
                resultA.Should().Be(val);
                resultB.Should().Be(val);
                resultC.Should().Be(val);
            }
        }

        [Theory()]
        [Trait("category", "Unreliable")]
        [ClassData(typeof(TestCacheManagers))]
        public async Task CacheManager_GetOrAdd_ForceRace<T>(T cache)
            where T : ICacheManager<object>
        {
            // arrange
            var key = Guid.NewGuid().ToString();
            var val = Guid.NewGuid().ToString();
            var counter = 0;
            var runs = 6;

            using (cache)
            {
                Func<Task<CacheItem<object>>> action = async () =>
               {
                   var tries = 0;
                   var created = await cache.GetOrAddAsync(key, async (k) =>
                   {
                       tries++;
                       Interlocked.Increment(ref counter);

                       // force collision so that multiple threads try to add... yea thats long, but parallel should be fine
                       Task.Delay(1).Wait();
                       return new CacheItem<object>(k, counter, ExpirationMode.Absolute, TimeSpan.FromMinutes(tries));
                   });

                   await cache.RemoveAsync(key);
                   return created;
               };

                var tasks = new List<Task<CacheItem<object>>>();
                for (var i = 0; i < runs; i++)
                {
                    tasks.Add(Task.Run(action));
                }

                var results = await Task.WhenAll(tasks.ToArray());

                await Task.Delay(0);

                // tries inside the factory counts how often the factory is being called, then we use that value as timeout
                // should be one as the factory should run only once
                results.Max(p => p.ExpirationTimeout.Minutes).Should().Be(1);

                // even with retries, the factory should not get invoked more than once per call!
                counter.Should().BeLessOrEqualTo(runs);
            }
        }

        [Theory()]
        [Trait("category", "Unreliable")]
        [ClassData(typeof(TestCacheManagers))]
        public async Task CacheManager_TryGetOrAdd_ForceRace<T>(T cache)
            where T : ICacheManager<object>
        {
            // arrange
            var key = Guid.NewGuid().ToString();
            var val = Guid.NewGuid().ToString();
            var counter = 0;
            var runs = 6;

            using (cache)
            {
                Func<Task<CacheItem<object>>> action = async () =>
                {
                    var tries = 0;
                    CacheItem<object> result = null;
                    while (!await cache.TryGetOrAddAsync(
                        key, async (k) =>
                        {
                            tries++;
                            Interlocked.Increment(ref counter);

                            // force collision so that multiple threads try to add... yea thats long, but parallel should be fine
                            Task.Delay(1).Wait();
                            return new CacheItem<object>(k, counter, ExpirationMode.Absolute, TimeSpan.FromMinutes(tries));
                        },
                        out result))
                    { }

                    await cache.RemoveAsync(key);
                    return result;
                };

                var tasks = new List<Task<CacheItem<object>>>();
                for (var i = 0; i < runs; i++)
                {
                    tasks.Add(Task.Run(action));
                }

                var results = await Task.WhenAll(tasks.ToArray());

                await Task.Delay(0);

                // tries inside the factory counts how often the factory is being called, then we use that value as timeout
                // should be one as the factory should run only once
                results.Max(p => p.ExpirationTimeout.Minutes).Should().Be(1);

                // even with retries, the factory should not get invoked more than once per call!
                counter.Should().BeLessOrEqualTo(runs);
            }
        }

        #endregion get or add

        #region Add validation

        [Fact]
        [ReplaceCulture]
        public void CacheManager_Add_InvalidKey()
        {
            // arrange
            using (var cache = CacheFactory.Build(settings =>
                {
                    settings.WithDictionaryHandle("h1");
                }))
            {
                // act
                Func<Task> act = () => cache.AddAsync(null, null);
                Func<Task> actR = () => cache.AddAsync(null, null, null);

                // assert
                act.Should().Throw<ArgumentException>()
                    .WithMessage("*Parameter name: key");

                actR.Should().Throw<ArgumentException>()
                    .WithMessage("*Parameter name: key");
            }
        }

        [Fact]
        [ReplaceCulture]
        public void CacheManager_Add_InvalidValue()
        {
            // arrange
            using (var cache = CacheFactory.Build(
                settings =>
                {
                    settings.WithDictionaryHandle("h1");
                }))
            {
                // act
                Func<Task> act = () => cache.AddAsync("key", null);
                Func<Task> actR = () => cache.AddAsync("key", null, "region");

                // assert
                act.Should().Throw<ArgumentException>()
                    .WithMessage("*Parameter name: value");

                actR.Should().Throw<ArgumentException>()
                    .WithMessage("*Parameter name: value");
            }
        }

        [Fact]
        [ReplaceCulture]
        public void CacheManager_Add_InvalidRegion()
        {
            // arrange
            using (var cache = CacheFactory.Build(
                settings =>
                {
                    settings.WithDictionaryHandle("h1");
                }))
            {
                // act
                Func<Task> actR = () => cache.AddAsync("key", "value", null);

                // assert
                actR.Should().Throw<ArgumentException>()
                    .WithMessage("*Parameter name: region");
            }
        }

        [Fact]
        [ReplaceCulture]
        public void CacheManager_Add_InvalidCacheItem()
        {
            // arrange
            using (var cache = CacheFactory.Build(settings =>
                {
                    settings.WithDictionaryHandle("h1");
                }))
            {
                // act
                Func<Task> act = () => cache.AddAsync(null);

                // assert
                act.Should().Throw<ArgumentNullException>()
                    .WithMessage("*Parameter name: item");
            }
        }

        [Theory]
        [InlineData(12345)]
        [InlineData("something")]
        [InlineData(true)]
        [InlineData(0.223f)]
        public async Task CacheManager_Add_CacheItem_Positive<T>(T value)
        {
            // arrange
            using (var cache = CacheFactory.Build(settings =>
                {
                    settings.WithDictionaryHandle("h1");
                }))
            {
                var key = "my key";
                var item = new CacheItem<object>(key, value);

                // act
                Func<Task> act = () => cache.AddAsync(item);

                // assert
                act.Should().NotThrow();
                (await cache.GetAsync(key)).Should().Be(value);
            }
        }

        [Theory]
        [InlineData(12345)]
        [InlineData("something")]
        [InlineData(true)]
        [InlineData(0.223f)]
        public async Task CacheManager_Add_KeyValue_Positive<T>(T value)
        {
            // arrange
            using (var cache = CacheFactory.Build(settings =>
                {
                    settings.WithDictionaryHandle("h1");
                }))
            {
                var key = "my key";

                // act
                Func<Task> act = () => cache.AddAsync(key, value);

                // assert
                act.Should().NotThrow();
                (await cache.GetAsync(key)).Should().Be(value);
            }
        }

        #endregion Add validation

        #region get validation

        [Fact]
        [ReplaceCulture]
        public void CacheManager_Get_InvalidKey()
        {
            // arrange
            using (var cache = CacheFactory.Build(settings =>
            {
                settings.WithDictionaryHandle("h1");
            }))
            {
                // act
                Func<Task> act = () => cache.GetAsync(null);
                Func<Task> actR = () => cache.GetAsync(null, "region");

                // assert
                act.Should().Throw<ArgumentNullException>()
                    .WithMessage("*Parameter name: key");

                actR.Should().Throw<ArgumentNullException>()
                    .WithMessage("*Parameter name: key");
            }
        }

        [Fact]
        [ReplaceCulture]
        public void CacheManager_Get_InvalidRegion()
        {
            // arrange
            using (var cache = CacheFactory.Build(settings =>
            {
                settings.WithDictionaryHandle("h1");
            }))
            {
                // act
                Func<Task> act = () => cache.GetAsync("key", null);

                // assert
                act.Should().Throw<ArgumentNullException>()
                    .WithMessage("*Parameter name: region");
            }
        }

        [Fact]
        [ReplaceCulture]
        public void CacheManager_GetItem_InvalidKey()
        {
            // arrange
            using (var cache = CacheFactory.Build(settings =>
            {
                settings.WithDictionaryHandle("h1");
            }))
            {
                // act
                Func<Task> act = () => cache.GetCacheItemAsync(null);
                Func<Task> actR = () => cache.GetCacheItemAsync(null, "region");

                // assert
                act.Should().Throw<ArgumentNullException>()
                    .WithMessage("*Parameter name: key");

                actR.Should().Throw<ArgumentNullException>()
                    .WithMessage("*Parameter name: key");
            }
        }

        [Fact]
        [ReplaceCulture]
        public void CacheManager_GetItem_InvalidRegion()
        {
            // arrange
            using (var cache = CacheFactory.Build(settings =>
            {
                settings.WithDictionaryHandle("h1");
            }))
            {
                // act
                Func<Task> act = () => cache.GetCacheItemAsync("key", null);

                // assert
                act.Should().Throw<ArgumentNullException>()
                    .WithMessage("*Parameter name: region");
            }
        }

        [Fact]
        [ReplaceCulture]
        public void CacheManager_GetT_InvalidKey()
        {
            // arrange
            using (var cache = CacheFactory.Build(settings =>
            {
                settings.WithDictionaryHandle("h1");
            }))
            {
                // act
                Func<Task> act = () => cache.GetAsync<string>(null);
                Func<Task> actR = () => cache.GetAsync<string>(null, "region");

                // assert
                act.Should().Throw<ArgumentNullException>()
                    .WithMessage("*Parameter name: key");

                actR.Should().Throw<ArgumentNullException>()
                    .WithMessage("*Parameter name: key");
            }
        }

        [Fact]
        [ReplaceCulture]
        public void CacheManager_GetT_InvalidRegion()
        {
            // arrange
            using (var cache = CacheFactory.Build(settings =>
            {
                settings.WithDictionaryHandle("h1");
            }))
            {
                // act
                Func<Task> act = () => cache.GetAsync<string>("key", null);

                // assert
                act.Should().Throw<ArgumentNullException>()
                    .WithMessage("*Parameter name: region");
            }
        }

        [Fact]
        [ReplaceCulture]
        public async Task CacheManager_Get_KeyNotAvailable()
        {
            // arrange
            using (var cache = CacheFactory.Build(settings =>
                {
                    settings.WithDictionaryHandle("h1");
                }))
            {
                string key = "some key";

                // act
                Func<Task<object>> act = () => cache.GetAsync(key);

                // assert
                (await act()).Should().BeNull("no object added");
            }
        }

        [Fact]
        [ReplaceCulture]
        public async Task CacheManager_GetAdd_Positive()
        {
            // arrange
            using (var cache = CacheFactory.Build(settings =>
                {
                    settings.WithDictionaryHandle("h1");
                }))
            {
                string key = "some key";
                string value = "some value";

                // act
                Func<Task<bool>> actAdd = () => cache.AddAsync(key, value);
                Func<Task<object>> actGet = () => cache.GetAsync(key);

                // assert
                (await actAdd()).Should().BeTrue("the cache should add the key/value");
                (await actGet()).Should()
                    .NotBeNull("object was added")
                    .And.Be(value);
            }
        }

        [Fact]
        [ReplaceCulture]
        public async Task CacheManager_GetCacheItem_Positive()
        {
            // arrange
            using (var cache = CacheFactory.Build(settings =>
            {
                settings.WithDictionaryHandle("h1");
            }))
            {
                string key = "some key";
                string value = "some value";

                // act
                Func<Task<bool>> actAdd = () => cache.AddAsync(key, value);
                Func<Task<CacheItem<object>>> actGet = () => cache.GetCacheItemAsync(key);

                // assert
                (await actAdd()).Should().BeTrue("the cache should add the key/value");
                (await actGet()).Should()
                    .NotBeNull("object was added")
                    .And.Should().BeEquivalentTo(new { Key = key, Value = value }, p => p.ExcludingMissingMembers());
            }
        }

        #endregion get validation

        #region remove

        [Fact]
        [ReplaceCulture]
        public void CacheManager_Remove_InvalidKey()
        {
            // arrange
            using (var cache = CacheFactory.Build(settings =>
                {
                    settings.WithDictionaryHandle("h1");
                }))
            {
                // act
                Func<Task> act = () => cache.RemoveAsync(null);
                Func<Task> actR = () => cache.RemoveAsync(null, "region");

                // assert
                act.Should().Throw<ArgumentException>()
                    .WithMessage("*Parameter name: key");

                actR.Should().Throw<ArgumentException>()
                    .WithMessage("*Parameter name: key");
            }
        }

        [Fact]
        [ReplaceCulture]
        public void CacheManager_Remove_InvalidRegion()
        {
            // arrange
            using (var cache = CacheFactory.Build(settings =>
            {
                settings.WithDictionaryHandle("h1");
            }))
            {
                // act
                Func<Task> act = () => cache.RemoveAsync("key", null);

                // assert
                act.Should().Throw<ArgumentException>()
                    .WithMessage("*Parameter name: region");
            }
        }

        [Fact]
        [ReplaceCulture]
        public void CacheManager_Remove_KeyEmpty()
        {
            // arrange
            using (var cache = CacheFactory.Build(settings =>
                {
                    settings.WithDictionaryHandle("h1");
                }))
            {
                string key = string.Empty;

                // act
                Func<Task> act = () => cache.RemoveAsync(key);

                // assert
                act.Should().Throw<ArgumentException>()
                    .WithMessage("*Parameter name: key");
            }
        }

        [Fact]
        [ReplaceCulture]
        public void CacheManager_Remove_KeyWhiteSpace()
        {
            // arrange
            using (var cache = CacheFactory.Build(settings =>
                {
                    settings.WithDictionaryHandle("h1");
                }))
            {
                string key = "                ";

                // act
                Func<Task> act = () => cache.RemoveAsync(key);

                // assert
                act.Should().Throw<ArgumentException>()
                    .WithMessage("*Parameter name: key");
            }
        }

        [Fact]
        [ReplaceCulture]
        public async Task CacheManager_Remove_KeyNotAvailable()
        {
            // arrange
            using (var cache = CacheFactory.Build(settings =>
                {
                    settings.WithDictionaryHandle("h1");
                }))
            {
                string key = "some key";

                // act
                Func<Task<bool>> act = () => cache.RemoveAsync(key);
                Func<Task<bool>> actR = () => cache.RemoveAsync(key, "region");

                // assert
                (await act()).Should().BeFalse("key should not be present");
                (await actR()).Should().BeFalse("key should not be present");
            }
        }

        [Fact]
        [ReplaceCulture]
        public async Task CacheManager_Remove_Positive()
        {
            // arrange
            using (var cache = CacheFactory.Build(settings =>
                {
                    settings.WithDictionaryHandle("h1");
                }))
            {
                string key = "some key";
                await cache.AddAsync(key, "something"); // add something to be removed

                // act
                var result = await cache.RemoveAsync(key);
                var item = cache[key];

                // assert
                result.Should().BeTrue("key should be present");
                item.Should().BeNull();
            }
        }

        #endregion remove

        #region indexer

        [Fact]
        [ReplaceCulture]
        public void CacheManager_Index_InvalidKey()
        {
            // arrange
            using (var cache = CacheFactory.Build(settings =>
                {
                    settings.WithDictionaryHandle("h1");
                }))
            {
                string key = null;

                // act
                object result;
                Action act = () => result = cache[key];
                Action actR = () => result = cache[key, "region"];

                // assert
                act.Should().Throw<ArgumentNullException>()
                    .WithMessage("*Parameter name: key");

                actR.Should().Throw<ArgumentNullException>()
                    .WithMessage("*Parameter name: key");
            }
        }

        [Fact]
        [ReplaceCulture]
        public void CacheManager_Index_Key_RegionEmpty()
        {
            // arrange
            using (var cache = CacheFactory.Build(settings =>
            {
                settings.WithDictionaryHandle("h1");
            }))
            {
                // act
                object result;
                Action act = () => result = cache["key", string.Empty];

                // assert
                act.Should().Throw<ArgumentException>()
                    .WithMessage("*Parameter name: region");
            }
        }

        [Fact]
        [ReplaceCulture]
        public void CacheManager_Index_KeyNotAvailable()
        {
            // arrange
            using (var cache = CacheFactory.Build(settings =>
                {
                    settings.WithDictionaryHandle("h1");
                }))
            {
                string key = "some key";

                // act
                Func<object> act = () => cache[key];

                // assert
                act().Should().BeNull("no object added for key");
            }
        }

        #endregion indexer

        #region testing empty handle list

        [Fact]
        public void CacheManager_NoCacheHandles()
        {
            // arrange
            // act
            Action act = () => new BaseCacheManager<string>(new CacheManagerConfiguration() { MaxRetries = 1000 });

            // assert
            act.Should().Throw<InvalidOperationException>().WithMessage("*no cache handles*");
        }

        #endregion testing empty handle list

        [Theory]
        [ClassData(typeof(TestCacheManagers))]
        [ReplaceCulture]
        public async Task CacheManager_CastGet_Region<T>(T cache)
            where T : ICacheManager<object>
        {
            using (cache)
            {
                // arrange
                var key = Guid.NewGuid().ToString();
                var region = Guid.NewGuid().ToString();

                // act
                Func<Task<bool>> actA = () => cache.AddAsync(key, "some value", region);
                Func<Task<string>> act = () => cache.GetAsync<string>(key, region);

                // assert
                (await actA()).Should().BeTrue();
                (await act()).Should().Be("some value");
            }
        }

        [Theory]
        [ClassData(typeof(TestCacheManagers))]
        [ReplaceCulture]
        public async Task CacheManager_CastGet<T>(T cache)
            where T : ICacheManager<object>
        {
            using (cache)
            {
                // arrange
                var keys = new List<string>() { Guid.NewGuid().ToString(), Guid.NewGuid().ToString(), Guid.NewGuid().ToString(), Guid.NewGuid().ToString(), Guid.NewGuid().ToString(), Guid.NewGuid().ToString(), Guid.NewGuid().ToString() };
                var values = new List<object>()
                {
                    "string", 33293, 0.123f, 0.324d, 123311L, true,
                    new ComplexType() { Name = "name", SomeBool = false, SomeId = 213 }
                };

                // act
                PopulateCache(cache, keys, values, 1);
                object strSomething = await cache.GetAsync<string>(keys[0]);
                object someNumber = await cache.GetAsync<int>(keys[1]);
                object someFloating = await cache.GetAsync<float>(keys[2]);
                object someDoubling = await cache.GetAsync<double>(keys[3]);
                object someLonging = await cache.GetAsync<long>(keys[4]);
                object someBooling = await cache.GetAsync<bool>(keys[5]);
                object obj = await cache.GetAsync<ComplexType>(keys[6]);
                object someObject = await cache.GetAsync<object>("nonexistent");

                // assert
                ValidateCacheValues(cache, keys, values);
                strSomething.Should().BeEquivalentTo(values[0]);
                someNumber.Should().BeEquivalentTo(values[1]);
                someFloating.Should().BeEquivalentTo(values[2]);
                someDoubling.Should().BeEquivalentTo(values[3]);
                someLonging.Should().BeEquivalentTo(values[4]);
                someBooling.Should().BeEquivalentTo(values[5]);
                obj.Should().BeEquivalentTo(values[6]);
                someObject.Should().Be(null);
            }
        }

        [Theory]
        [ClassData(typeof(TestCacheManagers))]
        [ReplaceCulture]
        public async Task CacheManager_CastGet_ICanHazString<T>(T cache)
            where T : ICacheManager<object>
        {
            using (cache)
            {
                var key = Guid.NewGuid().ToString();

                // arrange
                await cache.AddAsync(key, 123456);

                // act
                var val = await cache.GetAsync<string>(key);

                // assert
                val.Should().Be("123456");
            }
        }

        [Theory]
        [ClassData(typeof(TestCacheManagers))]
        [ReplaceCulture]
        public void CacheManager_SimplePut<T>(T cache)
            where T : ICacheManager<object>
        {
            using (cache)
            {
                // arrange
                var keys = new List<string>() { Guid.NewGuid().ToString(), Guid.NewGuid().ToString(), Guid.NewGuid().ToString() };
                var values = new List<object>() { true, 234, "test string" };

                // act
                Action actPut = () =>
                {
                    PopulateCache(cache, keys, values, 0);
                };

                // assert
                actPut.Should().NotThrow();
                ValidateCacheValues(cache, keys, values);
            }
        }

        [Theory]
        [ClassData(typeof(TestCacheManagers))]
        public void CacheManager_SimpleAdd<T>(T cache)
            where T : ICacheManager<object>
        {
            using (cache)
            {
                // arrange
                var keys = new List<string>() { Guid.NewGuid().ToString(), Guid.NewGuid().ToString(), Guid.NewGuid().ToString() };
                var values = new List<object>() { true, 234, "test string" };

                // act
                Action actSet = () =>
                {
                    PopulateCache(cache, keys, values, 1);
                };

                // assert
                actSet.Should().NotThrow();
                ValidateCacheValues(cache, keys, values);
            }
        }

        [Theory]
        [ClassData(typeof(TestCacheManagers))]
        public void CacheManager_SimpleIndexPut<T>(T cache)
            where T : ICacheManager<object>
        {
            using (cache)
            {
                // arrange
                var keys = new List<string>() { Guid.NewGuid().ToString(), Guid.NewGuid().ToString(), Guid.NewGuid().ToString() };
                var values = new List<object>() { true, 234, "test string" };

                // act
                Action actSet = () =>
                {
                    PopulateCache(cache, keys, values, 2);
                };

                // assert
                actSet.Should().NotThrow();
                ValidateCacheValues(cache, keys, values);
            }
        }

        [Theory]
        [ClassData(typeof(TestCacheManagers))]
        public async Task CacheManager_SimpleRemove<T>(T cache)
            where T : ICacheManager<object>
        {
            using (cache)
            {
                // arrange
                var keys = new List<string>() { Guid.NewGuid().ToString(), Guid.NewGuid().ToString(), Guid.NewGuid().ToString() };
                var values = new List<object>() { true, 234, "test string" };
                var nulls = new List<object>() { null, null, null };

                // act
                PopulateCache(cache, keys, values, 0);

                for (var i = 0; i < keys.Count; i++)
                {
                    await cache.RemoveAsync(keys[i]);
                }

                // assert
                ValidateCacheValues(cache, keys, nulls);
            }
        }

        [Fact]
        public async Task CacheManager_Clear_AllItemsRemoved()
        {
            // arrange act
            using (var cache = TestManagers.WithOneDicCacheHandle)
            {
                // arrange
                var key1 = Guid.NewGuid().ToString();
                var key2 = Guid.NewGuid().ToString();

                // act
                await cache.AddAsync(key1, "value1");
                await cache.AddAsync(key2, "value2");
                await cache.ClearAsync();

                // assert
                cache[key1].Should().BeNull();
                cache[key2].Should().BeNull();
            }
        }

        [Theory]
        [ClassData(typeof(TestCacheManagers))]
        public void CacheManager_SimpleUpdate<T>(T cache)
            where T : ICacheManager<object>
        {
            using (cache)
            {
                if (cache.Configuration.UpdateMode == CacheUpdateMode.None)
                {
                    // skip for none because we want to test the update mode
                    return;
                }

                // arrange
                var keys = new List<string>() { Guid.NewGuid().ToString(), Guid.NewGuid().ToString(), Guid.NewGuid().ToString() };
                var values = new List<object>() { 10, 20, 30 };
                var newValues = new List<object>() { 11, 21, 31 };

                // act
                Func<Task> actSet = async () =>
                {
                    PopulateCache(cache, keys, values, 1);

                    foreach (var key in keys)
                    {
                        var result = await cache.UpdateAsync(key, async item =>
                        {
                            int val = (int)item + 1;
                            return val;
                        });

                        var value = cache.GetAsync(key);
                        value.Should().NotBeNull();
                    }
                };

                // assert
                actSet.Should().NotThrow();
                ValidateCacheValues(cache, keys, newValues);
            }
        }

        [Theory]
        [ClassData(typeof(TestCacheManagers))]
        public async Task CacheManager_IsCaseSensitive_Key<T>(T cache)
            where T : ICacheManager<object>
        {
            var key = "A" + Guid.NewGuid().ToString().ToUpper();
            using (cache)
            {
                await cache.AddAsync(key, "some value");

                var result = await cache.GetAsync(key.ToLower());

                result.Should().BeNull();
            }
        }

        [Theory]
        [ClassData(typeof(TestCacheManagers))]
        public async Task CacheManager_IsCaseSensitive_Region<T>(T cache)
            where T : ICacheManager<object>
        {
            var key = "A" + Guid.NewGuid().ToString().ToUpper();
            var region = "A" + Guid.NewGuid().ToString().ToUpper();
            using (cache)
            {
                await cache.AddAsync(key, "some value", region);

                var result = await cache.GetAsync(key, region.ToLower());

                result.Should().BeNull();
            }
        }

        private static async Task PopulateCache<T>(ICacheManager<T> cache, IList<string> keys, IList<T> values, int mode)
        {
            // let us make this safe per run so cache doesn't get cleared/populated from multiple tests
            using (await asyncLock.LockAsync())
            {
                foreach (var key in keys)
                {
                    await cache.RemoveAsync(key);
                }

                for (int i = 0; i < values.Count; i++)
                {
                    var val = await cache.GetAsync(keys[i]);
                    if (val != null)
                    {
                        throw new InvalidOperationException("cache already contains this element");
                    }

                    if (mode == 0)
                    {
                        await cache.PutAsync(keys[i], values[i]);
                    }
                    else if (mode == 1)
                    {
                        (await cache.AddAsync(keys[i], values[i])).Should().BeTrue();
                    }
                    else if (mode == 2)
                    {
                        cache[keys[i]] = values[i];
                    }
                }
            }
        }

        private static async Task ValidateCacheValues<T>(ICacheManager<T> cache, IList<string> keys, IList<T> values)
        {
            var cacheCfgText = cache.ToString();

            Debug.WriteLine("Validating for cache: " + cacheCfgText);
            values.Select(async (value, index) =>
            {
                var val = await cache.GetAsync(keys[index]);
                val.Should().Be(value, cacheCfgText)
                    .And.Be(cache[keys[index]], cacheCfgText);

                return cache.CacheHandles
                        .All(p =>
                        {
                            (p.GetAsync(keys[index]).GetAwaiter().GetResult())
                                .Should().Be(value, cacheCfgText)
                                .And.Be(p[keys[index]], cacheCfgText);
                            return true;
                        });
            }).ToList();
        }

        [Serializable]
        [ProtoBuf.ProtoContract]
        [Bond.Schema]
        public class ComplexType
        {
            public static ComplexType Create()
            {
                return new ComplexType()
                {
                    Name = Guid.NewGuid().ToString(),
                    SomeId = long.MaxValue,
                    SomeBool = true
                };
            }

            [ProtoBuf.ProtoMember(1)]
            [Bond.Id(1)]
            public string Name { get; set; }

            [ProtoBuf.ProtoMember(2)]
            [Bond.Id(2)]
            public long SomeId { get; set; }

            [ProtoBuf.ProtoMember(3)]
            [Bond.Id(3)]
            public bool SomeBool { get; set; }

            public override bool Equals(object obj)
            {
                var target = obj as ComplexType;
                if (target == null)
                {
                    return false;
                }

                return this.Name.Equals(target.Name) && this.SomeBool.Equals(target.SomeBool) && this.SomeId.Equals(target.SomeId);
            }

            public override int GetHashCode() => base.GetHashCode();
        }
    }
}
