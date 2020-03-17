using System;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CacheManager.Core;
using CacheManager.Core.Internal;
using FluentAssertions;
using Xunit;

namespace CacheManager.RedisAsync.Tests
{
    [ExcludeFromCodeCoverage]
    public class CacheManagerStatsTest
    {
        [Theory]
        [ClassData(typeof(TestCacheManagers))]
        [ReplaceCulture]
        public async Task CacheManager_Stats_AddGet<T>(T cache)
            where T : ICacheManager<object>
        {
            using (cache)
            {
                // arrange
                var key1 = Guid.NewGuid().ToString();
                var key2 = Guid.NewGuid().ToString();
                var addCalls = cache.CacheHandles.Select(p => p.Stats.GetStatistic(CacheStatsCounterType.AddCalls));
                var getCalls = cache.CacheHandles.Select(p => p.Stats.GetStatistic(CacheStatsCounterType.GetCalls));
                var misses = cache.CacheHandles.Select(p => p.Stats.GetStatistic(CacheStatsCounterType.Misses));
                var hits = cache.CacheHandles.Select(p => p.Stats.GetStatistic(CacheStatsCounterType.Hits));
                var items = cache.CacheHandles.Select(p => p.Stats.GetStatistic(CacheStatsCounterType.Items));

                // act get without region, should not return anything and should not trigger the event
                var a1 = await cache.AddAsync(key1, "something");
                var a2 = await cache.AddAsync(key1, "something"); // should not increase adds, but evicts the item from the first handle, so miss +1

                // bot gets should increase first handle +1 and hits +1
                var r1 = await cache.GetAsync(key1);
                var r2 = cache[key1];

                // should increase all handles get + 1 and misses +1
                await cache.GetAsync(key1, Guid.NewGuid().ToString());

                // assert
                a1.Should().BeTrue();
                a2.Should().BeFalse();
                r1.Should().Be("something");
                r2.Should().Be("something");

                // each cache handle stats should have one addCall increase
                var handleCount = cache.CacheHandles.Count();
                if (handleCount > 1)
                {
                    addCalls.Last().Should().Be(1L);
                    addCalls.Take(handleCount - 1).Should().AllBeEquivalentTo(0L);
                }
                else
                {
                    addCalls.Should().AllBeEquivalentTo(1L);
                }

                items.Should().BeEquivalentTo(
                    Enumerable.Repeat(0L, cache.CacheHandles.Count() - 1).Concat(new[] { 1L }));
            }
        }

        [Fact]
        [ReplaceCulture]
        public async Task CacheManager_Stats_Clear()
        {
            using (var cache = TestManagers.WithOneDicCacheHandle)
            {
                // arrange
                var key1 = Guid.NewGuid().ToString();
                var key2 = Guid.NewGuid().ToString();
                var region = Guid.NewGuid().ToString();
                var clears = cache.CacheHandles.Select(p => p.Stats.GetStatistic(CacheStatsCounterType.ClearCalls));
                await cache.AddAsync(key1, "something");
                await cache.AddAsync(key2, "something");

                // act
                await cache.ClearRegionAsync(region); // should not trigger
                await cache.ClearAsync();
                await cache.ClearAsync();

                // assert all handles should have 2 clear increases.
                clears.Should().BeEquivalentTo(
                    Enumerable.Repeat(2L, cache.CacheHandles.Count()));
            }
        }

        [Fact]
        [ReplaceCulture]
        public async Task CacheManager_Stats_ClearRegion()
        {
            using (var cache = TestManagers.WithOneDicCacheHandle)
            {
                // arrange
                var key1 = Guid.NewGuid().ToString();
                var key2 = Guid.NewGuid().ToString();
                var region = Guid.NewGuid().ToString();
                var clears = cache.CacheHandles.Select(p => p.Stats.GetStatistic(CacheStatsCounterType.ClearRegionCalls));
                await cache.AddAsync(key1, "something");
                await cache.AddAsync(key2, "something");
                await cache.AddAsync(key2, "something", region);

                // act
                await cache.ClearRegionAsync(region);
                await cache.ClearAsync();  // should not trigger
                await cache.ClearRegionAsync(Guid.NewGuid().ToString());

                // assert all handles should have 2 clearRegion increases.
                clears.Should().BeEquivalentTo(
                    Enumerable.Repeat(2L, cache.CacheHandles.Count()));
            }
        }

        [Theory]
        [ClassData(typeof(TestCacheManagers))]
        [ReplaceCulture]
        public async Task CacheManager_Stats_Put<T>(T cache)
            where T : ICacheManager<object>
        {
            using (cache)
            {
                // arrange
                var key1 = Guid.NewGuid().ToString();
                var key2 = Guid.NewGuid().ToString();
                var region = Guid.NewGuid().ToString();
                var puts = cache.CacheHandles.Select(p => p.Stats.GetStatistic(CacheStatsCounterType.PutCalls));

                // act
                await cache.PutAsync(key1, "something");
                await cache.PutAsync(key2, "something");
                await cache.PutAsync(key2, "something", region);

                // assert all handles should have 2 clearRegion increases.
                puts.Should().BeEquivalentTo(
                    Enumerable.Repeat(3L, cache.CacheHandles.Count()));
            }
        }

        [Theory]
        [ClassData(typeof(TestCacheManagers))]
        [ReplaceCulture]
        public async Task CacheManager_Stats_Update<T>(T cache)
            where T : ICacheManager<object>
        {
            using (cache)
            {
                // arrange
                var key1 = Guid.NewGuid().ToString();
                var key2 = Guid.NewGuid().ToString();
                var adds = cache.CacheHandles.Select(p => p.Stats.GetStatistic(CacheStatsCounterType.AddCalls));
                var gets = cache.CacheHandles.Select(p => p.Stats.GetStatistic(CacheStatsCounterType.GetCalls));
                var hits = cache.CacheHandles.Select(p => p.Stats.GetStatistic(CacheStatsCounterType.Hits));
                await cache.AddAsync(key1, "something");
                await cache.AddAsync(key2, "something");

                // act
                await cache.UpdateAsync(key1, async v => "somethingelse");
                await cache.UpdateAsync(key2, async v => "somethingelse");

                // assert could be more than 2 adds.Should().AllBeEquivalentTo( Enumerable.Repeat(0,
                // cache.CacheHandles.Count)); gets.Should().AllBeEquivalentTo( Enumerable.Repeat(2,
                // cache.CacheHandles.Count)); hits.Should().AllBeEquivalentTo( Enumerable.Repeat(2, cache.CacheHandles.Count));
            }
        }

        [Theory]
        [ClassData(typeof(TestCacheManagers))]
        [ReplaceCulture]
        public async Task CacheManager_Stats_Remove<T>(T cache)
            where T : ICacheManager<object>
        {
            using (cache)
            {
                // arrange
                var key1 = Guid.NewGuid().ToString();
                var key2 = Guid.NewGuid().ToString();
                var region = Guid.NewGuid().ToString();
                var adds = cache.CacheHandles.Select(p => p.Stats.GetStatistic(CacheStatsCounterType.AddCalls));
                var removes = cache.CacheHandles.Select(p => p.Stats.GetStatistic(CacheStatsCounterType.RemoveCalls));

                // act
                var r1 = await cache.RemoveAsync(key2);               // false
                var r2 = await cache.RemoveAsync(key2, region);        // false

                var a1 = await cache.AddAsync(key1, "something");            // true
                var a2 = await cache.AddAsync(key2, "something");            // true
                var a3 = await cache.AddAsync(key2, "something", region);    // true
                var a4 = await cache.AddAsync(key1, "something");            // false
                var r3 = await cache.RemoveAsync(key2);                      // true
                var r4 = await cache.RemoveAsync(key2, region);              // true
                var a5 = await cache.AddAsync(key2, "something");            // true
                var a6 = await cache.AddAsync(key2, "something", region);    // true

                // assert
                (r1 && r2).Should().BeFalse();
                (r3 && r4).Should().BeTrue();
                a4.Should().BeFalse();
                (a1 && a2 && a3 && a5 && a6).Should().BeTrue();

                // all handles should have 5 add increases.
                if (adds.Count() > 1)
                {
                    adds.Last().Should().Be(5);
                    adds.Take(adds.Count() - 1).Should().AllBeEquivalentTo(0);
                }
                else
                {
                    adds.Last().Should().Be(5);
                }
            }
        }

        [Theory]
        [ClassData(typeof(TestCacheManagers))]
        [ReplaceCulture]
        [Trait("category", "Unreliable")]
        public async Task CacheManager_Stats_Threaded<T>(T cache)
            where T : ICacheManager<object>
        {
            var puts = cache.CacheHandles.Select(p => p.Stats.GetStatistic(CacheStatsCounterType.PutCalls));
            var adds = cache.CacheHandles.Select(p => p.Stats.GetStatistic(CacheStatsCounterType.AddCalls));
            var threads = 4;
            var iterations = 10;
            var putCounter = 0;

            using (cache)
            {
                var key = Guid.NewGuid().ToString();
                await ThreadTestHelper.RunAsync(
                    async () =>
                    {
                        await cache.AddAsync(key, "hi");
                        await cache.PutAsync(key, "changed");
                        Interlocked.Increment(ref putCounter);
                        await Task.Delay(0);
                    },
                    threads,
                    iterations);
            }

            await Task.Delay(20);
            putCounter.Should().Be(threads * iterations);
            puts.Should().BeEquivalentTo(
                    Enumerable.Repeat((long)(threads * iterations), cache.CacheHandles.Count()));
        }
    }
}