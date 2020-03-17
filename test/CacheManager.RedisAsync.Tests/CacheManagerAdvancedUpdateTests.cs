using System;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading.Tasks;
using CacheManager.Core;
using CacheManager.Core.Internal;
using CacheManager.Core.Logging;
using FluentAssertions;
using Xunit;

namespace CacheManager.RedisAsync.Tests
{
    [ExcludeFromCodeCoverage]
    public class CacheManagerAdvancedUpdateTests
    {
        [Theory]
        [ClassData(typeof(TestCacheManagers))]
        public async Task Update_ThrowsIf_FactoryReturnsNull(ICacheManager<object> cache)
        {
            using (cache)
            {
                var key = Guid.NewGuid().ToString();
                await cache.AddAsync(key, "value");
                Func<Task> act = () => cache.UpdateAsync(key, async v => null);
                act.Should().Throw<InvalidOperationException>("factory");
            }
        }

        [Fact]
        [ReplaceCulture]
        public void UpdateItemResult_ForSuccess()
        {
            // arrange act
            var item = new CacheItem<object>("key", new object());
            Func<UpdateItemResult<object>> act = () => UpdateItemResult.ForSuccess<object>(item, true, 1001);

            // assert
            act().Should().BeEquivalentTo(new { Value = item, UpdateState = UpdateItemResultState.Success, NumberOfTriesNeeded = 1001, VersionConflictOccurred = true });
        }

        [Fact]
        [ReplaceCulture]
        public void UpdateItemResult_ForTooManyTries()
        {
            // arrange act
            Func<UpdateItemResult<object>> act = () => UpdateItemResult.ForTooManyRetries<object>(1001);

            // assert
            act().Should().BeEquivalentTo(new { Value = default(object), UpdateState = UpdateItemResultState.TooManyRetries, NumberOfTriesNeeded = 1001, VersionConflictOccurred = true });
        }

        [Fact]
        [ReplaceCulture]
        public void UpdateItemResult_ForDidNotExist()
        {
            // arrange act
            Func<UpdateItemResult<object>> act = () => UpdateItemResult.ForItemDidNotExist<object>();

            // assert
            act().Should().BeEquivalentTo(new { Value = default(object), UpdateState = UpdateItemResultState.ItemDidNotExist, NumberOfTriesNeeded = 1, VersionConflictOccurred = false });
        }

        [Fact]
        [ReplaceCulture]
        public void UpdateItemResult_ForFactoryReturnsNull()
        {
            // arrange act
            Func<UpdateItemResult<object>> act = () => UpdateItemResult.ForFactoryReturnedNull<object>();

            // assert
            act().Should().BeEquivalentTo(new { Value = default(object), UpdateState = UpdateItemResultState.FactoryReturnedNull, NumberOfTriesNeeded = 1, VersionConflictOccurred = false });
        }

        [Fact]
        public async Task CacheManager_Update_Validate_LowestWins()
        {
            // arrange
            Func<string, Task<string>> updateFunc = async s => s;
            int updateCalls = 0;
            int putCalls = 0;
            int removeCalls = 0;

            var cache = MockHandles(
                count: 5,
                updateCalls: Enumerable.Repeat<Action>(() => updateCalls++, 5).ToArray(),
                updateCallResults: new UpdateItemResult<string>[]
                {
                    null,
                    null,
                    null,
                    null,
                    UpdateItemResult.ForSuccess<string>(new CacheItem<string>("key", string.Empty), true, 100)
                },
                putCalls: Enumerable.Repeat<Action>(() => putCalls++, 5).ToArray(),
                removeCalls: Enumerable.Repeat<Action>(() => removeCalls++, 5).ToArray());

            // act
            using (cache)
            {
                string value;
                var updateResult = await cache.TryUpdateAsync("key", updateFunc, 1, out value);

                // assert
                updateCalls.Should().Be(1, "first handle should have been invoked");
                putCalls.Should().Be(0, "evicted");
                removeCalls.Should().Be(4, "items should have been removed");
                updateResult.Should().BeTrue();
            }
        }

        [Fact]
        public async Task CacheManager_Update_ItemDoesNotExist()
        {
            // arrange
            Func<string, Task<string>> updateFunc = async s => s;
            int updateCalls = 0;
            int putCalls = 0;
            int removeCalls = 0;

            var cache = MockHandles(
                count: 5,
                updateCalls: Enumerable.Repeat<Action>(() => updateCalls++, 5).ToArray(),
                updateCallResults: new UpdateItemResult<string>[]
                {
                    UpdateItemResult.ForItemDidNotExist<string>(),
                    UpdateItemResult.ForItemDidNotExist<string>(),
                    UpdateItemResult.ForItemDidNotExist<string>(),
                    UpdateItemResult.ForItemDidNotExist<string>(),
                    UpdateItemResult.ForItemDidNotExist<string>()
                },
                putCalls: Enumerable.Repeat<Action>(() => putCalls++, 5).ToArray(),
                removeCalls: Enumerable.Repeat<Action>(() => removeCalls++, 5).ToArray());

            // act
            using (cache)
            {
                string value;
                var updateResult = await cache.TryUpdateAsync("key", updateFunc, 1, out value);

                // assert
                updateCalls.Should().Be(1, "should exit after the first item did not exist");
                putCalls.Should().Be(0, "no put calls expected");
                removeCalls.Should().Be(4, "item should be removed from others");
                updateResult.Should().BeFalse();
            }
        }

        [Fact]
        public async Task CacheManager_Update_ExceededRetryLimit()
        {
            // arrange
            Func<string, Task<string>> updateFunc = async s => s;
            int updateCalls = 0;
            int putCalls = 0;
            int removeCalls = 0;

            var cache = MockHandles(
                count: 5,
                updateCalls: Enumerable.Repeat<Action>(() => updateCalls++, 5).ToArray(),
                updateCallResults: new UpdateItemResult<string>[]
                {
                    UpdateItemResult.ForSuccess<string>(new CacheItem<string>("key", string.Empty), true, 100),
                    UpdateItemResult.ForSuccess<string>(new CacheItem<string>("key", string.Empty), true, 100),
                    UpdateItemResult.ForSuccess<string>(new CacheItem<string>("key", string.Empty), true, 100),
                    UpdateItemResult.ForTooManyRetries<string>(1000),
                    UpdateItemResult.ForItemDidNotExist<string>(),
                },
                putCalls: Enumerable.Repeat<Action>(() => putCalls++, 5).ToArray(),
                removeCalls: Enumerable.Repeat<Action>(() => removeCalls++, 5).ToArray());

            // act
            using (cache)
            {
                string value;
                var updateResult = await cache.TryUpdateAsync("key", updateFunc, 1, out value);

                // assert
                updateCalls.Should().Be(1, "failed because item did not exist");
                putCalls.Should().Be(0, "no put calls expected");
                removeCalls.Should().Be(4, "the key should have been removed from the other 4 handles");
                updateResult.Should().BeFalse("the update in handle 4 was not successful.");
            }
        }

        [Fact]
        public async Task CacheManager_Update_Success_ValidateEvict()
        {
            // arrange
            Func<string, Task<string>> updateFunc = async s => s;

            int updateCalls = 0;
            int addCalls = 0;
            int putCalls = 0;
            int removeCalls = 0;

            var cache = MockHandles(
                count: 5,
                updateCalls: Enumerable.Repeat<Action>(() => updateCalls++, 5).ToArray(),
                updateCallResults: new UpdateItemResult<string>[]
                {
                    UpdateItemResult.ForItemDidNotExist<string>(),
                    UpdateItemResult.ForItemDidNotExist<string>(),
                    UpdateItemResult.ForItemDidNotExist<string>(),
                    UpdateItemResult.ForItemDidNotExist<string>(),
                    UpdateItemResult.ForSuccess(new CacheItem<string>("key", "some value"), true, 100)
                },
                putCalls: Enumerable.Repeat<Action>(() => putCalls++, 5).ToArray(),
                removeCalls: Enumerable.Repeat<Action>(() => removeCalls++, 5).ToArray(),
                getCallValues: new CacheItem<string>[]
                {
                    null,
                    null,
                    null,
                    new CacheItem<string>("key", "updated value"),  // have to return an item for the second one
                    null
                },
                addCalls: Enumerable.Repeat<Func<bool>>(() => { addCalls++; return true; }, 5).ToArray());

            // act
            using (cache)
            {
                string value;
                var updateResult = await cache.TryUpdateAsync("key", updateFunc, 1, out value);

                // assert
                updateCalls.Should().Be(1, "first succeeds second fails");
                putCalls.Should().Be(0, "no puts");
                addCalls.Should().Be(0, "no adds");
                removeCalls.Should().Be(4, "should remove from all others");
                updateResult.Should().BeTrue("updated successfully.");
            }
        }

        private static ICacheManager<string> MockHandles(int count, Action[] updateCalls, UpdateItemResult<string>[] updateCallResults, Action[] putCalls, Action[] removeCalls, CacheItem<string>[] getCallValues = null, Func<bool>[] addCalls = null)
        {
            if (count <= 0)
            {
                throw new InvalidOperationException();
            }

            if (updateCalls.Length != count || updateCallResults.Length != count || putCalls.Length != count || removeCalls.Length != count)
            {
                throw new InvalidOperationException("Count and arrays must match");
            }

            var manager = CacheFactory.Build<string>(
                settings =>
                {
                    for (int i = 0; i < count; i++)
                    {
                        settings
                            .WithHandle(typeof(MockCacheHandle<>), "handle" + i)
                            .EnableStatistics();
                    }
                });

            for (var i = 0; i < count; i++)
            {
                var handle = manager.CacheHandles.ElementAt(i) as MockCacheHandle<string>;
                handle.GetCallValue = getCallValues == null ? null : getCallValues[i];
                if (putCalls != null)
                {
                    handle.PutCall = putCalls[i];
                }
                if (addCalls != null)
                {
                    handle.AddCall = addCalls[i];
                }
                if (removeCalls != null)
                {
                    handle.RemoveCall = removeCalls[i];
                }
                if (updateCalls != null)
                {
                    handle.UpdateCall = updateCalls[i];
                }
                if (updateCallResults != null)
                {
                    handle.UpdateValue = updateCallResults[i];
                }
            }

            return manager;
        }
    }

    [ExcludeFromCodeCoverage]
    public class MockCacheHandle<TCacheValue> : BaseCacheHandle<TCacheValue>
    {
        public MockCacheHandle(CacheManagerConfiguration managerConfiguration, CacheHandleConfiguration configuration, ILoggerFactory loggerFactory)
            : base(managerConfiguration, configuration)
        {
            this.Logger = loggerFactory.CreateLogger(this);
            this.AddCall = () => true;
            this.PutCall = () => { };
            this.RemoveCall = () => { };
            this.UpdateCall = () => { };
        }

        public CacheItem<TCacheValue> GetCallValue { get; set; }

        public Func<bool> AddCall { get; set; }

        public Action PutCall { get; set; }

        public Action RemoveCall { get; set; }

        public Action UpdateCall { get; set; }

        public UpdateItemResult<TCacheValue> UpdateValue { get; set; }

        public override async Task<int> CountAsync() => 0;

        protected override ILogger Logger { get; }

        public override async Task ClearAsync()
        {
        }

        public override async Task ClearRegionAsync(string region)
        {
        }

        public override async Task<UpdateItemResult<TCacheValue>> UpdateAsync(string key, Func<TCacheValue, Task<TCacheValue>> updateValue, int maxRetries)
        {
            this.UpdateCall();
            return this.UpdateValue;
        }

        public override async Task<UpdateItemResult<TCacheValue>> UpdateAsync(string key, string region, Func<TCacheValue, Task<TCacheValue>> updateValue, int maxRetries)
        {
            this.UpdateCall();
            return this.UpdateValue;
        }

        public override async Task<bool> ExistsAsync(string key)
        {
            return false;
        }

        public override async Task<bool> ExistsAsync(string key, string region)
        {
            return false;
        }

        protected override async Task<bool> AddInternalPreparedAsync(CacheItem<TCacheValue> item) => this.AddCall();

        protected override async Task<CacheItem<TCacheValue>> GetCacheItemInternalAsync(string key) => this.GetCallValue;

        protected override async Task<CacheItem<TCacheValue>> GetCacheItemInternalAsync(string key, string region) => this.GetCallValue;

        protected override async Task PutInternalPreparedAsync(CacheItem<TCacheValue> item) => this.PutCall();

        protected override async Task<bool> RemoveInternalAsync(string key)
        {
            this.RemoveCall();
            return true;
        }

        protected override async Task<bool> RemoveInternalAsync(string key, string region)
        {
            this.RemoveCall();
            return true;
        }
    }
}
