using System;
using System.Linq;
using System.Threading.Tasks;
using CacheManager.Core.Internal;
using CacheManager.Core.Logging;
using static CacheManager.Core.Utility.Guard;

namespace CacheManager.Core
{
    public partial class BaseCacheManager<TCacheValue>
    {
        /// <inheritdoc />
        public Task<TCacheValue> AddOrUpdateAsync(string key, TCacheValue addValue, Func<TCacheValue, Task<TCacheValue>> updateValue) =>
            AddOrUpdateAsync(key, addValue, updateValue, Configuration.MaxRetries);

        /// <inheritdoc />
        public Task<TCacheValue> AddOrUpdateAsync(string key, string region, TCacheValue addValue, Func<TCacheValue, Task<TCacheValue>> updateValue) =>
            AddOrUpdateAsync(key, region, addValue, updateValue, Configuration.MaxRetries);

        /// <inheritdoc />
        public Task<TCacheValue> AddOrUpdateAsync(string key, TCacheValue addValue, Func<TCacheValue, Task<TCacheValue>> updateValue, int maxRetries) =>
            AddOrUpdateAsync(new CacheItem<TCacheValue>(key, addValue), updateValue, maxRetries);

        /// <inheritdoc />
        public Task<TCacheValue> AddOrUpdateAsync(string key, string region, TCacheValue addValue, Func<TCacheValue, Task<TCacheValue>> updateValue, int maxRetries) =>
            AddOrUpdateAsync(new CacheItem<TCacheValue>(key, region, addValue), updateValue, maxRetries);

        /// <inheritdoc />
        public Task<TCacheValue> AddOrUpdateAsync(CacheItem<TCacheValue> addItem, Func<TCacheValue, Task<TCacheValue>> updateValue) =>
            AddOrUpdateAsync(addItem, updateValue, Configuration.MaxRetries);

        /// <inheritdoc />
        public Task<TCacheValue> AddOrUpdateAsync(CacheItem<TCacheValue> addItem, Func<TCacheValue, Task<TCacheValue>> updateValue, int maxRetries)
        {
            NotNull(addItem, nameof(addItem));
            NotNull(updateValue, nameof(updateValue));
            Ensure(maxRetries >= 0, "Maximum number of retries must be greater than or equal to zero.");

            return AddOrUpdateInternalAsync(addItem, updateValue, maxRetries);
        }

        private async Task<TCacheValue> AddOrUpdateInternalAsync(CacheItem<TCacheValue> item, Func<TCacheValue, Task<TCacheValue>> updateValue, int maxRetries)
        {
            CheckDisposed();
            if (_logTrace)
            {
                Logger.LogTrace("Add or update: {0} {1}.", item.Key, item.Region);
            }

            var tries = 0;
            do
            {
                tries++;

                if (await AddInternalAsync(item))
                {
                    if (_logTrace)
                    {
                        Logger.LogTrace("Add or update: {0} {1}: successfully added the item.", item.Key, item.Region);
                    }

                    return item.Value;
                }

                if (_logTrace)
                {
                    Logger.LogTrace(
                        "Add or update: {0} {1}: add failed, trying to update...",
                        item.Key,
                        item.Region);
                }

                TCacheValue returnValue;
                var updated = string.IsNullOrWhiteSpace(item.Region) ?
                    await TryUpdateAsync(item.Key, updateValue, maxRetries, out returnValue) :
                    await TryUpdateAsync(item.Key, item.Region, updateValue, maxRetries, out returnValue);

                if (updated)
                {
                    if (_logTrace)
                    {
                        Logger.LogTrace("Add or update: {0} {1}: successfully updated.", item.Key, item.Region);
                    }

                    return returnValue;
                }

                if (_logTrace)
                {
                    Logger.LogTrace(
                        "Add or update: {0} {1}: update FAILED, retrying [{2}/{3}].",
                        item.Key,
                        item.Region,
                        tries,
                        Configuration.MaxRetries);
                }
            }
            while (tries <= maxRetries);

            // exceeded max retries, failing the operation... (should not happen in 99,99% of the cases though, better throw?)
            return default(TCacheValue);
        }

        /// <inheritdoc />
        public Task<bool> TryUpdateAsync(string key, Func<TCacheValue, Task<TCacheValue>> updateValue, out TCacheValue value) =>
            TryUpdateAsync(key, updateValue, Configuration.MaxRetries, out value);

        /// <inheritdoc />
        public Task<bool> TryUpdateAsync(string key, string region, Func<TCacheValue, Task<TCacheValue>> updateValue, out TCacheValue value) =>
            TryUpdateAsync(key, region, updateValue, Configuration.MaxRetries, out value);

        /// <inheritdoc />
        public Task<bool> TryUpdateAsync(string key, Func<TCacheValue, Task<TCacheValue>> updateValue, int maxRetries, out TCacheValue value)
        {
            NotNullOrWhiteSpace(key, nameof(key));
            NotNull(updateValue, nameof(updateValue));
            Ensure(maxRetries >= 0, "Maximum number of retries must be greater than or equal to zero.");

            return UpdateInternalAsync(_cacheHandles, key, updateValue, maxRetries, false, out value);
        }

        /// <inheritdoc />
        public Task<bool> TryUpdateAsync(string key, string region, Func<TCacheValue, Task<TCacheValue>> updateValue, int maxRetries, out TCacheValue value)
        {
            NotNullOrWhiteSpace(key, nameof(key));
            NotNullOrWhiteSpace(region, nameof(region));
            NotNull(updateValue, nameof(updateValue));
            Ensure(maxRetries >= 0, "Maximum number of retries must be greater than or equal to zero.");

            return UpdateInternalAsync(_cacheHandles, key, region, updateValue, maxRetries, false, out value);
        }

        /// <inheritdoc />
        public Task<TCacheValue> UpdateAsync(string key, Func<TCacheValue, Task<TCacheValue>> updateValue) =>
            UpdateAsync(key, updateValue, Configuration.MaxRetries);

        /// <inheritdoc />
        public Task<TCacheValue> UpdateAsync(string key, string region, Func<TCacheValue, Task<TCacheValue>> updateValue) =>
            UpdateAsync(key, region, updateValue, Configuration.MaxRetries);

        /// <inheritdoc />
        public async Task<TCacheValue> UpdateAsync(string key, Func<TCacheValue, Task<TCacheValue>> updateValue, int maxRetries)
        {
            NotNullOrWhiteSpace(key, nameof(key));
            NotNull(updateValue, nameof(updateValue));
            Ensure(maxRetries >= 0, "Maximum number of retries must be greater than or equal to zero.");

            var value = default(TCacheValue);
            await UpdateInternalAsync(_cacheHandles, key, updateValue, maxRetries, true, out value);

            return value;
        }

        /// <inheritdoc />
        public async Task<TCacheValue> UpdateAsync(string key, string region, Func<TCacheValue, Task<TCacheValue>> updateValue, int maxRetries)
        {
            NotNullOrWhiteSpace(key, nameof(key));
            NotNullOrWhiteSpace(region, nameof(region));
            NotNull(updateValue, nameof(updateValue));
            Ensure(maxRetries >= 0, "Maximum number of retries must be greater than or equal to zero.");

            var value = default(TCacheValue);
            await UpdateInternalAsync(_cacheHandles, key, region, updateValue, maxRetries, true, out value);

            return value;
        }

        private Task<bool> UpdateInternalAsync(
            BaseCacheHandle<TCacheValue>[] handles,
            string key,
            Func<TCacheValue, Task<TCacheValue>> updateValue,
            int maxRetries,
            bool throwOnFailure,
            out TCacheValue value) =>
            UpdateInternalAsync(handles, key, null, updateValue, maxRetries, throwOnFailure, out value);

        private Task<bool> UpdateInternalAsync(
            BaseCacheHandle<TCacheValue>[] handles,
            string key,
            string region,
            Func<TCacheValue, Task<TCacheValue>> updateValue,
            int maxRetries,
            bool throwOnFailure,
            out TCacheValue value)
        {
            CheckDisposed();

            async Task<Tuple<bool, TCacheValue>> LocalFunc()
            {
                // assign null
                TCacheValue outValue = default(TCacheValue);

                if (handles.Length == 0)
                {
                    return Tuple.Create(false, outValue);
                }

                if (_logTrace)
                {
                    Logger.LogTrace("Update: {0} {1}.", key, region);
                }

                // lowest level
                // todo: maybe check for only run on the backplate if configured (could potentially be not the last one).
                var handleIndex = handles.Length - 1;
                var handle = handles[handleIndex];

                var result = string.IsNullOrWhiteSpace(region) ?
                    await handle.UpdateAsync(key, updateValue, maxRetries) :
                    await handle.UpdateAsync(key, region, updateValue, maxRetries);

                if (_logTrace)
                {
                    Logger.LogTrace(
                        "Update: {0} {1}: tried on handle {2}: result: {3}.",
                        key,
                        region,
                        handle.Configuration.Name,
                        result.UpdateState);
                }

                if (result.UpdateState == UpdateItemResultState.Success)
                {
                    // only on success, the returned value will not be null
                    outValue = result.Value.Value;
                    handle.Stats.OnUpdate(key, region, result);

                    // evict others, we don't know if the update on other handles could actually
                    // succeed... There is a risk the update on other handles could create a
                    // different version than we created with the first successful update... we can
                    // safely add the item to handles below us though.
                    await EvictFromHandlesAboveAsync(key, region, handleIndex);

                    // optimizing - not getting the item again from cache. We already have it
                    // var item = string.IsNullOrWhiteSpace(region) ? handle.GetCacheItem(key) : handle.GetCacheItem(key, region);
                    await AddToHandlesBelowAsync(result.Value, handleIndex);
                    TriggerOnUpdate(key, region);
                }
                else if (result.UpdateState == UpdateItemResultState.FactoryReturnedNull)
                {
                    Logger.LogWarn($"Update failed on '{region}:{key}' because value factory returned null.");

                    if (throwOnFailure)
                    {
                        throw new InvalidOperationException($"Update failed on '{region}:{key}' because value factory returned null.");
                    }
                }
                else if (result.UpdateState == UpdateItemResultState.TooManyRetries)
                {
                    // if we had too many retries, this basically indicates an
                    // invalid state of the cache: The item is there, but we couldn't update it and
                    // it most likely has a different version
                    Logger.LogWarn($"Update failed on '{region}:{key}' because of too many retries.");

                    await EvictFromOtherHandlesAsync(key, region, handleIndex);

                    if (throwOnFailure)
                    {
                        throw new InvalidOperationException($"Update failed on '{region}:{key}' because of too many retries: {result.NumberOfTriesNeeded}.");
                    }
                }
                else if (result.UpdateState == UpdateItemResultState.ItemDidNotExist)
                {
                    // If update fails because item doesn't exist AND the current handle is backplane source or the lowest cache handle level,
                    // remove the item from other handles (if exists).
                    // Otherwise, if we do not exit here, calling update on the next handle might succeed and would return a misleading result.
                    Logger.LogInfo($"Update failed on '{region}:{key}' because the region/key did not exist.");

                    await EvictFromOtherHandlesAsync(key, region, handleIndex);

                    if (throwOnFailure)
                    {
                        throw new InvalidOperationException($"Update failed on '{region}:{key}' because the region/key did not exist.");
                    }
                }

                // update backplane
                if (result.UpdateState == UpdateItemResultState.Success && _cacheBackplane != null)
                {
                    if (_logTrace)
                    {
                        Logger.LogTrace("Update: {0} {1}: notifies backplane [change].", key, region);
                    }

                    if (string.IsNullOrWhiteSpace(region))
                    {
                        _cacheBackplane.NotifyChange(key, CacheItemChangedEventAction.Update);
                    }
                    else
                    {
                        _cacheBackplane.NotifyChange(key, region, CacheItemChangedEventAction.Update);
                    }
                }

                return Tuple.Create(result.UpdateState == UpdateItemResultState.Success, outValue);
            }

            var task = Task.Run(LocalFunc);
            var tuple = task.GetAwaiter().GetResult();

            value = tuple.Item2;
            return Task.FromResult(tuple.Item1);
        }
    }
}
