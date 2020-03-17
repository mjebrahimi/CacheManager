using System;
using System.Linq;
using System.Threading.Tasks;
using static CacheManager.Core.Utility.Guard;

namespace CacheManager.Core
{
    public partial class BaseCacheManager<TCacheValue>
    {
        /// <inheritdoc />
        public Task<TCacheValue> GetOrAddAsync(string key, TCacheValue value)
            => GetOrAddAsync(key, (k) => Task.FromResult(value));

        /// <inheritdoc />
        public Task<TCacheValue> GetOrAddAsync(string key, string region, TCacheValue value)
            => GetOrAddAsync(key, region, (k, r) => Task.FromResult(value));

        /// <inheritdoc />
        public async Task<TCacheValue> GetOrAddAsync(string key, Func<string, Task<TCacheValue>> valueFactory)
        {
            NotNullOrWhiteSpace(key, nameof(key));
            NotNull(valueFactory, nameof(valueFactory));

            return (await GetOrAddInternalAsync(key, null, async (k, r) => new CacheItem<TCacheValue>(k, await valueFactory(k)))).Value;
        }

        /// <inheritdoc />
        public async Task<TCacheValue> GetOrAddAsync(string key, string region, Func<string, string, Task<TCacheValue>> valueFactory)
        {
            NotNullOrWhiteSpace(key, nameof(key));
            NotNullOrWhiteSpace(region, nameof(region));
            NotNull(valueFactory, nameof(valueFactory));

            return (await GetOrAddInternalAsync(key, region, async (k, r) => new CacheItem<TCacheValue>(k, r, await valueFactory(k, r)))).Value;
        }

        /// <inheritdoc />
        public Task<CacheItem<TCacheValue>> GetOrAddAsync(string key, Func<string, Task<CacheItem<TCacheValue>>> valueFactory)
        {
            NotNullOrWhiteSpace(key, nameof(key));
            NotNull(valueFactory, nameof(valueFactory));

            return GetOrAddInternalAsync(key, null, (k, r) => valueFactory(k));
        }

        /// <inheritdoc />
        public Task<CacheItem<TCacheValue>> GetOrAddAsync(string key, string region, Func<string, string, Task<CacheItem<TCacheValue>>> valueFactory)
        {
            NotNullOrWhiteSpace(key, nameof(key));
            NotNullOrWhiteSpace(region, nameof(region));
            NotNull(valueFactory, nameof(valueFactory));

            return GetOrAddInternalAsync(key, region, valueFactory);
        }

        /// <inheritdoc />
        public Task<bool> TryGetOrAddAsync(string key, Func<string, Task<TCacheValue>> valueFactory, out TCacheValue value)
        {
            NotNullOrWhiteSpace(key, nameof(key));
            NotNull(valueFactory, nameof(valueFactory));

            async Task<Tuple<bool, TCacheValue>> LocalFunc()
            {
                TCacheValue outValue;
                if (await TryGetOrAddInternalAsync(
                key,
                null,
                async (k, r) =>
                {
                    var newValue = await valueFactory(k);
                    return newValue == null ? null : new CacheItem<TCacheValue>(k, newValue);
                },
                out var item))
                {
                    outValue = item.Value;
                    return Tuple.Create(true, outValue);
                }

                outValue = default(TCacheValue);
                return Tuple.Create(false, outValue);
            }

            //Dead-lock not occurs if ConfigurAwait(false) missed
            var task = Task.Run(LocalFunc);
            var tuple = task.GetAwaiter().GetResult();

            //Dead-lock not occurs if ConfigurAwait(false) missed (a little better performance)
            //var task = Task.Run<Task<Tuple<string, int>>>(LocalFunc);
            //var tuple = task.Unwrap().GetAwaiter().GetResult();

            //Dead-lock occurs if ConfigurAwait(false) missed
            //var tuple = LocalFunc().GetAwaiter().GetResult();

            value = tuple.Item2;
            return Task.FromResult(tuple.Item1);
        }

        /// <inheritdoc />
        public Task<bool> TryGetOrAddAsync(string key, string region, Func<string, string, Task<TCacheValue>> valueFactory, out TCacheValue value)
        {
            NotNullOrWhiteSpace(key, nameof(key));
            NotNullOrWhiteSpace(region, nameof(region));
            NotNull(valueFactory, nameof(valueFactory));

            async Task<Tuple<bool, TCacheValue>> LocalFunc()
            {
                TCacheValue outValue;
                if (await TryGetOrAddInternalAsync(
                    key,
                    region,
                    async (k, r) =>
                    {
                        var newValue = await valueFactory(k, r);
                        return newValue == null ? null : new CacheItem<TCacheValue>(k, r, newValue);
                    },
                    out var item))
                {
                    outValue = item.Value;
                    return Tuple.Create(true, outValue);
                }

                outValue = default(TCacheValue);
                return Tuple.Create(false, outValue);
            }

            var task = Task.Run(LocalFunc);
            var tuple = task.GetAwaiter().GetResult();

            value = tuple.Item2;
            return Task.FromResult(tuple.Item1);
        }

        /// <inheritdoc />
        public Task<bool> TryGetOrAddAsync(string key, Func<string, Task<CacheItem<TCacheValue>>> valueFactory, out CacheItem<TCacheValue> item)
        {
            NotNullOrWhiteSpace(key, nameof(key));
            NotNull(valueFactory, nameof(valueFactory));

            return TryGetOrAddInternalAsync(key, null, (k, r) => valueFactory(k), out item);
        }

        /// <inheritdoc />
        public Task<bool> TryGetOrAddAsync(string key, string region, Func<string, string, Task<CacheItem<TCacheValue>>> valueFactory, out CacheItem<TCacheValue> item)
        {
            NotNullOrWhiteSpace(key, nameof(key));
            NotNullOrWhiteSpace(region, nameof(region));
            NotNull(valueFactory, nameof(valueFactory));

            return TryGetOrAddInternalAsync(key, region, valueFactory, out item);
        }

        private  Task<bool> TryGetOrAddInternalAsync(string key, string region, Func<string, string, Task<CacheItem<TCacheValue>>> valueFactory, out CacheItem<TCacheValue> item)
        {
            async Task<Tuple<bool, CacheItem<TCacheValue>>> LocalFunc()
            {
                CacheItem<TCacheValue> outValue;
                CacheItem<TCacheValue> newItem = null;
                var tries = 0;
                do
                {
                    tries++;
                    outValue = await GetCacheItemInternalAsync(key, region);
                    if (outValue != null)
                    {
                        return Tuple.Create(true, outValue);
                    }

                    // changed logic to invoke the factory only once in case of retries
                    if (newItem == null)
                    {
                        newItem = await valueFactory(key, region);
                    }

                    if (newItem == null)
                    {
                        return Tuple.Create(false, outValue);
                    }

                    if (await AddInternalAsync(newItem))
                    {
                        outValue = newItem;
                        return Tuple.Create(true, outValue);
                    }
                }
                while (tries <= Configuration.MaxRetries);

                return Tuple.Create(false, outValue);
            }

            var task = Task.Run(LocalFunc);
            var tuple = task.GetAwaiter().GetResult();

            item = tuple.Item2;
            return Task.FromResult(tuple.Item1);
        }

        private async Task<CacheItem<TCacheValue>> GetOrAddInternalAsync(string key, string region, Func<string, string, Task<CacheItem<TCacheValue>>> valueFactory)
        {
            CacheItem<TCacheValue> newItem = null;
            var tries = 0;
            do
            {
                tries++;
                var item = await GetCacheItemInternalAsync(key, region);
                if (item != null)
                {
                    return item;
                }

                // changed logic to invoke the factory only once in case of retries
                if (newItem == null)
                {
                    newItem = await valueFactory(key, region);
                }

                // Throw explicit to me more consistent. Otherwise it would throw later eventually...
                if (newItem == null)
                {
                    throw new InvalidOperationException("The CacheItem which should be added must not be null.");
                }

                if (await AddInternalAsync(newItem))
                {
                    return newItem;
                }
            }
            while (tries <= Configuration.MaxRetries);

            // should usually never occur, but could if e.g. max retries is 1 and an item gets added between the get and add.
            // pretty unusual, so keep the max tries at least around 50
            throw new InvalidOperationException(
                string.Format("Could not get nor add the item {0} {1}", key, region));
        }
    }
}
