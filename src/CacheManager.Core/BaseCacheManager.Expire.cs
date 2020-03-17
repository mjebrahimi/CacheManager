using System;
using System.Linq;
using System.Threading.Tasks;
using CacheManager.Core.Logging;

namespace CacheManager.Core
{
    public partial class BaseCacheManager<TCacheValue>
    {
        /// <inheritdoc />
        public Task ExpireAsync(string key, ExpirationMode mode, TimeSpan timeout)
            => ExpireInternalAsync(key, null, mode, timeout);

        /// <inheritdoc />
        public Task ExpireAsync(string key, string region, ExpirationMode mode, TimeSpan timeout)
            => ExpireInternalAsync(key, region, mode, timeout);

        private async Task ExpireInternalAsync(string key, string region, ExpirationMode mode, TimeSpan timeout)
        {
            CheckDisposed();

            var item = await GetCacheItemInternalAsync(key, region);
            if (item == null)
            {
                Logger.LogTrace("Expire: item not found for key {0}:{1}", key, region);
                return;
            }

            if (_logTrace)
            {
                Logger.LogTrace("Expire [{0}] started.", item);
            }

            if (mode == ExpirationMode.Absolute)
            {
                item = item.WithAbsoluteExpiration(timeout);
            }
            else if (mode == ExpirationMode.Sliding)
            {
                item = item.WithSlidingExpiration(timeout);
            }
            else if (mode == ExpirationMode.None)
            {
                item = item.WithNoExpiration();
            }
            else if (mode == ExpirationMode.Default)
            {
                item = item.WithDefaultExpiration();
            }

            if (_logTrace)
            {
                Logger.LogTrace("Expire - Expiration of [{0}] has been modified. Using put to store the item...", item);
            }

            await PutInternalAsync(item);
        }

        /// <inheritdoc />
        public Task ExpireAsync(string key, DateTimeOffset absoluteExpiration)
        {
            var timeout = absoluteExpiration.UtcDateTime - DateTime.UtcNow;
            if (timeout <= TimeSpan.Zero)
            {
                throw new ArgumentException("Expiration value must be greater than zero.", nameof(absoluteExpiration));
            }

            return ExpireAsync(key, ExpirationMode.Absolute, timeout);
        }

        /// <inheritdoc />
        public Task ExpireAsync(string key, string region, DateTimeOffset absoluteExpiration)
        {
            var timeout = absoluteExpiration.UtcDateTime - DateTime.UtcNow;
            if (timeout <= TimeSpan.Zero)
            {
                throw new ArgumentException("Expiration value must be greater than zero.", nameof(absoluteExpiration));
            }

            return ExpireAsync(key, region, ExpirationMode.Absolute, timeout);
        }

        /// <inheritdoc />
        public Task ExpireAsync(string key, TimeSpan slidingExpiration)
        {
            if (slidingExpiration <= TimeSpan.Zero)
            {
                throw new ArgumentException("Expiration value must be greater than zero.", nameof(slidingExpiration));
            }

            return ExpireAsync(key, ExpirationMode.Sliding, slidingExpiration);
        }

        /// <inheritdoc />
        public Task ExpireAsync(string key, string region, TimeSpan slidingExpiration)
        {
            if (slidingExpiration <= TimeSpan.Zero)
            {
                throw new ArgumentException("Expiration value must be greater than zero.", nameof(slidingExpiration));
            }

            return ExpireAsync(key, region, ExpirationMode.Sliding, slidingExpiration);
        }

        /// <inheritdoc />
        public Task RemoveExpirationAsync(string key)
        {
            return ExpireAsync(key, ExpirationMode.None, default(TimeSpan));
        }

        /// <inheritdoc />
        public Task RemoveExpirationAsync(string key, string region)
        {
            return ExpireAsync(key, region, ExpirationMode.None, default(TimeSpan));
        }
    }
}
