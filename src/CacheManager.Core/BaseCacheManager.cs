using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using CacheManager.Core.Internal;
using CacheManager.Core.Logging;
using static CacheManager.Core.Utility.Guard;

namespace CacheManager.Core
{
    /// <summary>
    /// The <see cref="BaseCacheManager{TCacheValue}"/> implements <see cref="ICacheManager{TCacheValue}"/> and is the main class
    /// of this library.
    /// The cache manager delegates all cache operations to the list of <see cref="BaseCacheHandle{T}"/>'s which have been
    /// added. It will keep them in sync according to rules and depending on the configuration.
    /// </summary>
    /// <typeparam name="TCacheValue">The type of the cache value.</typeparam>
    public partial class BaseCacheManager<TCacheValue> : BaseCache<TCacheValue>, ICacheManager<TCacheValue>, IDisposable
    {
        private readonly bool _logTrace = false;
        private readonly BaseCacheHandle<TCacheValue>[] _cacheHandles;
        private readonly CacheBackplane _cacheBackplane;

        /// <summary>
        /// Initializes a new instance of the <see cref="BaseCacheManager{TCacheValue}"/> class
        /// using the specified <paramref name="configuration"/>.
        /// If the name of the <paramref name="configuration"/> is defined, the cache manager will
        /// use it. Otherwise a random string will be generated.
        /// </summary>
        /// <param name="configuration">
        /// The configuration which defines the structure and complexity of the cache manager.
        /// </param>
        /// <exception cref="System.ArgumentNullException">
        /// When <paramref name="configuration"/> is null.
        /// </exception>
        /// <see cref="CacheFactory"/>
        /// <see cref="ConfigurationBuilder"/>
        /// <see cref="BaseCacheHandle{TCacheValue}"/>
        public BaseCacheManager(ICacheManagerConfiguration configuration)
            : this(configuration?.Name ?? Guid.NewGuid().ToString(), configuration)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BaseCacheManager{TCacheValue}"/> class
        /// using the specified <paramref name="name"/> and <paramref name="configuration"/>.
        /// </summary>
        /// <param name="name">The cache name.</param>
        /// <param name="configuration">
        /// The configuration which defines the structure and complexity of the cache manager.
        /// </param>
        /// <exception cref="ArgumentNullException">
        /// When <paramref name="name"/> or <paramref name="configuration"/> is null.
        /// </exception>
        /// <see cref="CacheFactory"/>
        /// <see cref="ConfigurationBuilder"/>
        /// <see cref="BaseCacheHandle{TCacheValue}"/>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Usage", "CA2200:RethrowToPreserveStackDetails", Justification = "fine for now")]
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope", Justification = "nope")]
        private BaseCacheManager(string name, ICacheManagerConfiguration configuration)
        {
            NotNullOrWhiteSpace(name, nameof(name));
            NotNull(configuration, nameof(configuration));

            Name = name;
            Configuration = configuration;

            var loggerFactory = CacheReflectionHelper.CreateLoggerFactory(configuration);
            var serializer = CacheReflectionHelper.CreateSerializer(configuration, loggerFactory);

            Logger = loggerFactory.CreateLogger(this);

            _logTrace = Logger.IsEnabled(LogLevel.Trace);

            Logger.LogInfo("Cache manager: adding cache handles...");

            try
            {
                _cacheHandles = CacheReflectionHelper.CreateCacheHandles(this, loggerFactory, serializer).ToArray();

                var index = 0;
                foreach (var handle in _cacheHandles)
                {
                    var handleIndex = index;
                    handle.OnCacheSpecificRemove += (sender, args) =>
                    {
                        // added sync for using backplane with in-memory caches on cache specific removal
                        // but commented for now, this is not really needed if all instances use the same expiration etc, would just cause dublicated events
                        ////if (_cacheBackplane != null && handle.Configuration.IsBackplaneSource && !handle.IsDistributedCache)
                        ////{
                        ////    if (string.IsNullOrEmpty(args.Region))
                        ////    {
                        ////        _cacheBackplane.NotifyRemove(args.Key);
                        ////    }
                        ////    else
                        ////    {
                        ////        _cacheBackplane.NotifyRemove(args.Key, args.Region);
                        ////    }
                        ////}

                        // base cache handle does logging for this

                        if (Configuration.UpdateMode == CacheUpdateMode.Up)
                        {
                            if (_logTrace)
                            {
                                Logger.LogTrace("Cleaning handles above '{0}' because of remove event.", handleIndex);
                            }

                            EvictFromHandlesAboveAsync(args.Key, args.Region, handleIndex).GetAwaiter().GetResult();
                        }

                        // moving down below cleanup, optherwise the item could still be in memory
                        TriggerOnRemoveByHandle(args.Key, args.Region, args.Reason, handleIndex + 1, args.Value);
                    };

                    index++;
                }

                _cacheBackplane = CacheReflectionHelper.CreateBackplane(configuration, loggerFactory);
                if (_cacheBackplane != null)
                {
                    RegisterCacheBackplane(_cacheBackplane);
                }
            }
            catch (Exception ex)
            {
                Logger.LogError(ex, "Error occurred while creating the cache manager.");
                throw ex.InnerException ?? ex;
            }
        }

        /// <inheritdoc />
        public event EventHandler<CacheActionEventArgs> OnAdd;

        /// <inheritdoc />
        public event EventHandler<CacheClearEventArgs> OnClear;

        /// <inheritdoc />
        public event EventHandler<CacheClearRegionEventArgs> OnClearRegion;

        /// <inheritdoc />
        public event EventHandler<CacheActionEventArgs> OnGet;

        /// <inheritdoc />
        public event EventHandler<CacheActionEventArgs> OnPut;

        /// <inheritdoc />
        public event EventHandler<CacheActionEventArgs> OnRemove;

        /// <inheritdoc />
        public event EventHandler<CacheItemRemovedEventArgs> OnRemoveByHandle;

        /// <inheritdoc />
        public event EventHandler<CacheActionEventArgs> OnUpdate;

        /// <inheritdoc />
        public IReadOnlyCacheManagerConfiguration Configuration { get; }

        /// <inheritdoc />
        public IEnumerable<BaseCacheHandle<TCacheValue>> CacheHandles
            => new ReadOnlyCollection<BaseCacheHandle<TCacheValue>>(
                new List<BaseCacheHandle<TCacheValue>>(
                    _cacheHandles));

        /// <summary>
        /// Gets the configured cache backplane.
        /// </summary>
        /// <value>The backplane.</value>
        public CacheBackplane Backplane => _cacheBackplane;

        /// <summary>
        /// Gets the cache name.
        /// </summary>
        /// <value>The name of the cache.</value>
        public string Name { get; }

        /// <inheritdoc />
        protected override ILogger Logger { get; }

        /// <inheritdoc />
        public override async Task ClearAsync()
        {
            CheckDisposed();
            if (_logTrace)
            {
                Logger.LogTrace("Clear: flushing cache...");
            }

            foreach (var handle in _cacheHandles)
            {
                if (_logTrace)
                {
                    Logger.LogTrace("Clear: clearing handle {0}.", handle.Configuration.Name);
                }

                await handle.ClearAsync();
                handle.Stats.OnClear();
            }

            if (_cacheBackplane != null)
            {
                if (_logTrace)
                {
                    Logger.LogTrace("Clear: notifies backplane.");
                }

                _cacheBackplane.NotifyClear();
            }

            TriggerOnClear();
        }

        /// <inheritdoc />
        public override async Task ClearRegionAsync(string region)
        {
            NotNullOrWhiteSpace(region, nameof(region));

            CheckDisposed();
            if (_logTrace)
            {
                Logger.LogTrace("Clear region: {0}.", region);
            }

            foreach (var handle in _cacheHandles)
            {
                if (_logTrace)
                {
                    Logger.LogTrace("Clear region: {0} in handle {1}.", region, handle.Configuration.Name);
                }

                await handle.ClearRegionAsync(region);
                handle.Stats.OnClearRegion(region);
            }

            if (_cacheBackplane != null)
            {
                if (_logTrace)
                {
                    Logger.LogTrace("Clear region: {0}: notifies backplane [clear region].", region);
                }

                _cacheBackplane.NotifyClearRegion(region);
            }

            TriggerOnClearRegion(region);
        }

        /// <inheritdoc />
        public override async Task<bool> ExistsAsync(string key)
        {
            foreach (var handle in _cacheHandles)
            {
                if (_logTrace)
                {
                    Logger.LogTrace("Checking if [{0}] exists on handle '{1}'.", key, handle.Configuration.Name);
                }

                if (await handle.ExistsAsync(key))
                {
                    return true;
                }
            }

            return false;
        }

        /// <inheritdoc />
        public override async Task<bool> ExistsAsync(string key, string region)
        {
            foreach (var handle in _cacheHandles)
            {
                if (_logTrace)
                {
                    Logger.LogTrace("Checking if [{0}:{1}] exists on handle '{2}'.", region, key, handle.Configuration.Name);
                }

                if (await handle.ExistsAsync(key, region))
                {
                    return true;
                }
            }

            return false;
        }

        /// <summary>
        /// Returns a <see cref="string" /> that represents this instance.
        /// </summary>
        /// <returns>
        /// A <see cref="string" /> that represents this instance.
        /// </returns>
        public override string ToString() =>
            string.Format(CultureInfo.InvariantCulture, "Name: {0}, Handles: [{1}]", Name, string.Join(",", _cacheHandles.Select(p => p.GetType().Name)));

        /// <inheritdoc />
        protected internal override async Task<bool> AddInternalAsync(CacheItem<TCacheValue> item)
        {
            NotNull(item, nameof(item));

            CheckDisposed();
            if (_logTrace)
            {
                Logger.LogTrace("Add [{0}] started.", item);
            }

            var handleIndex = _cacheHandles.Length - 1;

            var result = await AddItemToHandleAsync(item, _cacheHandles[handleIndex]);

            // evict from other handles in any case because if it exists, it might be a different version
            // if not exist, its just a sanity check to invalidate other versions in upper layers.
            await EvictFromOtherHandlesAsync(item.Key, item.Region, handleIndex);

            if (result)
            {
                // update backplane
                if (_cacheBackplane != null)
                {
                    if (string.IsNullOrWhiteSpace(item.Region))
                    {
                        _cacheBackplane.NotifyChange(item.Key, CacheItemChangedEventAction.Add);
                    }
                    else
                    {
                        _cacheBackplane.NotifyChange(item.Key, item.Region, CacheItemChangedEventAction.Add);
                    }

                    if (_logTrace)
                    {
                        Logger.LogTrace("Notified backplane 'change' because [{0}] was added.", item);
                    }
                }

                // trigger only once and not per handle and only if the item was added!
                TriggerOnAdd(item.Key, item.Region);
            }

            return result;
        }

        /// <inheritdoc />
        protected internal override async Task PutInternalAsync(CacheItem<TCacheValue> item)
        {
            NotNull(item, nameof(item));

            CheckDisposed();
            if (_logTrace)
            {
                Logger.LogTrace("Put [{0}] started.", item);
            }

            foreach (var handle in _cacheHandles)
            {
                if (handle.Configuration.EnableStatistics)
                {
                    // check if it is really a new item otherwise the items count is crap because we
                    // count it every time, but use only the current handle to retrieve the item,
                    // otherwise we would trigger gets and find it in another handle maybe
                    var oldItem = string.IsNullOrWhiteSpace(item.Region) ?
                        await handle.GetCacheItemAsync(item.Key) :
                        await handle.GetCacheItemAsync(item.Key, item.Region);

                    handle.Stats.OnPut(item, oldItem == null);
                }

                if (_logTrace)
                {
                    Logger.LogTrace(
                        "Put [{0}:{1}] successfully to handle '{2}'.",
                        item.Region,
                        item.Key,
                        handle.Configuration.Name);
                }

                await handle.PutAsync(item);
            }

            // update backplane
            if (_cacheBackplane != null)
            {
                if (_logTrace)
                {
                    Logger.LogTrace("Put [{0}:{1}] was scuccessful. Notifying backplane [change].", item.Region, item.Key);
                }

                if (string.IsNullOrWhiteSpace(item.Region))
                {
                    _cacheBackplane.NotifyChange(item.Key, CacheItemChangedEventAction.Put);
                }
                else
                {
                    _cacheBackplane.NotifyChange(item.Key, item.Region, CacheItemChangedEventAction.Put);
                }
            }

            TriggerOnPut(item.Key, item.Region);
        }

        /// <inheritdoc />
        protected override void Dispose(bool disposeManaged)
        {
            if (disposeManaged)
            {
                foreach (var handle in _cacheHandles)
                {
                    handle.Dispose();
                }

                if (_cacheBackplane != null)
                {
                    _cacheBackplane.Dispose();
                }
            }

            base.Dispose(disposeManaged);
        }

        /// <inheritdoc />
        protected override Task<CacheItem<TCacheValue>> GetCacheItemInternalAsync(string key) =>
            GetCacheItemInternalAsync(key, null);

        /// <inheritdoc />
        protected override async Task<CacheItem<TCacheValue>> GetCacheItemInternalAsync(string key, string region)
        {
            CheckDisposed();

            CacheItem<TCacheValue> cacheItem = null;

            if (_logTrace)
            {
                Logger.LogTrace("Get [{0}:{1}] started.", region, key);
            }

            for (var handleIndex = 0; handleIndex < _cacheHandles.Length; handleIndex++)
            {
                var handle = _cacheHandles[handleIndex];
                if (string.IsNullOrWhiteSpace(region))
                {
                    cacheItem = await handle.GetCacheItemAsync(key);
                }
                else
                {
                    cacheItem = await handle.GetCacheItemAsync(key, region);
                }

                handle.Stats.OnGet(region);

                if (cacheItem != null)
                {
                    if (_logTrace)
                    {
                        Logger.LogTrace("Get [{0}:{1}], found in handle[{2}] '{3}'.", region, key, handleIndex, handle.Configuration.Name);
                    }

                    // update last accessed, might be used for custom sliding implementations
                    cacheItem.LastAccessedUtc = DateTime.UtcNow;

                    // update other handles if needed
                    await AddToHandlesAsync(cacheItem, handleIndex);
                    handle.Stats.OnHit(region);
                    TriggerOnGet(key, region);
                    break;
                }
                else
                {
                    if (_logTrace)
                    {
                        Logger.LogTrace("Get [{0}:{1}], item NOT found in handle[{2}] '{3}'.", region, key, handleIndex, handle.Configuration.Name);
                    }

                    handle.Stats.OnMiss(region);
                }
            }

            return cacheItem;
        }

        /// <inheritdoc />
        protected override Task<bool> RemoveInternalAsync(string key) =>
            RemoveInternalAsync(key, null);

        /// <inheritdoc />
        protected override async Task<bool> RemoveInternalAsync(string key, string region)
        {
            CheckDisposed();

            var result = false;

            if (_logTrace)
            {
                Logger.LogTrace("Removing [{0}:{1}].", region, key);
            }

            foreach (var handle in _cacheHandles)
            {
                var handleResult = false;
                if (!string.IsNullOrWhiteSpace(region))
                {
                    handleResult = await handle.RemoveAsync(key, region);
                }
                else
                {
                    handleResult = await handle.RemoveAsync(key);
                }

                if (handleResult)
                {
                    if (_logTrace)
                    {
                        Logger.LogTrace(
                            "Remove [{0}:{1}], successfully removed from handle '{2}'.",
                            region,
                            key,
                            handle.Configuration.Name);
                    }

                    result = true;
                    handle.Stats.OnRemove(region);
                }
            }

            if (result)
            {
                // update backplane
                if (_cacheBackplane != null)
                {
                    if (_logTrace)
                    {
                        Logger.LogTrace("Removed [{0}:{1}], notifying backplane [remove].", region, key);
                    }

                    if (string.IsNullOrWhiteSpace(region))
                    {
                        _cacheBackplane.NotifyRemove(key);
                    }
                    else
                    {
                        _cacheBackplane.NotifyRemove(key, region);
                    }
                }

                // trigger only once and not per handle
                TriggerOnRemove(key, region);
            }

            return result;
        }

        private static async Task<bool> AddItemToHandleAsync(CacheItem<TCacheValue> item, BaseCacheHandle<TCacheValue> handle)
        {
            if (await handle.AddAsync(item))
            {
                handle.Stats.OnAdd(item);
                return true;
            }

            return false;
        }

        private static async Task ClearHandlesAsync(BaseCacheHandle<TCacheValue>[] handles)
        {
            foreach (var handle in handles)
            {
                await handle.ClearAsync();
                handle.Stats.OnClear();
            }

            ////this.TriggerOnClear();
        }

        private static async Task ClearRegionHandlesAsync(string region, BaseCacheHandle<TCacheValue>[] handles)
        {
            foreach (var handle in handles)
            {
                await handle.ClearRegionAsync(region);
                handle.Stats.OnClearRegion(region);
            }

            ////this.TriggerOnClearRegion(region);
        }

        private async Task EvictFromHandlesAsync(string key, string region, BaseCacheHandle<TCacheValue>[] handles)
        {
            foreach (var handle in handles)
            {
                await EvictFromHandleAsync(key, region, handle);
            }
        }

        private async Task EvictFromHandleAsync(string key, string region, BaseCacheHandle<TCacheValue> handle)
        {
            if (Logger.IsEnabled(LogLevel.Debug))
            {
                Logger.LogDebug(
                    "Evicting '{0}:{1}' from handle '{2}'.",
                    region,
                    key,
                    handle.Configuration.Name);
            }

            bool result;
            if (string.IsNullOrWhiteSpace(region))
            {
                result = await handle.RemoveAsync(key);
            }
            else
            {
                result = await handle.RemoveAsync(key, region);
            }

            if (result)
            {
                handle.Stats.OnRemove(region);
            }
        }

        private async Task AddToHandlesAsync(CacheItem<TCacheValue> item, int foundIndex)
        {
            if (_logTrace)
            {
                Logger.LogTrace(
                    "Start updating handles with [{0}].",
                    item);
            }

            if (foundIndex == 0)
            {
                return;
            }

            // update all cache handles with lower order, up the list
            for (var handleIndex = 0; handleIndex < _cacheHandles.Length; handleIndex++)
            {
                if (handleIndex < foundIndex)
                {
                    if (_logTrace)
                    {
                        Logger.LogTrace("Updating handles, added [{0}] to handle '{1}'.", item, _cacheHandles[handleIndex].Configuration.Name);
                    }

                    await _cacheHandles[handleIndex].AddAsync(item);
                }
            }
        }

        private async Task AddToHandlesBelowAsync(CacheItem<TCacheValue> item, int foundIndex)
        {
            if (item == null)
            {
                return;
            }

            if (_logTrace)
            {
                Logger.LogTrace("Add [{0}] to handles below handle '{1}'.", item, foundIndex);
            }

            for (var handleIndex = 0; handleIndex < _cacheHandles.Length; handleIndex++)
            {
                if (handleIndex > foundIndex)
                {
                    if (await _cacheHandles[handleIndex].AddAsync(item))
                    {
                        _cacheHandles[handleIndex].Stats.OnAdd(item);
                    }
                }
            }
        }

        private async Task EvictFromOtherHandlesAsync(string key, string region, int excludeIndex)
        {
            if (excludeIndex < 0 || excludeIndex >= _cacheHandles.Length)
            {
                throw new ArgumentOutOfRangeException(nameof(excludeIndex));
            }

            if (_logTrace)
            {
                Logger.LogTrace("Evict [{0}:{1}] from other handles excluding handle '{2}'.", region, key, excludeIndex);
            }

            for (var handleIndex = 0; handleIndex < _cacheHandles.Length; handleIndex++)
            {
                if (handleIndex != excludeIndex)
                {
                    await EvictFromHandleAsync(key, region, _cacheHandles[handleIndex]);
                }
            }
        }

        private async Task EvictFromHandlesAboveAsync(string key, string region, int excludeIndex)
        {
            if (_logTrace)
            {
                Logger.LogTrace("Evict from handles above: {0} {1}: above handle {2}.", key, region, excludeIndex);
            }

            if (excludeIndex < 0 || excludeIndex >= _cacheHandles.Length)
            {
                throw new ArgumentOutOfRangeException(nameof(excludeIndex));
            }

            for (var handleIndex = 0; handleIndex < _cacheHandles.Length; handleIndex++)
            {
                if (handleIndex < excludeIndex)
                {
                    await EvictFromHandleAsync(key, region, _cacheHandles[handleIndex]);
                }
            }
        }

        private void RegisterCacheBackplane(CacheBackplane backplane)
        {
            NotNull(backplane, nameof(backplane));

            // this should have been checked during activation already, just to be totally sure...
            if (_cacheHandles.Any(p => p.Configuration.IsBackplaneSource))
            {
                // added includeSource param to get the handles which need to be synced.
                // in case the backplane source is non-distributed (in-memory), only remotly triggered remove and clear should also
                // trigger a sync locally. For distribtued caches, we expect that the distributed cache is already the source and in sync
                // as that's the layer which triggered the event. In this case, only other in-memory handles above the distribtued, would be synced.
                var handles = new Func<bool, BaseCacheHandle<TCacheValue>[]>((includSource) =>
                {
                    var handleList = new List<BaseCacheHandle<TCacheValue>>();
                    foreach (var handle in _cacheHandles)
                    {
                        if (!handle.Configuration.IsBackplaneSource ||
                            (includSource && handle.Configuration.IsBackplaneSource && !handle.IsDistributedCache))
                        {
                            handleList.Add(handle);
                        }
                    }
                    return handleList.ToArray();
                });

                backplane.Changed += (sender, args) =>
                {
                    if (Logger.IsEnabled(LogLevel.Debug))
                    {
                        Logger.LogDebug("Backplane event: [Changed] for '{1}:{0}'.", args.Key, args.Region);
                    }

                    EvictFromHandlesAsync(args.Key, args.Region, handles(false)).GetAwaiter().GetResult();
                    switch (args.Action)
                    {
                        case CacheItemChangedEventAction.Add:
                            TriggerOnAdd(args.Key, args.Region, CacheActionEventArgOrigin.Remote);
                            break;

                        case CacheItemChangedEventAction.Put:
                            TriggerOnPut(args.Key, args.Region, CacheActionEventArgOrigin.Remote);
                            break;

                        case CacheItemChangedEventAction.Update:
                            TriggerOnUpdate(args.Key, args.Region, CacheActionEventArgOrigin.Remote);
                            break;
                    }
                };

                backplane.Removed += (sender, args) =>
                {
                    if (_logTrace)
                    {
                        Logger.LogTrace("Backplane event: [Remove] of {0} {1}.", args.Key, args.Region);
                    }

                    EvictFromHandlesAsync(args.Key, args.Region, handles(true)).GetAwaiter().GetResult();
                    TriggerOnRemove(args.Key, args.Region, CacheActionEventArgOrigin.Remote);
                };

                backplane.Cleared += (sender, args) =>
                {
                    if (_logTrace)
                    {
                        Logger.LogTrace("Backplane event: [Clear].");
                    }

                     ClearHandlesAsync(handles(true)).GetAwaiter().GetResult();
                    TriggerOnClear(CacheActionEventArgOrigin.Remote);
                };

                backplane.ClearedRegion += (sender, args) =>
                {
                    if (_logTrace)
                    {
                        Logger.LogTrace("Backplane event: [Clear Region] region: {0}.", args.Region);
                    }

                    ClearRegionHandlesAsync(args.Region, handles(true)).GetAwaiter().GetResult();
                    TriggerOnClearRegion(args.Region, CacheActionEventArgOrigin.Remote);
                };
            }
        }

        private void TriggerOnAdd(string key, string region, CacheActionEventArgOrigin origin = CacheActionEventArgOrigin.Local)
        {
            OnAdd?.Invoke(this, new CacheActionEventArgs(key, region, origin));
        }

        private void TriggerOnClear(CacheActionEventArgOrigin origin = CacheActionEventArgOrigin.Local)
        {
            OnClear?.Invoke(this, new CacheClearEventArgs(origin));
        }

        private void TriggerOnClearRegion(string region, CacheActionEventArgOrigin origin = CacheActionEventArgOrigin.Local)
        {
            OnClearRegion?.Invoke(this, new CacheClearRegionEventArgs(region, origin));
        }

        private void TriggerOnGet(string key, string region, CacheActionEventArgOrigin origin = CacheActionEventArgOrigin.Local)
        {
            OnGet?.Invoke(this, new CacheActionEventArgs(key, region, origin));
        }

        private void TriggerOnPut(string key, string region, CacheActionEventArgOrigin origin = CacheActionEventArgOrigin.Local)
        {
            OnPut?.Invoke(this, new CacheActionEventArgs(key, region, origin));
        }

        private void TriggerOnRemove(string key, string region, CacheActionEventArgOrigin origin = CacheActionEventArgOrigin.Local)
        {
            NotNullOrWhiteSpace(key, nameof(key));
            OnRemove?.Invoke(this, new CacheActionEventArgs(key, region, origin));
        }

        private void TriggerOnRemoveByHandle(string key, string region, CacheItemRemovedReason reason, int level, object value)
        {
            NotNullOrWhiteSpace(key, nameof(key));
            OnRemoveByHandle?.Invoke(this, new CacheItemRemovedEventArgs(key, region, reason, value, level));
        }

        private void TriggerOnUpdate(string key, string region, CacheActionEventArgOrigin origin = CacheActionEventArgOrigin.Local)
        {
            OnUpdate?.Invoke(this, new CacheActionEventArgs(key, region, origin));
        }
    }
}
