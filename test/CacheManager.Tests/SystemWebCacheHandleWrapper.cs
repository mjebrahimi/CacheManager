﻿// disabling for NET40, our Web implementation is not available on that framework
// disabling it for builds on Mono because setting the HttpContext.Current causes all kinds of strange exceptions
#if !NET40 && MOCK_HTTPCONTEXTE_ENABLED
using System.IO;
using System.Web;
using CacheManager.Core;
using CacheManager.Web;

namespace CacheManager.Tests
{
    internal class SystemWebCacheHandleWrapper<TCacheValue> : SystemWebCacheHandle<TCacheValue>
    {
        public SystemWebCacheHandleWrapper(ICacheManager<TCacheValue> manager, CacheHandleConfiguration configuration)
            : base(manager, configuration)
        {
        }

        protected override HttpContextBase Context
        {
            get
            {
                if (HttpContext.Current == null)
                {
                    HttpContext.Current = new HttpContext(new HttpRequest("test", "http://test", string.Empty), new HttpResponse(new StringWriter()));
                }

                return new HttpContextWrapper(HttpContext.Current);
            }
        }
    }
}
#endif