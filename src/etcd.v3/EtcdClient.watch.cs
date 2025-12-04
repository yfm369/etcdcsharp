using Etcdserverpb;
using Google.Protobuf;
using Grpc.Core;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Etcd;

public partial interface IEtcdClient
{
    public Watch.WatchClient WatchClient { get; }

    Task<EtcdWatcher> WatchAsync(WatchRequest request, Metadata headers = null, DateTime? deadline = null,
        CancellationToken cancellationToken = default);

    Task<EtcdWatcher> WatchRangeAsync(string path, Metadata headers = null, DateTime? deadline = null, long startRevision = 0, bool noPut = false, bool noDelete = false,
        CancellationToken cancellationToken = default);

    Task<EtcdWatcher> WatchAsync(string key, Metadata headers = null, DateTime? deadline = null, long startRevision = 0, bool noPut = false, bool noDelete = false,
        CancellationToken cancellationToken = default);

    Task WatchRangeBackendAsync(string path, Func<WatchResponse, Task> func, Metadata headers = null, DateTime? deadline = null, long startRevision = 0,
        bool noPut = false, bool noDelete = false, Action<Exception> ex = null, bool reWatchWhenException = false, CancellationToken cancellationToken = default);

    Task WatchBackendAsync(string key, Func<WatchResponse, Task> func, Metadata headers = null, DateTime? deadline = null, long startRevision = 0,
        bool noPut = false, bool noDelete = false, Action<Exception> ex = null, bool reWatchWhenException = false, CancellationToken cancellationToken = default);
}

public partial class EtcdClient : IEtcdClient
{
    private Watch.WatchClient watchClient;
    public Watch.WatchClient WatchClient => watchClient ??= new Watch.WatchClient(callInvoker);

    public async Task<EtcdWatcher> WatchAsync(WatchRequest request, Metadata headers = null, DateTime? deadline = null,
        CancellationToken cancellationToken = default)
    {
        var stream = WatchClient.Watch(headers, deadline, cancellationToken);
        await stream.RequestStream.WriteAsync(new WatchRequest() { CreateRequest = request.CreateRequest }, cancellationToken).ConfigureAwait(false);
        return new EtcdWatcher(stream);
    }

    public Task<EtcdWatcher> WatchRangeAsync(string path, Metadata headers = null, DateTime? deadline = null, long startRevision = 0, bool noPut = false, bool noDelete = false,
        CancellationToken cancellationToken = default)
    {
        var req = CreateWatchReq(path, startRevision, noPut, noDelete);
        req.CreateRequest.RangeEnd = ByteString.CopyFromUtf8(path.GetRangeEnd());
        return WatchAsync(req, headers, deadline, cancellationToken);
    }

    public Task<EtcdWatcher> WatchAsync(string key, Metadata headers = null, DateTime? deadline = null, long startRevision = 0, bool noPut = false, bool noDelete = false,
        CancellationToken cancellationToken = default)
    {
        return WatchAsync(CreateWatchReq(key, startRevision, noPut, noDelete), headers, deadline, cancellationToken);
    }

    public Task WatchRangeBackendAsync(string path, Func<WatchResponse, Task> func, Metadata headers = null, DateTime? deadline = null, long startRevision = 0,
        bool noPut = false, bool noDelete = false, Action<Exception> ex = null, bool reWatchWhenException = false, CancellationToken cancellationToken = default)
    {
        return Task.Factory.StartNew(async () =>
        {
            // 退避策略：初始延迟100ms，最大延迟5s，每次失败后延迟翻倍
            int retryDelay = 100;
            const int maxRetryDelay = 5000;
            
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var watcher = await WatchRangeAsync(path, headers, deadline, startRevision, noPut, noDelete, cancellationToken);
                    await watcher.ForAllAsync(reWatchWhenException
                        ? i =>
                    {
                        startRevision = i.FindRevision(startRevision);
                        return func(i);
                    }
                    : func, CancellationToken.None);
                }
                catch (Exception e)
                {
                    // 忽略TaskCanceledException，因为这通常是正常的取消操作
                    if (e is TaskCanceledException && cancellationToken.IsCancellationRequested)
                    {
                        return;
                    }
                    
                    ex?.Invoke(e);
                    if (!reWatchWhenException || cancellationToken.IsCancellationRequested)
                    {
                        return;
                    }
                    
                    // 应用退避策略，在重试前添加延迟
                    await Task.Delay(retryDelay, cancellationToken);
                    // 延迟翻倍，但不超过最大延迟
                    retryDelay = Math.Min(retryDelay * 2, maxRetryDelay);
                }
            }
        }, cancellationToken);
    }

    public Task WatchBackendAsync(string key, Func<WatchResponse, Task> func, Metadata headers = null, DateTime? deadline = null, long startRevision = 0,
        bool noPut = false, bool noDelete = false, Action<Exception> ex = null, bool reWatchWhenException = false, CancellationToken cancellationToken = default)
    {
        return Task.Factory.StartNew(async () =>
        {
            // 退避策略：初始延迟100ms，最大延迟5s，每次失败后延迟翻倍
            int retryDelay = 100;
            const int maxRetryDelay = 5000;
            
            while (!cancellationToken.IsCancellationRequested)
            {
                var watcher = await WatchAsync(key, headers, deadline, startRevision, noPut, noDelete, cancellationToken);
                try
                {
                    await watcher.ForAllAsync(reWatchWhenException
                        ? i =>
                        {
                            startRevision = i.FindRevision(startRevision);
                            return func(i);
                        }
                    : func, CancellationToken.None);
                }
                catch (Exception e)
                {
                    // 忽略TaskCanceledException，因为这通常是正常的取消操作
                    if (e is TaskCanceledException && cancellationToken.IsCancellationRequested)
                    {
                        return;
                    }
                    
                    ex?.Invoke(e);
                    if (!reWatchWhenException || cancellationToken.IsCancellationRequested)
                    {
                        return;
                    }
                    
                    // 应用退避策略，在重试前添加延迟
                    await Task.Delay(retryDelay, cancellationToken);
                    // 延迟翻倍，但不超过最大延迟
                    retryDelay = Math.Min(retryDelay * 2, maxRetryDelay);
                }
            }
        }, cancellationToken);
    }

    private static WatchRequest CreateWatchReq(string key, long startRevision, bool noPut, bool noDelete)
    {
        var req = new WatchCreateRequest
        {
            Key = ByteString.CopyFromUtf8(key),
            StartRevision = startRevision,
            ProgressNotify = true,
            PrevKv = true,
        };
        if (noPut)
        {
            req.Filters.Add(WatchCreateRequest.Types.FilterType.Noput);
        }
        if (noDelete)
        {
            req.Filters.Add(WatchCreateRequest.Types.FilterType.Nodelete);
        }

        return new WatchRequest()
        {
            CreateRequest = req
        };
    }
}
