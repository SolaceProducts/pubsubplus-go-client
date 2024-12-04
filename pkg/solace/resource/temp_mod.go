package resource

type CachedMessageSubscriptionRequest interface {
        GetCacheName() string
        GetName() string
}
