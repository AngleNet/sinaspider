// Defines remote services.
namespace py services

typedef i32 Integer

enum RetStatus{
    SUCCESS = 0,
    FAILED = 1
}

/**
 * Pair of user name and password. The downloader will request the pair to login
 * and download the pages.
 */
struct UserIdentity{
    1: required string name,
    2: required string pwd
}

/**
 * Proxy address. 
 */
struct ProxyAddress{
    1: required string addr,
    2: required Integer port
}

/**
 * A scheduler service.
 */
service scheduler_service{
    /**
     * Register the downloader along with the name.
     */
    RetStatus register_downloader(1: required string name),

    /**
     * Unregister the named downloader.
     */
    RetStatus unregister_downloader(1: required string name),

    /**
     * Get a pair of user name and password. For now, each pair of user name and 
     * password can only be granted to exactly one downloader.
     */
    UserIdentity request_user_identity(),

    /**
     * Give up the user identity.
     */
    RetStatus resign_user_identity(1: required UserIdentity pair),

    /**
     * Grab a batch of links.
     */
    list<string> grab_links(1: required Integer size),

    /**
     * Submit a batch of links.
     */
    RetStatus submit_links(1: required list<string> links),

    /**
     * Request a living proxy.
     */
    ProxyAddress request_proxy(),

    /**
     * Resign a proxy. If a downloader find out the proxy is dead, tell the scheduler.
     * The scheduler will give it a new one.
     */
    ProxyAddress resign_proxy(1: required ProxyAddress addr),

    /**
     * Submit a batch of proxies to scheduler.
     */
    RetStatus submit_proxies(1: required list<ProxyAddress> addrs)
}