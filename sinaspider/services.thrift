// Defines remote services.
namespace py services

typedef i32 Integer

enum ret_status{
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
service SchedulerService{
    /**
     * Register the downloader along with the name.
     */
    ret_status register_downloader(1: required string name),

    /**
     * Unregister the named downloader.
     */
    ret_status unregister_downloader(1: required string name),

    /**
     * Get a pair of user name and password. For now, each pair of user name and 
     * password can only be granted to exactly one downloader.
     */
    UserIdentity request_user_identity(),

    /**
     * Give up the user identity.
     */
    ret_status resign_user_identity(1: required UserIdentity pair),

    /**
     * Grab a batch of links.
     */
    list<string> grab_links(1: required Integer size),

    /**
     * Submit a batch of links.
     */
    ret_status submit_links(1: required list<string> links),

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
    ret_status submit_proxies(1: required list<ProxyAddress> addrs)
}