//! Which addresses a webhook may be sent to.
//!
//! Relay's product is "give us a URL and we will send an HTTP request to it", from a
//! machine that sits inside a private network. That is a server-side request forgery
//! engine unless something stops it: a customer registering
//! `http://169.254.169.254/latest/meta-data/iam/security-credentials/` gets the cloud
//! metadata service fetched from inside the instance, where it answers without
//! authentication, and the response snippet then carries the credentials back out
//! through the delivery history. That is roughly how the 2019 Capital One breach
//! went.
//!
//! The attacker never touches our network. We touch it for them, wearing our own
//! badge. So the rule is simple and blunt: **a customer's webhook endpoint lives on
//! the public internet, never inside our network.**
//!
//! Two things this module deliberately does not do.
//!
//! It does not match on the URL text. Loopback has far too many spellings —
//! `127.0.0.1`, `127.1`, `0.0.0.0`, `2130706433`, `0x7f000001`, `[::1]`,
//! `::ffff:127.0.0.1` — and a string blocklist will always miss one. It checks the
//! resolved address, because a number has exactly one spelling.
//!
//! And it does not resolve anything, because this crate performs no I/O. The caller
//! resolves and passes the addresses in. That matters for more than purity: the
//! address that gets checked has to be the same one that gets connected to, or an
//! attacker's DNS server can answer honestly the first time and dishonestly the
//! second.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

/// Why an endpoint was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refused {
    /// Not `http` or `https`. `file://`, `gopher://` and friends are not things a
    /// webhook receiver speaks, and some of them read local files.
    Scheme(String),
    /// The URL has no host to resolve.
    NoHost,
    /// The host resolved to an address inside our own network.
    Internal(IpAddr),
    /// The host resolved to nothing at all.
    Unresolvable,
}

/// Implemented so this can be returned from an HTTP client's resolver hook, which
/// requires a boxed `std::error::Error`. The guard has to live inside the client's
/// own name resolution, not merely alongside it.
impl std::error::Error for Refused {}

impl std::fmt::Display for Refused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Scheme(s) => write!(f, "refused: scheme {s:?} is not http or https"),
            Self::NoHost => write!(f, "refused: url has no host"),
            Self::Internal(ip) => write!(f, "refused: {ip} is not a public address"),
            Self::Unresolvable => write!(f, "refused: host does not resolve"),
        }
    }
}

/// How strict to be about where deliveries may go.
///
/// `Default` is the strict policy. That is deliberate and load-bearing: a permissive
/// default is a vulnerability that ships whenever somebody forgets to configure it,
/// which is always. Callers that need loopback have to say so.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Policy {
    /// Permit private, loopback and link-local addresses.
    ///
    /// Off in production, on for local development and tests, where every receiver
    /// is on `127.0.0.1` and would otherwise be refused.
    pub allow_private: bool,
}

impl Policy {
    /// For local development and tests, where receivers live on loopback.
    pub fn permissive() -> Self {
        Self {
            allow_private: true,
        }
    }

    pub fn check_scheme(&self, scheme: &str) -> Result<(), Refused> {
        match scheme {
            "http" | "https" => Ok(()),
            other => Err(Refused::Scheme(other.to_string())),
        }
    }

    /// Check every address a host resolved to.
    ///
    /// Refuses if *any* of them is internal, not merely if all are. A host under the
    /// attacker's control can return one public address and one loopback address and
    /// hope the connection picks the second.
    pub fn check_addrs(&self, addrs: &[IpAddr]) -> Result<(), Refused> {
        if addrs.is_empty() {
            return Err(Refused::Unresolvable);
        }
        if self.allow_private {
            return Ok(());
        }
        match addrs.iter().find(|a| is_internal(**a)) {
            Some(bad) => Err(Refused::Internal(*bad)),
            None => Ok(()),
        }
    }
}

/// Whether an address is one we must never send a customer's webhook to.
///
/// Errs towards refusing. Anything not clearly a routable public address is treated
/// as internal: the cost of refusing a legitimate endpoint is an error message, and
/// the cost of allowing an illegitimate one is the infrastructure.
pub fn is_internal(addr: IpAddr) -> bool {
    match addr {
        IpAddr::V4(v4) => is_internal_v4(v4),
        IpAddr::V6(v6) => {
            // An IPv4 address wearing an IPv6 costume. `::ffff:127.0.0.1` reaches
            // loopback just as well as `127.0.0.1` does, so it has to be unwrapped
            // and judged as what it is — otherwise the whole v4 blocklist is
            // bypassed by writing the address differently.
            if let Some(mapped) = v6.to_ipv4_mapped() {
                return is_internal_v4(mapped);
            }
            if let Some(compat) = v6.to_ipv4() {
                return is_internal_v4(compat);
            }
            is_internal_v6(v6)
        }
    }
}

fn is_internal_v4(ip: Ipv4Addr) -> bool {
    let [a, b, _, _] = ip.octets();
    ip.is_loopback()          // 127.0.0.0/8   — this machine
        || ip.is_private()    // 10/8, 172.16/12, 192.168/16
        || ip.is_link_local() // 169.254.0.0/16 — the cloud metadata service
        || ip.is_multicast()  // 224.0.0.0/4
        || ip.is_broadcast()  // 255.255.255.255
        || ip.is_documentation()
        || a == 0             // 0.0.0.0/8 — "this network", another route to loopback
        || (a == 100 && (64..128).contains(&b)) // 100.64/10 carrier-grade NAT
        || (a == 192 && b == 0)                  // 192.0.0/24 IETF protocol assignments
        || (a == 198 && (18..20).contains(&b))   // 198.18/15 benchmarking
        || a >= 240 // 240.0.0.0/4 reserved
}

fn is_internal_v6(ip: Ipv6Addr) -> bool {
    let segments = ip.segments();
    ip.is_loopback()        // ::1
        || ip.is_unspecified() // ::
        || ip.is_multicast()   // ff00::/8
        || (segments[0] & 0xfe00) == 0xfc00 // fc00::/7  unique local
        || (segments[0] & 0xffc0) == 0xfe80 // fe80::/10 link-local
        // 100::/64 discard-only. The full prefix, not just the first segment: the
        // rest of 0100::/8 is merely unallocated, not special.
        || (segments[0] == 0x0100 && segments[1..4] == [0, 0, 0])
        || (segments[0] == 0x2001 && segments[1] == 0x0db8) // 2001:db8::/32 docs
        // 64:ff9b::/96 is NAT64: it carries an embedded IPv4 address to somewhere
        // else's network, which is exactly the indirection this module exists to
        // refuse.
        || (segments[0] == 0x0064 && segments[1] == 0xff9b)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn ip(s: &str) -> IpAddr {
        IpAddr::from_str(s).expect("parseable address")
    }

    fn strict() -> Policy {
        Policy::default()
    }

    #[test]
    fn the_cloud_metadata_service_is_refused() {
        // The one that matters most. It answers without authentication, on the
        // assumption that only the machine itself can reach it — and we are the
        // machine.
        assert!(is_internal(ip("169.254.169.254")));
        assert_eq!(
            strict().check_addrs(&[ip("169.254.169.254")]),
            Err(Refused::Internal(ip("169.254.169.254")))
        );
    }

    #[test]
    fn loopback_is_refused_however_it_is_written() {
        // These are all the same machine. A blocklist matching URL text would have to
        // know every one of these spellings; checking the parsed address means there
        // is only ever one.
        for s in [
            "127.0.0.1",
            "127.0.0.2",
            "127.1.2.3",
            "0.0.0.0",
            "0.1.2.3",
            "::1",
            "::ffff:127.0.0.1",
            "::127.0.0.1",
        ] {
            assert!(is_internal(ip(s)), "{s} should be refused");
        }
    }

    #[test]
    fn private_networks_are_refused() {
        for s in [
            "10.0.0.1",
            "10.255.255.255",
            "172.16.0.1",
            "172.31.255.255",
            "192.168.0.1",
            "192.168.255.255",
            "100.64.0.1",      // carrier-grade NAT
            "192.0.0.1",       // IETF protocol assignments
            "198.18.0.1",      // benchmarking
            "240.0.0.1",       // reserved
            "255.255.255.255", // broadcast
            "224.0.0.1",       // multicast
        ] {
            assert!(is_internal(ip(s)), "{s} should be refused");
        }
    }

    #[test]
    fn internal_ipv6_is_refused() {
        for s in [
            "fc00::1",         // unique local
            "fd12:3456::1",    // unique local
            "fe80::1",         // link-local
            "ff02::1",         // multicast
            "::",              // unspecified
            "100::1",          // discard
            "2001:db8::1",     // documentation
            "64:ff9b::7f00:1", // NAT64 carrying an embedded IPv4
        ] {
            assert!(is_internal(ip(s)), "{s} should be refused");
        }
    }

    #[test]
    fn public_addresses_are_allowed() {
        // The other half. A guard that refuses everything is not a guard, it is an
        // outage.
        // The boundary cases are the point: an off-by-one in a range check either
        // opens a hole or blackholes a legitimate customer.
        for s in [
            "1.1.1.1",
            "8.8.8.8",
            "93.184.216.34",
            "172.15.255.255",  // just below the private 172.16/12 range
            "172.32.0.0",      // just above it
            "100.63.255.255",  // just below the CGNAT range
            "100.128.0.0",     // just above it
            "198.17.255.255",  // just below the benchmarking range
            "198.20.0.0",      // just above it
            "223.255.255.255", // just below multicast
            "2606:4700::1111",
            "2a00:1450:4009:81b::200e",
        ] {
            assert!(!is_internal(ip(s)), "{s} should be allowed");
        }
    }

    #[test]
    fn one_bad_address_among_several_refuses_the_lot() {
        // A host the attacker controls can answer with one public address and one
        // loopback address and hope the connection picks the second.
        let addrs = [ip("93.184.216.34"), ip("127.0.0.1")];
        assert_eq!(
            strict().check_addrs(&addrs),
            Err(Refused::Internal(ip("127.0.0.1")))
        );
    }

    #[test]
    fn a_host_that_resolves_to_nothing_is_refused() {
        assert_eq!(strict().check_addrs(&[]), Err(Refused::Unresolvable));
        // Even permissively: there is nothing to send to.
        assert_eq!(
            Policy::permissive().check_addrs(&[]),
            Err(Refused::Unresolvable)
        );
    }

    #[test]
    fn only_http_and_https_are_accepted() {
        assert!(strict().check_scheme("http").is_ok());
        assert!(strict().check_scheme("https").is_ok());
        for s in ["file", "gopher", "ftp", "redis", "data", "jar"] {
            assert_eq!(
                strict().check_scheme(s),
                Err(Refused::Scheme(s.to_string())),
                "{s} should be refused"
            );
        }
    }

    #[test]
    fn the_permissive_policy_only_relaxes_addresses() {
        // Local development needs loopback receivers. It does not need `file://`.
        assert!(Policy::permissive().check_addrs(&[ip("127.0.0.1")]).is_ok());
        assert!(Policy::permissive().check_scheme("file").is_err());
    }

    #[test]
    fn the_default_policy_is_the_strict_one() {
        // The whole design rests on this. A permissive default is a vulnerability
        // that ships whenever somebody forgets to configure it.
        assert!(!Policy::default().allow_private);
        assert!(
            Policy::default()
                .check_addrs(&[ip("169.254.169.254")])
                .is_err()
        );
    }
}
