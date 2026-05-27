//go:build linux

package wg

import (
	"errors"
	"fmt"
	"net"

	"github.com/vishvananda/netlink"
)

func syncRoutes(name string, allowedIPs []*net.IPNet) error {
	link, err := netlink.LinkByName(name)
	if err != nil {
		return fmt.Errorf("wg: lookup %s for routes: %w", name, err)
	}
	desired := make(map[string]*net.IPNet, len(allowedIPs))
	for _, n := range allowedIPs {
		if n == nil {
			continue
		}
		desired[n.String()] = n
	}
	existing, err := netlink.RouteList(link, netlink.FAMILY_V4)
	if err != nil {
		return fmt.Errorf("wg: route list %s: %w", name, err)
	}
	have := map[string]bool{}
	for _, r := range existing {
		if r.Dst == nil {
			continue
		}
		key := r.Dst.String()
		if _, want := desired[key]; want {
			have[key] = true
			continue
		}
		_ = netlink.RouteDel(&r)
	}
	for key, dst := range desired {
		if have[key] {
			continue
		}
		if err := netlink.RouteAdd(&netlink.Route{
			LinkIndex: link.Attrs().Index,
			Dst:       dst,
			Scope:     netlink.SCOPE_LINK,
		}); err != nil {
			return fmt.Errorf("wg: route add %s: %w", key, err)
		}
	}
	return nil
}

func ensureInterface(name string, addr net.IP, mask net.IPMask) error {
	link, err := netlink.LinkByName(name)
	if err != nil {
		var notFound netlink.LinkNotFoundError
		if !errors.As(err, &notFound) {
			return fmt.Errorf("wg: lookup %s: %w", name, err)
		}
		la := netlink.NewLinkAttrs()
		la.Name = name
		wg := &netlink.GenericLink{LinkAttrs: la, LinkType: "wireguard"}
		if err := netlink.LinkAdd(wg); err != nil {
			return fmt.Errorf("wg: add %s: %w", name, err)
		}
		link, err = netlink.LinkByName(name)
		if err != nil {
			return fmt.Errorf("wg: lookup after add %s: %w", name, err)
		}
	}
	desired := &netlink.Addr{IPNet: &net.IPNet{IP: addr, Mask: mask}}
	addrs, err := netlink.AddrList(link, netlink.FAMILY_V4)
	if err != nil {
		return fmt.Errorf("wg: addr list %s: %w", name, err)
	}
	have := false
	for _, a := range addrs {
		if a.IPNet != nil && a.IP.Equal(desired.IP) {
			have = true
			continue
		}
		_ = netlink.AddrDel(link, &a)
	}
	if !have {
		if err := netlink.AddrAdd(link, desired); err != nil {
			return fmt.Errorf("wg: addr add %s: %w", name, err)
		}
	}
	if link.Attrs().Flags&net.FlagUp == 0 {
		if err := netlink.LinkSetUp(link); err != nil {
			return fmt.Errorf("wg: link up %s: %w", name, err)
		}
	}
	return nil
}
